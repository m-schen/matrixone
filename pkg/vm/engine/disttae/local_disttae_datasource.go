// Copyright 2021-2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"go.uber.org/zap"
	"slices"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/engine_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

func NewLocalDataSource(
	ctx context.Context,
	table *txnTable,
	txnOffset int,
	rangesSlice objectio.BlockInfoSlice,
	extraTombstones engine.Tombstoner,
	skipReadMem bool,
	policy engine.TombstoneApplyPolicy,
	category engine.DataSourceType,
) (source *LocalDisttaeDataSource, err error) {

	source = &LocalDisttaeDataSource{}
	source.category = category
	source.extraTombstones = extraTombstones
	source.fs = table.getTxn().engine.fs
	source.ctx = ctx
	source.mp = table.proc.Load().Mp()
	source.tombstonePolicy = policy

	if rangesSlice != nil && rangesSlice.Len() > 0 {
		if bytes.Equal(
			objectio.EncodeBlockInfo(rangesSlice.Get(0)),
			objectio.EmptyBlockInfoBytes) {
			rangesSlice = rangesSlice.Slice(1, rangesSlice.Len())
		}

		source.rangeSlice = rangesSlice
		source.rc.prefetchDisabled = rangesSlice.Len() < 4
	} else {
		source.rc.prefetchDisabled = true
	}

	if source.category != engine.ShardingLocalDataSource {
		state, err := table.getPartitionState(ctx)
		if err != nil {
			return nil, err
		}
		source.pState = state
	}

	source.table = table
	source.txnOffset = txnOffset
	source.snapshotTS = types.TimestampToTS(table.db.op.SnapshotTS())

	source.iteratePhase = engine.InMem
	if skipReadMem {
		source.iteratePhase = engine.Persisted
	}

	return source, nil
}

// --------------------------------------------------------------------------------
//	LocalDataSource defines and APIs
// --------------------------------------------------------------------------------

type LocalDisttaeDataSource struct {
	category        engine.DataSourceType
	extraTombstones engine.Tombstoner
	rangeSlice      objectio.BlockInfoSlice
	pState          *logtailreplay.PartitionState

	memPKFilter *engine_util.MemPKFilter
	pStateRows  struct {
		insIter logtailreplay.RowsIter
	}

	table     *txnTable
	wsCursor  int
	txnOffset int

	// runtime config
	rc struct {
		prefetchDisabled    bool
		batchPrefetchCursor int
		WorkspaceLocked     bool
		//SkipPStateDeletes   bool
	}

	mp  *mpool.MPool
	ctx context.Context
	fs  fileservice.FileService

	rangesCursor int
	snapshotTS   types.TS
	iteratePhase engine.DataState

	//TODO:: It's so ugly, need to refactor
	//for order by
	desc     bool
	blockZMS []index.ZM
	sorted   bool // blks need to be sorted by zonemap
	OrderBy  []*plan.OrderBySpec

	filterZM        objectio.ZoneMap
	tombstonePolicy engine.TombstoneApplyPolicy
}

func (ls *LocalDisttaeDataSource) String() string {
	blks := make([]*objectio.BlockInfo, ls.rangeSlice.Len())
	for i := range blks {
		blks[i] = ls.rangeSlice.Get(i)
	}

	return fmt.Sprintf("snapshot: %s, phase: %v, txnOffset: %d, rangeCursor: %d, blk list: %v",
		ls.snapshotTS.ToString(),
		ls.iteratePhase,
		ls.txnOffset,
		ls.rangesCursor,
		blks)
}

func (ls *LocalDisttaeDataSource) SetOrderBy(orderby []*plan.OrderBySpec) {
	ls.OrderBy = orderby
}

func (ls *LocalDisttaeDataSource) GetOrderBy() []*plan.OrderBySpec {
	return ls.OrderBy
}

func (ls *LocalDisttaeDataSource) SetFilterZM(zm objectio.ZoneMap) {
	if !ls.filterZM.IsInited() {
		ls.filterZM = zm.Clone()
		return
	}
	if ls.desc && ls.filterZM.CompareMax(zm) < 0 {
		ls.filterZM = zm.Clone()
		return
	}
	if !ls.desc && ls.filterZM.CompareMin(zm) > 0 {
		ls.filterZM = zm.Clone()
		return
	}
}

func (ls *LocalDisttaeDataSource) needReadBlkByZM(i int) bool {
	zm := ls.blockZMS[i]
	if !ls.filterZM.IsInited() || !zm.IsInited() {
		return true
	}
	if ls.desc {
		return ls.filterZM.CompareMax(zm) <= 0
	} else {
		return ls.filterZM.CompareMin(zm) >= 0
	}
}

func (ls *LocalDisttaeDataSource) getBlockZMs() {
	orderByCol, _ := ls.OrderBy[0].Expr.Expr.(*plan.Expr_Col)

	def := ls.table.tableDef
	orderByColIDX := int(def.Cols[int(orderByCol.Col.ColPos)].Seqnum)

	sliceLen := ls.rangeSlice.Len()
	ls.blockZMS = make([]index.ZM, sliceLen)
	var objDataMeta objectio.ObjectDataMeta
	var location objectio.Location
	for i := ls.rangesCursor; i < sliceLen; i++ {
		location = ls.rangeSlice.Get(i).MetaLocation()
		if !objectio.IsSameObjectLocVsMeta(location, objDataMeta) {
			objMeta, err := objectio.FastLoadObjectMeta(ls.ctx, &location, false, ls.fs)
			if err != nil {
				panic("load object meta error when ordered scan!")
			}
			objDataMeta = objMeta.MustDataMeta()
		}
		blkMeta := objDataMeta.GetBlockMeta(uint32(location.ID()))
		ls.blockZMS[i] = blkMeta.ColumnMeta(uint16(orderByColIDX)).ZoneMap()
	}
}

func (ls *LocalDisttaeDataSource) sortBlockList() {
	sliceLen := ls.rangeSlice.Len()
	// FIXME: no pointer in helper
	helper := make([]*blockSortHelper, sliceLen)
	for i := range sliceLen {
		helper[i] = &blockSortHelper{}
		helper[i].blk = ls.rangeSlice.Get(i)
		helper[i].zm = ls.blockZMS[i]
	}
	ls.rangeSlice = make(objectio.BlockInfoSlice, ls.rangeSlice.Size())

	if ls.desc {
		sort.Slice(helper, func(i, j int) bool {
			zm1 := helper[i].zm
			if !zm1.IsInited() {
				return true
			}
			zm2 := helper[j].zm
			if !zm2.IsInited() {
				return false
			}
			return zm1.CompareMax(zm2) > 0
		})
	} else {
		sort.Slice(helper, func(i, j int) bool {
			zm1 := helper[i].zm
			if !zm1.IsInited() {
				return true
			}
			zm2 := helper[j].zm
			if !zm2.IsInited() {
				return false
			}
			return zm1.CompareMin(zm2) < 0
		})
	}

	for i := range helper {
		ls.rangeSlice.Set(i, helper[i].blk)
		//ls.ranges[i] = helper[i].blk
		ls.blockZMS[i] = helper[i].zm
	}
}

func (ls *LocalDisttaeDataSource) Close() {
	if ls.pStateRows.insIter != nil {
		ls.pStateRows.insIter.Close()
		ls.pStateRows.insIter = nil
	}
}

func (ls *LocalDisttaeDataSource) Next(
	ctx context.Context,
	cols []string,
	types []types.Type,
	seqNums []uint16,
	filter any,
	mp *mpool.MPool,
	outBatch *batch.Batch,
) (*objectio.BlockInfo, engine.DataState, error) {

	if ls.memPKFilter == nil {
		ff := filter.(engine_util.MemPKFilter)
		ls.memPKFilter = &ff
	}

	if len(cols) == 0 {
		return nil, engine.End, nil
	}

	// bathed prefetch block data and deletes
	ls.batchPrefetch(seqNums)

	for {
		switch ls.iteratePhase {
		case engine.InMem:
			outBatch.CleanOnlyData()
			err := ls.iterateInMemData(ctx, cols, types, seqNums, outBatch, mp)
			if err != nil {
				return nil, engine.InMem, err
			}

			if outBatch.RowCount() == 0 {
				ls.iteratePhase = engine.Persisted
				continue
			}

			return nil, engine.InMem, nil

		case engine.Persisted:
			if ls.rangesCursor >= ls.rangeSlice.Len() {
				return nil, engine.End, nil
			}

			ls.handleOrderBy()

			if ls.rangesCursor >= ls.rangeSlice.Len() {
				return nil, engine.End, nil
			}

			blk := ls.rangeSlice.Get(ls.rangesCursor)
			ls.rangesCursor++

			return blk, engine.Persisted, nil

		case engine.End:
			return nil, ls.iteratePhase, nil
		}
	}
}

func (ls *LocalDisttaeDataSource) handleOrderBy() {
	// for ordered scan, sort blocklist by zonemap info, and then filter by zonemap
	if len(ls.OrderBy) > 0 {
		if !ls.sorted {
			ls.desc = ls.OrderBy[0].Flag&plan.OrderBySpec_DESC != 0
			ls.getBlockZMs()
			ls.sortBlockList()
			ls.sorted = true
		}
		i := ls.rangesCursor
		sliceLen := ls.rangeSlice.Len()
		for i < sliceLen {
			if ls.needReadBlkByZM(i) {
				break
			}
			i++
		}
		ls.rangesCursor = i
	}
}

func (ls *LocalDisttaeDataSource) iterateInMemData(
	ctx context.Context,
	cols []string,
	colTypes []types.Type,
	seqNums []uint16,
	outBatch *batch.Batch,
	mp *mpool.MPool,
) (err error) {

	outBatch.SetRowCount(0)

	if ls.category != engine.ShardingRemoteDataSource {
		if err = ls.filterInMemUnCommittedInserts(ctx, seqNums, mp, outBatch); err != nil {
			return err
		}
	}

	if ls.category != engine.ShardingLocalDataSource {
		if err = ls.filterInMemCommittedInserts(ctx, colTypes, seqNums, mp, outBatch); err != nil {
			return err
		}
	}

	return nil
}

func checkWorkspaceEntryType(
	tbl *txnTable,
	entry Entry,
	isInsert bool,
) bool {
	if entry.DatabaseId() != tbl.db.databaseId || entry.TableId() != tbl.tableId {
		return false
	}

	// within a txn, the later statement could delete the previous
	// inserted rows, the left rows will be recorded in `batSelectList`.
	// if no rows left, this bat can be seen deleted.
	//
	// Note that: some row have been deleted, but some left
	if isInsert {
		if entry.typ != INSERT ||
			entry.bat == nil ||
			entry.bat.IsEmpty() ||
			entry.bat.Attrs[0] == catalog.BlockMeta_MetaLoc {
			return false
		}
		if left, exist := tbl.getTxn().batchSelectList[entry.bat]; exist && len(left) == 0 {
			// all rows have deleted in this bat
			return false
		} else if len(left) > 0 {
			// FIXME: if len(left) > 0, we need to exclude the deleted rows in this batch
			logutil.Fatal("FIXME: implement later")
		}
		return true
	}

	// handle delete entry
	return (entry.typ == DELETE) && (entry.fileName == "")
}

func checkTxnOffsetZero(ls *LocalDisttaeDataSource, writes []Entry) {
	if len(writes) > 200 && ls.txnOffset == 0 && ls.table.accountId == 0 && ls.table.tableName == "mo_increment_columns" {
		logutil.Info("yyyyyy zero txnOffset",
			zap.String("txn", hex.EncodeToString(ls.table.db.op.Txn().ID)),
			zap.Bool("isSnapOp", ls.table.db.op.IsSnapOp()),
			zap.String("entries", stringifySlice(writes[len(writes)-2:], func(a any) string {
				e := a.(Entry)
				batstr := "nil"
				if e.bat != nil {
					batstr = common.MoBatchToString(e.bat, 3)
				}
				return e.String() + " " + batstr
			})))
	}
}

func checkTxnLastInsertRow(ls *LocalDisttaeDataSource, writes []Entry, cursor int, outBatch *batch.Batch) {
	if len(writes) > 400 && ls.table.accountId == 0 && ls.table.tableName == "mo_increment_columns" && writes[len(writes)-1].typ == INSERT && writes[len(writes)-1].tableId == ls.table.tableId {
		logutil.Info("yyyyyy checkTxnLastInsertRow",
			zap.String("txn", hex.EncodeToString(ls.table.db.op.Txn().ID)),
			zap.Int("txnOffset", ls.txnOffset),
			zap.Int("cursor", cursor),
			zap.Int("writes", len(writes)),
			zap.Bool("isSnapOp", ls.table.db.op.IsSnapOp()),
			zap.String("entries", stringifySlice(writes[len(writes)-2:], func(a any) string {
				e := a.(Entry)
				batstr := "nil"
				if e.bat != nil {
					batstr = common.MoBatchToString(e.bat, 3)
				}
				return e.String() + " " + batstr
			})),
			zap.String("outBatch", common.MoBatchToString(outBatch, 3)),
		)
	}
}

func (ls *LocalDisttaeDataSource) filterInMemUnCommittedInserts(
	_ context.Context,
	seqNums []uint16,
	mp *mpool.MPool,
	outBatch *batch.Batch,
) error {
	if ls.wsCursor >= ls.txnOffset {
		return nil
	}
	ls.table.getTxn().Lock()
	ls.rc.WorkspaceLocked = true
	defer func() {
		ls.table.getTxn().Unlock()
		ls.rc.WorkspaceLocked = false
	}()

	rows := 0
	writes := ls.table.getTxn().writes
	maxRows := objectio.BlockMaxRows
	if len(writes) == 0 {
		return nil
	}

	var retainedRowIds []objectio.Rowid

	beginCursor := ls.wsCursor

	for ; ls.wsCursor < ls.txnOffset; ls.wsCursor++ {
		if writes[ls.wsCursor].bat == nil {
			continue
		}

		if rows+writes[ls.wsCursor].bat.RowCount() > maxRows {
			break
		}

		entry := writes[ls.wsCursor]

		if ok := checkWorkspaceEntryType(ls.table, entry, true); !ok {
			continue
		}

		retainedRowIds = vector.MustFixedColWithTypeCheck[objectio.Rowid](entry.bat.Vecs[0])
		offsets := engine_util.RowIdsToOffset(retainedRowIds, int64(0)).([]int64)

		b := retainedRowIds[0].BorrowBlockID()
		sels, err := ls.ApplyTombstones(
			ls.ctx, b, offsets, engine.Policy_CheckUnCommittedOnly)
		if err != nil {
			return err
		}

		if len(sels) == 0 {
			continue
		}

		rows += len(sels)

		for i, destVec := range outBatch.Vecs {
			colIdx := int(seqNums[i])
			if colIdx != objectio.SEQNUM_ROWID {
				colIdx++
			} else {
				colIdx = 0
			}
			if err = destVec.Union(entry.bat.Vecs[colIdx], sels, mp); err != nil {
				return err
			}
		}
	}

	checkTxnLastInsertRow(ls, writes, beginCursor, outBatch)
	outBatch.SetRowCount(outBatch.Vecs[0].Length())
	return nil
}

func (ls *LocalDisttaeDataSource) filterInMemCommittedInserts(
	_ context.Context,
	colTypes []types.Type,
	seqNums []uint16,
	mp *mpool.MPool,
	outBatch *batch.Batch,
) error {
	if outBatch.RowCount() >= objectio.BlockMaxRows {
		return nil
	}

	var (
		err  error
		sels []int64
	)

	if ls.pStateRows.insIter == nil {
		if ls.memPKFilter.SpecFactory == nil {
			ls.pStateRows.insIter = ls.pState.NewRowsIter(ls.snapshotTS, nil, false)
		} else {
			ls.pStateRows.insIter = ls.pState.NewPrimaryKeyIter(
				ls.memPKFilter.TS, ls.memPKFilter.SpecFactory(ls.memPKFilter))
		}
	}

	var (
		physicalColumn    vector.Vector
		physicalColumnPtr *vector.Vector
		physicalColumnPos int
	)
	if physicalColumnPos = slices.Index(
		outBatch.Attrs,
		objectio.PhysicalAddr_Attr,
	); physicalColumnPos == -1 {
		physicalColumn.SetType(objectio.RowidType)
		physicalColumnPtr = &physicalColumn
		defer physicalColumn.Free(mp)
	} else {
		physicalColumnPtr = outBatch.Vecs[physicalColumnPos]
	}

	applyPolicy := engine.TombstoneApplyPolicy(
		engine.Policy_SkipCommittedInMemory | engine.Policy_SkipCommittedS3,
	)

	var (
		goNext      bool = true
		minTS            = types.MaxTs()
		inputRowCnt      = outBatch.RowCount()
		applyOffset      = 0
	)

	for goNext && outBatch.Vecs[0].Length() < int(objectio.BlockMaxRows) {
		for outBatch.Vecs[0].Length() < int(objectio.BlockMaxRows) {
			if goNext = ls.pStateRows.insIter.Next(); !goNext {
				break
			}

			entry := ls.pStateRows.insIter.Entry()
			b, o := entry.RowID.Decode()

			if sels, err = ls.ApplyTombstones(
				ls.ctx, b, []int64{int64(o)}, applyPolicy,
			); err != nil {
				return err
			}

			if len(sels) == 0 {
				continue
			}

			if minTS.GT(&entry.Time) {
				minTS = entry.Time
			}

			if err = vector.AppendFixed(
				physicalColumnPtr,
				entry.RowID,
				false,
				mp,
			); err != nil {
				return err
			}

			for i := range outBatch.Attrs {
				if i == physicalColumnPos {
					continue
				}
				idx := 2 /*rowid and commits*/ + seqNums[i]
				if int(idx) >= len(entry.Batch.Vecs) /*add column*/ ||
					entry.Batch.Attrs[idx] == "" /*drop column*/ {
					err = vector.AppendAny(
						outBatch.Vecs[i],
						nil,
						true,
						mp)
				} else {
					err = outBatch.Vecs[i].UnionOne(
						entry.Batch.Vecs[int(2+seqNums[i])],
						entry.Offset,
						mp,
					)
				}
				if err != nil {
					return err
				}
			}
		}

		rowIds := vector.MustFixedColNoTypeCheck[objectio.Rowid](physicalColumnPtr)
		deleted, err := ls.batchApplyTombstoneObjects(minTS, rowIds[applyOffset:])
		if err != nil {
			return err
		}

		if len(deleted) > 0 {
			if physicalColumnPos == -1 {
				for i := range deleted {
					deleted[i] += int64(applyOffset)
				}
				physicalColumnPtr.Shrink(deleted, true)
				for i := range deleted {
					deleted[i] += int64(inputRowCnt)
				}
				outBatch.Shrink(deleted, true)
			} else {
				for i := range deleted {
					deleted[i] += int64(applyOffset)
				}
				outBatch.Shrink(deleted, true)
			}
		}

		minTS = types.MaxTs()
		applyOffset = physicalColumnPtr.Length()
	}

	outBatch.SetRowCount(outBatch.Vecs[0].Length())

	return nil
}

// ApplyTombstones check if any deletes exist in
//  1. unCommittedInmemDeletes:
//     a. workspace writes
//     b. flushed to s3
//     c. raw rowId offset deletes (not flush yet)
//  3. committedInmemDeletes
//  4. committedPersistedTombstone
func (ls *LocalDisttaeDataSource) ApplyTombstones(
	ctx context.Context,
	bid *objectio.Blockid,
	rowsOffset []int64,
	dynamicPolicy engine.TombstoneApplyPolicy,
) ([]int64, error) {

	if len(rowsOffset) == 0 {
		return nil, nil
	}

	slices.SortFunc(rowsOffset, func(a, b int64) int {
		return int(a - b)
	})

	var err error

	if ls.category == engine.ShardingRemoteDataSource {
		if ls.extraTombstones != nil {
			rowsOffset = ls.extraTombstones.ApplyInMemTombstones(bid, rowsOffset, nil)
			rowsOffset, err = ls.extraTombstones.ApplyPersistedTombstones(ctx, ls.fs, &ls.snapshotTS, bid, rowsOffset, nil)
			if err != nil {
				return nil, err
			}
		}
		if len(rowsOffset) == 0 {
			return nil, nil
		}
	}

	if ls.tombstonePolicy&engine.Policy_SkipUncommitedInMemory == 0 &&
		dynamicPolicy&engine.Policy_SkipUncommitedInMemory == 0 {
		rowsOffset = ls.applyWorkspaceEntryDeletes(bid, rowsOffset, nil)
	}
	if len(rowsOffset) == 0 {
		return nil, nil
	}
	if ls.tombstonePolicy&engine.Policy_SkipUncommitedS3 == 0 &&
		dynamicPolicy&engine.Policy_SkipUncommitedS3 == 0 {
		rowsOffset, err = ls.applyWorkspaceFlushedS3Deletes(bid, rowsOffset, nil)
		if err != nil {
			return nil, err
		}
	}
	if len(rowsOffset) == 0 {
		return nil, nil
	}

	if ls.tombstonePolicy&engine.Policy_SkipUncommitedInMemory == 0 &&
		dynamicPolicy&engine.Policy_SkipUncommitedInMemory == 0 {
		rowsOffset = ls.applyWorkspaceRawRowIdDeletes(bid, rowsOffset, nil)
	}
	if len(rowsOffset) == 0 {
		return nil, nil
	}
	if ls.tombstonePolicy&engine.Policy_SkipCommittedInMemory == 0 &&
		dynamicPolicy&engine.Policy_SkipCommittedInMemory == 0 {
		rowsOffset = ls.applyPStateInMemDeletes(bid, rowsOffset, nil)
	}
	if len(rowsOffset) == 0 {
		return nil, nil
	}
	if ls.tombstonePolicy&engine.Policy_SkipCommittedS3 == 0 &&
		dynamicPolicy&engine.Policy_SkipCommittedS3 == 0 {
		rowsOffset, err = ls.applyPStateTombstoneObjects(bid, rowsOffset, nil)
		if err != nil {
			return nil, err
		}
	}

	return rowsOffset, nil
}

func (ls *LocalDisttaeDataSource) GetTombstones(
	ctx context.Context, bid *objectio.Blockid,
) (deletedRows objectio.Bitmap, err error) {

	deletedRows = objectio.GetReusableBitmap()

	if ls.category == engine.ShardingRemoteDataSource {
		if ls.extraTombstones != nil {
			ls.extraTombstones.ApplyInMemTombstones(bid, nil, &deletedRows)
			if _, err = ls.extraTombstones.ApplyPersistedTombstones(
				ctx, ls.fs, &ls.snapshotTS, bid, nil, &deletedRows,
			); err != nil {
				deletedRows.Release()
				return
			}
		}
	}

	if ls.tombstonePolicy&engine.Policy_SkipUncommitedInMemory == 0 {
		ls.applyWorkspaceEntryDeletes(bid, nil, &deletedRows)
	}
	if ls.tombstonePolicy&engine.Policy_SkipUncommitedS3 == 0 {
		if _, err = ls.applyWorkspaceFlushedS3Deletes(
			bid, nil, &deletedRows,
		); err != nil {
			deletedRows.Release()
			return
		}
	}

	if ls.tombstonePolicy&engine.Policy_SkipUncommitedInMemory == 0 {
		ls.applyWorkspaceRawRowIdDeletes(bid, nil, &deletedRows)
	}

	if ls.tombstonePolicy&engine.Policy_SkipCommittedInMemory == 0 {
		ls.applyPStateInMemDeletes(bid, nil, &deletedRows)
	}

	if _, err = ls.applyPStateTombstoneObjects(bid, nil, &deletedRows); err != nil {
		deletedRows.Release()
		return
	}

	return
}

func (ls *LocalDisttaeDataSource) applyWorkspaceEntryDeletes(
	bid *objectio.Blockid,
	offsets []int64,
	deletedRows *objectio.Bitmap,
) (leftRows []int64) {

	leftRows = offsets

	// may have locked in `filterInMemUnCommittedInserts`
	if !ls.rc.WorkspaceLocked {
		ls.table.getTxn().Lock()
		defer ls.table.getTxn().Unlock()
	}

	done := false
	writes := ls.table.getTxn().writes[:ls.txnOffset]

	var delRowIds []objectio.Rowid

	for idx := range writes {
		if ok := checkWorkspaceEntryType(ls.table, writes[idx], false); !ok {
			continue
		}

		delRowIds = vector.MustFixedColWithTypeCheck[objectio.Rowid](writes[idx].bat.Vecs[0])
		for _, delRowId := range delRowIds {
			b, o := delRowId.Decode()
			if bid.Compare(b) != 0 {
				continue
			}

			leftRows = engine_util.FastApplyDeletedRows(leftRows, deletedRows, o)
			if leftRows != nil && len(leftRows) == 0 {
				done = true
				break
			}
		}

		if done {
			break
		}
	}

	return leftRows
}

func (ls *LocalDisttaeDataSource) applyWorkspaceFlushedS3Deletes(
	bid *objectio.Blockid,
	offsets []int64,
	deletedRows *objectio.Bitmap,
) (leftRows []int64, err error) {

	leftRows = offsets

	s3FlushedDeletes := &ls.table.getTxn().cn_flushed_s3_tombstone_object_stats_list
	s3FlushedDeletes.RWMutex.Lock()
	defer s3FlushedDeletes.RWMutex.Unlock()

	if len(s3FlushedDeletes.data) == 0 {
		return
	}

	release := func() {}
	if deletedRows == nil {
		bm := objectio.GetReusableBitmap()
		deletedRows = &bm
		release = bm.Release
	}
	defer release()

	var curr int
	getTombstone := func() (*objectio.ObjectStats, error) {
		if curr >= len(s3FlushedDeletes.data) {
			return nil, nil
		}
		i := curr
		curr++
		return &s3FlushedDeletes.data[i], nil
	}

	if err = blockio.GetTombstonesByBlockId(
		ls.ctx,
		&ls.snapshotTS,
		bid,
		getTombstone,
		deletedRows,
		ls.fs,
	); err != nil {
		return nil, err
	}

	offsets = engine_util.RemoveIf(offsets, func(t int64) bool {
		return deletedRows.Contains(uint64(t))
	})

	return offsets, nil
}

func (ls *LocalDisttaeDataSource) applyWorkspaceRawRowIdDeletes(
	bid *objectio.Blockid,
	offsets []int64,
	deletedRows *objectio.Bitmap,
) (leftRows []int64) {

	leftRows = offsets

	rawRowIdDeletes := ls.table.getTxn().deletedBlocks
	rawRowIdDeletes.RWMutex.RLock()
	defer rawRowIdDeletes.RWMutex.RUnlock()

	for _, o := range rawRowIdDeletes.offsets[*bid] {
		leftRows = engine_util.FastApplyDeletedRows(leftRows, deletedRows, uint32(o))
		if leftRows != nil && len(leftRows) == 0 {
			break
		}
	}

	return leftRows
}

func (ls *LocalDisttaeDataSource) applyPStateInMemDeletes(
	bid *objectio.Blockid,
	offsets []int64,
	deletedRows *objectio.Bitmap,
) (leftRows []int64) {
	var delIter logtailreplay.RowsIter

	if ls.memPKFilter == nil || ls.memPKFilter.SpecFactory == nil {
		delIter = ls.pState.NewRowsIter(ls.snapshotTS, bid, true)
	} else {
		delIter = ls.pState.NewPrimaryKeyDelIter(
			&ls.memPKFilter.TS,
			ls.memPKFilter.SpecFactory(ls.memPKFilter), bid)
	}

	leftRows = offsets

	for delIter.Next() {
		rowid := delIter.Entry().RowID
		o := rowid.GetRowOffset()
		leftRows = engine_util.FastApplyDeletedRows(leftRows, deletedRows, o)
		if leftRows != nil && len(leftRows) == 0 {
			break
		}
	}

	delIter.Close()

	return leftRows
}

func (ls *LocalDisttaeDataSource) applyPStateTombstoneObjects(
	bid *objectio.Blockid,
	offsets []int64,
	deletedRows *objectio.Bitmap,
) ([]int64, error) {
	if ls.pState.ApproxTombstoneObjectsNum() == 0 {
		return offsets, nil
	}

	var iter logtailreplay.ObjectsIter
	getTombstone := func() (*objectio.ObjectStats, error) {
		var err error
		if iter == nil {
			if iter, err = ls.pState.NewObjectsIter(
				ls.snapshotTS, true, true,
			); err != nil {
				return nil, err
			}
		}
		if iter.Next() {
			entry := iter.Entry()
			return &entry.ObjectStats, nil
		}
		return nil, nil
	}
	defer func() {
		if iter != nil {
			iter.Close()
		}
	}()

	// PXU TODO: handle len(offsets) < 10 or 20, 30?
	if len(offsets) == 1 {
		rowid := objectio.NewRowid(bid, uint32(offsets[0]))
		deleted, err := blockio.IsRowDeleted(
			ls.ctx,
			&ls.snapshotTS,
			rowid,
			getTombstone,
			ls.fs,
		)
		if err != nil {
			return nil, err
		}
		if deleted {
			return nil, nil
		}
		return offsets, nil
	}

	release := func() {}
	if deletedRows == nil {
		bm := objectio.GetReusableBitmap()
		deletedRows = &bm
		release = bm.Release
	}
	defer release()

	if err := blockio.GetTombstonesByBlockId(
		ls.ctx,
		&ls.snapshotTS,
		bid,
		getTombstone,
		deletedRows,
		ls.fs,
	); err != nil {
		return nil, err
	}

	offsets = engine_util.RemoveIf(offsets, func(t int64) bool {
		return deletedRows.Contains(uint64(t))
	})

	return offsets, nil
}

func (ls *LocalDisttaeDataSource) batchPrefetch(seqNums []uint16) {
	if ls.rc.prefetchDisabled {
		return
	}
	if ls.rc.batchPrefetchCursor >= ls.rangeSlice.Len() ||
		ls.rangesCursor < ls.rc.batchPrefetchCursor {
		return
	}

	batchSize := min(engine_util.BatchPrefetchSize, ls.rangeSlice.Len()-ls.rangesCursor)

	begin := ls.rangesCursor
	end := ls.rangesCursor + batchSize

	var preObj types.Objectid
	for idx := begin; idx < end; idx++ {
		blk := ls.rangeSlice.Get(idx)
		if blk.BlockID.Object().EQ(&preObj) {
			continue
		}

		preObj = *blk.BlockID.Object()

		// prefetch blk data
		err := blockio.Prefetch(
			ls.table.proc.Load().GetService(), ls.fs, blk.MetaLocation())
		if err != nil {
			logutil.Errorf("pefetch block data: %s, blk:%s",
				err.Error(),
				blk.String())
		}
	}

	ls.rc.batchPrefetchCursor = end
}

func (ls *LocalDisttaeDataSource) batchApplyTombstoneObjects(
	minTS types.TS,
	rowIds []objectio.Rowid,
) (deleted []int64, err error) {

	if ls.pState.ApproxTombstoneObjectsNum() == 0 {
		return nil, nil
	}

	iter, err := ls.pState.NewObjectsIter(ls.snapshotTS, true, true)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var (
		location objectio.Location

		release func()
	)

	anyIf := func(check func(row objectio.Rowid) bool) bool {
		for _, r := range rowIds {
			if check(r) {
				return true
			}
		}
		return false
	}

	attrs := objectio.GetTombstoneAttrs(objectio.HiddenColumnSelection_CommitTS)
	cacheVectors := containers.NewVectors(len(attrs))

	for iter.Next() && len(deleted) < len(rowIds) {
		obj := iter.Entry()

		if !obj.GetAppendable() {
			if obj.CreateTime.LT(&minTS) {
				continue
			}
		}

		if !obj.ZMIsEmpty() {
			objZM := obj.SortKeyZoneMap()

			if !anyIf(func(row objectio.Rowid) bool {
				return objZM.Contains(row)
			}) {
				continue
			}
		}

		for idx := 0; idx < int(obj.BlkCnt()) && len(rowIds) > len(deleted); idx++ {
			location = obj.ObjectStats.BlockLocation(uint16(idx), objectio.BlockMaxRows)

			if _, release, err = blockio.ReadDeletes(
				ls.ctx, location, ls.fs, obj.GetCNCreated(), cacheVectors,
			); err != nil {
				return nil, err
			}

			var deletedRowIds []objectio.Rowid
			var commit []types.TS

			deletedRowIds = vector.MustFixedColWithTypeCheck[objectio.Rowid](&cacheVectors[0])
			if !obj.GetCNCreated() {
				commit = vector.MustFixedColWithTypeCheck[types.TS](&cacheVectors[1])
			}

			for i := 0; i < len(rowIds); i++ {
				s, e := blockio.FindStartEndOfBlockFromSortedRowids(
					deletedRowIds, rowIds[i].BorrowBlockID())

				for j := s; j < e; j++ {
					if rowIds[i].EQ(&deletedRowIds[j]) &&
						(commit == nil || commit[j].LE(&ls.snapshotTS)) {
						deleted = append(deleted, int64(i))
						break
					}
				}
			}

			release()
		}
	}

	return deleted, nil
}
