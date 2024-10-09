// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hashcms

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"runtime"
	"time"
)

// SpilledHashMap splits and spills the data sources of the hash map to the disk.
//
// hashInfo: information for hash map creation.
// spilledBatchInfo: information for spilled file.
type SpilledHashMap struct {
	usr context.Context
	mp  *mpool.MPool
	srv ReadWriteImplementer
	uid uuid.UUID

	// HashTable related information.
	hashInfo struct {
		isStrHashMap bool
		keyHasNulls  bool

		// hashOnUniqueColumn is true for hash on primary key and other columns with unique index.
		hashOnUniqueColumn bool

		// totalColumnCount indicates how many columns were stored to disk.
		totalColumnCount int
		// keyColumnIdxList indicates which columns were hash table keys.
		keyColumnIdxList []int
	}

	// spilled batches.
	blocks []spilledBatchInfo
}

// ReadWriteImplementer is a subset of fileservice.FileService,
// providing Read, Write, and Close methods externally.
// The reason we did not use fileservice.FileService directly is that it facilitates easier testing.
type ReadWriteImplementer interface {
	Write(ctx context.Context, vector fileservice.IOVector) error
	Read(ctx context.Context, vector *fileservice.IOVector) error
	Delete(ctx context.Context, filePaths ...string) error
	Close()
}

// InitSpilledHashMap return the SpilledHashMap which support
// 1. store batch.
// 2. get hash map from multiple stored batches.
// 3. close.
func InitSpilledHashMap(
	usr context.Context, srv ReadWriteImplementer,
	mp *mpool.MPool,
	hashOnUniqueColumn bool,
	keyWidth int, keyWithNulls bool,
) SpilledHashMap {

	hm := SpilledHashMap{
		usr: usr,
		mp:  mp,
		srv: srv,
		uid: uuid.New(),
	}
	hm.hashInfo.isStrHashMap = keyWidth > 8
	hm.hashInfo.keyHasNulls = keyWithNulls
	hm.hashInfo.hashOnUniqueColumn = hashOnUniqueColumn

	return hm
}

// spilledBatchInfo stores all information about one spilled batch.
type spilledBatchInfo struct {
	path     string
	rowCount uint64
}

type hashmapBuildingContext struct {
	// doPreAllocatedCheck true means should do specific check during insert data.
	doPreAllocatedCheck bool
	// requireInputRows is how many rows of data should insert.
	requireInputRows uint64
	// alreadyInputRows is how many rows of data has been insert into hashmap.
	alreadyInputRows int
}

// readBatchByPath read a batch from its disk path.
func (spilledHm *SpilledHashMap) readBatchByPath(str string) (*batch.Batch, error) {
	entry := getIOEntryToReadBatch()
	ioVector := fileservice.IOVector{
		FilePath: str,
		Entries:  []fileservice.IOEntry{entry},
	}

	if err := spilledHm.srv.Read(spilledHm.usr, &ioVector); err != nil {
		return nil, err
	}

	data := ioVector.Entries[0].Data
	bat := batch.NewOffHeapEmpty()
	if err := bat.UnmarshalBinaryWithAnyMp(data, spilledHm.mp); err != nil {
		return nil, err
	}
	return bat, nil
}

// writeBatchToFileService write a batch to disk.
func (spilledHm *SpilledHashMap) writeBatchToFileService(data *batch.Batch) (string, error) {
	path := spilledHm.buildSpilledBatchPath()
	entry, err := getIOEntryToWriteBatch(data)
	if err != nil {
		return "", err
	}

	ioVector := fileservice.IOVector{
		FilePath: path,
		Entries:  []fileservice.IOEntry{entry},
	}

	if err = spilledHm.srv.Write(spilledHm.usr, ioVector); err != nil {
		return "", err
	}

	return path, nil
}

// buildSpilledBatchPath return a unique path for a new spilled block.
func (spilledHm *SpilledHashMap) buildSpilledBatchPath() string {
	// format: temp_sh_uid_{block_idx}_time.
	return fmt.Sprintf(
		"temp_sh_%s_{%d}_%s", spilledHm.uid, len(spilledHm.blocks), time.Now())
}

// getIOEntryToWriteBatch
// 1. encode the batch.
// 2. generate an io entry to write all the whole data.
func getIOEntryToWriteBatch(b *batch.Batch) (fileservice.IOEntry, error) {
	bs, err := b.MarshalBinary()

	return fileservice.IOEntry{
		Offset: 0,
		Size:   -1,
		Data:   bs,
	}, err
}

// getIOEntryToReadBatch return an IOEntry to get all the data.
func getIOEntryToReadBatch() fileservice.IOEntry {
	return fileservice.IOEntry{
		Offset: 0,
		Size:   -1,
	}
}

// StoreBatch spill the batch with key column to disk.
//
// 1. create a new dst batch to avoid modifying the src batch.
// 2. evaluate the keys of hash map, and save them to the dst batch.
// 3. spill the dst batch to the disk.
// 4. add spill-batch information to the SpilledHashMap.
func (spilledHm *SpilledHashMap) StoreBatch(
	proc *process.Process, src *batch.Batch, keyExecutors ...colexec.ExpressionExecutor) error {

	// If this is the first time to spill a batch, do some prepared calculation.
	if len(spilledHm.blocks) == 0 {
		spilledHm.hashInfo.totalColumnCount = len(src.Vecs)
		spilledHm.hashInfo.keyColumnIdxList = make([]int, len(keyExecutors))

		for i, exec := range keyExecutors {
			if exec.IsColumnExpr() {
				columnExpr := exec.(*colexec.ColumnExpressionExecutor)
				spilledHm.hashInfo.keyColumnIdxList[i] = columnExpr.GetColIndex()
				continue
			}

			spilledHm.hashInfo.keyColumnIdxList[i] = spilledHm.hashInfo.totalColumnCount
			spilledHm.hashInfo.totalColumnCount++
		}
	}

	// normal spill.
	dst := &batch.Batch{
		Recursive:  src.Recursive,
		Attrs:      src.Attrs,
		ShuffleIDX: src.ShuffleIDX,
		Vecs:       make([]*vector.Vector, 0, spilledHm.hashInfo.totalColumnCount),
	}
	dst.SetRowCount(src.RowCount())
	dst.Vecs = append(dst.Vecs, src.Vecs...)

	for i, idx := range spilledHm.hashInfo.keyColumnIdxList {
		if idx < len(src.Vecs) {
			dst.Vecs = append(dst.Vecs, src.Vecs[idx])
			continue
		}

		// if the key column is not an origin vector.
		v, e := keyExecutors[i].Eval(proc, []*batch.Batch{src}, nil)
		if e != nil {
			return e
		}
		dst.Vecs = append(dst.Vecs, v)
	}

	path, err := spilledHm.writeBatchToFileService(dst)
	if err != nil {
		return err
	}

	spilledHm.blocks = append(spilledHm.blocks, spilledBatchInfo{
		path:     path,
		rowCount: uint64(dst.RowCount()),
	})
	return nil
}

// ReadHashMapByIdxes return a hashmap from specific batches.
//
// 1. build an empty hash map.
// 2. loop to read batch and insert into hash map.
func (spilledHm *SpilledHashMap) ReadHashMapByIdxes(idxes ...int) (hashmap.HashMap, error) {
	hmp, hmpItr, err := buildEmptyHashMap(spilledHm.hashInfo.isStrHashMap, spilledHm.hashInfo.keyHasNulls)
	if err != nil {
		return nil, err
	}

	insertContext := &hashmapBuildingContext{
		requireInputRows: 0,
		alreadyInputRows: 0,
	}

	for _, idx := range idxes {
		insertContext.requireInputRows += spilledHm.blocks[idx].rowCount
	}

	// If hash on a unique column, total row count is the final size of hash map.
	//
	// we allocated memory for hash map to avoid memory expand while appending data.
	if spilledHm.hashInfo.hashOnUniqueColumn {
		if err = hmp.PreAlloc(insertContext.requireInputRows); err != nil {
			hmp.Free()
			return nil, err
		}
	}

	// If not hash on unique column, and this may not a very small hash table.
	insertContext.doPreAllocatedCheck = !spilledHm.hashInfo.hashOnUniqueColumn &&
		insertContext.requireInputRows > hashmap.HashMapSizeThreshHold

	// data insert.
	var b *batch.Batch
	for _, idx := range idxes {
		b, err = spilledHm.readBatchByPath(spilledHm.blocks[idx].path)
		if err != nil {
			hmp.Free()
			return nil, err
		}

		err = spilledHm.insertBatchIntoHashmap(hmp, hmpItr, b, insertContext)
		b.Clean(spilledHm.mp)
		if err != nil {
			hmp.Free()
			return nil, err
		}
	}
	return hmp, nil
}

// buildEmptyHashMap generate an empty hash map.
func buildEmptyHashMap(isStr bool, hasNulls bool) (hashmap.HashMap, hashmap.Iterator, error) {
	var hmp hashmap.HashMap
	var hmpItr hashmap.Iterator

	if isStr {
		strHmp, err := hashmap.NewStrMap(hasNulls)
		if err != nil {
			return nil, nil, err
		}

		hmpItr = strHmp.NewIterator()
		hmp = strHmp
	} else {
		intHmp, err := hashmap.NewIntHashMap(hasNulls)
		if err != nil {
			return nil, nil, err
		}

		hmpItr = intHmp.NewIterator()
		hmp = intHmp
	}

	return hmp, hmpItr, nil
}

// insertBatchIntoHashmap upsert the hash map with input data.
//
// 1. try to expand the memory.
// 2. do insertion.
func (spilledHm *SpilledHashMap) insertBatchIntoHashmap(
	hmp hashmap.HashMap,
	itr hashmap.Iterator,
	data *batch.Batch, ctx *hashmapBuildingContext) error {

	thisLength := data.Vecs[0].Length()

	keys := make([]*vector.Vector, len(spilledHm.hashInfo.keyColumnIdxList))

	for i := 0; i < thisLength; i += hashmap.UnitLimit {
		if i%(hashmap.UnitLimit*32) == 0 {
			runtime.Gosched()
		}

		n := thisLength - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}

		// do estimation for hashmap size and do pre-allocation after 8192 rows.
		if ctx.doPreAllocatedCheck {
			if ctx.alreadyInputRows >= hashmap.HashMapSizeEstimate {

				group := hmp.GroupCount()
				rate := float64(group) / float64(ctx.alreadyInputRows)
				mayCap := uint64(float64(ctx.requireInputRows) * rate)
				if mayCap > group {
					if err := hmp.PreAlloc(mayCap - group); err != nil {
						return err
					}
				}

				ctx.doPreAllocatedCheck = false
			}
		}

		// insertion.
		for j := range keys {
			keys[j] = data.Vecs[spilledHm.hashInfo.keyColumnIdxList[j]]
		}
		_, _, err := itr.Insert(i, n, keys)
		if err != nil {
			return err
		}

		// update the already input count.
		ctx.alreadyInputRows += n
	}

	return nil
}

// Close
// 1. do memory clean for SpilledHashMap.
// 2. remove all its spilled files.
func (spilledHm *SpilledHashMap) Close() {
	for _, block := range spilledHm.blocks {
		_ = spilledHm.srv.Delete(context.TODO(), block.path)
	}
	spilledHm.srv.Close()
}
