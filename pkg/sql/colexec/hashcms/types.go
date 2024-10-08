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
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"runtime"
)

// spilledHashMap splits and spills the data sources of the hash map to the disk.
//
// hashInfo: information for hash map creation.
// spilledBatchInfo: information for spilled file.
type spilledHashMap struct {
	usr context.Context
	srv fileservice.MutableFileService
	uid uuid.UUID

	// HashTable related information.
	hashInfo struct {
		isStrHashMap bool
		keyHasNulls  bool

		// hashOnUniqueColumn is true for hash on primary key and other columns with unique index.
		hashOnUniqueColumn bool

		// totalColumnCount indicates how many vectors should be stored for one spilledBatch.
		totalColumnCount int
		// keyColumnIdxList indicates which columns were hash table keys.
		keyColumnIdxList []int
	}

	// spilled batches.
	blocks []spilledBatchInfo
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

// readBatchByPath read a batch according to its disk path.
func readBatchByPath(str string) *batch.Batch {
	return nil
}

// writeBatchToFileService write a batch to disk.
func (spilledHm *spilledHashMap) writeBatchToFileService(data *batch.Batch) (filePath string, err error) {
	// todo: 使用 fileservice 包下的 mutator 相关.
	// 具体使用方法看 mutable_file_service_test.go.

	path := spilledHm.buildSpilledBatchPath()
	entry := getIOEntryToWriteBatch(data)
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
func (spilledHm *spilledHashMap) buildSpilledBatchPath() string {
	// format: temp_sh_uid_{block_idx}.
	return ""
}

// getIOEntryToWriteBatch
// 1. encode the batch.
// 2. generate an io entry to write all the whole data.
func getIOEntryToWriteBatch(b *batch.Batch) fileservice.IOEntry {
	// todo: bs is from b.
	var bs []byte

	return fileservice.IOEntry{
		Offset: 0,
		Size:   -1,
		Data:   bs,
	}
}

// StoreBatch spill the batch with key column to disk.
//
// 1. create a new dst batch to avoid modifying the src batch.
// 2. evaluate the keys of hash map, and save them to the dst batch.
// 3. spill the dst batch to the disk.
// 4. add spill-batch information to the spilledHashMap.
func (spilledHm *spilledHashMap) StoreBatch(
	proc *process.Process, src *batch.Batch, keyExecutors ...colexec.ExpressionExecutor) error {

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
func (spilledHm *spilledHashMap) ReadHashMapByIdxes(idxes ...int) (hashmap.HashMap, error) {
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
	for _, idx := range idxes {
		b := readBatchByPath(spilledHm.blocks[idx].path)
		if err = spilledHm.insertBatchIntoHashmap(hmp, hmpItr, b, insertContext); err != nil {
			hmp.Free()
			return nil, err
		}

		// todo: 怎么处理b.
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
func (spilledHm *spilledHashMap) insertBatchIntoHashmap(
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
// 1. do memory clean for spilledHashMap.
// 2. remove all its spilled files.
func (spilledHm *spilledHashMap) Close() {
	for _, block := range spilledHm.blocks {
		_ = spilledHm.srv.Delete(context.TODO(), block.path)
	}
}
