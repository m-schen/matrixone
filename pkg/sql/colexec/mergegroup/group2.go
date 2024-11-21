// Copyright 2021 Matrix Origin
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

package mergegroup

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// getInputBatch return the received data from its last operator.
//
// a description about what data does this operator will receive.
// 1. batch with group-by column and agg-middle result of each group.
// 2. batch with only agg-middle result.
// 3. nil batch.
//
// all the agg-middle results take a same chunk size,
// and the row of group-by column is always not more than the size.
func (mergeGroup *MergeGroup) getInputBatch(proc *process.Process) (*batch.Batch, error) {
	r, err := vm.ChildrenCall(mergeGroup.GetChildren(0), proc, mergeGroup.OpAnalyzer)
	return r.Batch, err
}

// consumeInputBatch
// 1. put batch into hashtable, and get a group list.
// 2. union the new rows and update the aggList.
func (mergeGroup *MergeGroup) consumeInputBatch(
	proc *process.Process,
	bat *batch.Batch, res *thisResult) (err error) {
	if bat.IsEmpty() {
		return nil
	}

	if len(res.aggList) == 0 {
		res.aggList, bat.Aggs = bat.Aggs, nil
		res.chunkSize = aggexec.GetChunkSizeOfAggregator(res.aggList[0])
		res.toNext = append(res.toNext, getInitialBatchWithSameType(bat))
	}

	// put into hash table.
	var vals []uint64
	var more int
	count := bat.RowCount()
	for i := 0; i < count; i += hashmap.UnitLimit {
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}

		rowCount := res.hash.GroupCount()
		vals, _, err = res.itr.Insert(i, n, bat.Vecs)
		if err != nil {
			return err
		}
		res.updateInserted(vals, rowCount)
		more, err = res.appendBatch(proc.Mp(), bat, i, res.inserted)
		if err != nil {
			return err
		}
		if err = res.updateAgg(bat, vals, i, len(res.inserted), more); err != nil {
			return err
		}
	}

	return nil
}

type thisResult struct {
	chunkSize int

	// hashmap related structure.
	itr      hashmap.Iterator
	hash     hashmap.HashMap
	inserted []uint8

	// this operator's final result.
	toNext []*batch.Batch
	// the aggregator of this operator.
	aggList []aggexec.AggFuncExec
}

func (r *thisResult) free(m *mpool.MPool) {
	if r.hash != nil {
		r.hash.Free()
		r.hash = nil
	}
	for i := range r.toNext {
		if r.toNext[i] != nil {
			r.toNext[i].Clean(m)
		}
	}
	for i := range r.aggList {
		if r.aggList[i] != nil {
			r.aggList[i].Free()
		}
	}

	r.toNext, r.aggList = nil, nil
}

func (r *thisResult) updateInserted(vals []uint64, groupCountBefore uint64) {
	if cap(r.inserted) < len(vals) {
		r.inserted = make([]uint8, len(vals))
	} else {
		r.inserted = r.inserted[:len(vals)]
	}

	for i, v := range vals {
		if v > groupCountBefore {
			r.inserted[i] = 1
			groupCountBefore++
		}
	}
}

func (r *thisResult) appendBatch(
	mp *mpool.MPool,
	bat *batch.Batch, offset int, insertList []uint8) (count int, err error) {

	space1 := r.chunkSize - r.toNext[len(r.toNext)-1].RowCount()
	more, k := countNonZeroAndFindKth(insertList, space1)

	// if there is not any new row should append, just return.
	if more == 0 {
		return 0, nil
	}

	// try to append rows to multiple batches.
	// 1. fill the last part first.
	if space1 >= more {
		if err = r.unionToSpecificBatch(mp, len(r.toNext)-1, bat, int64(offset), insertList, more); err != nil {
			return 0, err
		}
		return 0, nil
	}
	if err = r.unionToSpecificBatch(mp, len(r.toNext)-1, bat, int64(offset), insertList[:k+1], space1); err != nil {
		return 0, err
	}

	// 2. add a new batch to continue the append action.
	r.toNext = append(r.toNext, getInitialBatchWithSameType(bat))
	_, err = r.appendBatch(mp, bat, offset+k+1, insertList[k+1:])

	return more, err
}

func (r *thisResult) updateAgg(
	bat *batch.Batch,
	vals []uint64, offset int, length int,
	moreGroup int) error {
	if len(bat.Aggs) == 0 {
		return nil
	}

	for i := range r.aggList {
		if err := r.aggList[i].GroupGrow(moreGroup); err != nil {
			return err
		}
	}

	for i := range r.aggList {
		if err := r.aggList[i].BatchMerge(bat.Aggs[i], offset, vals[:length]); err != nil {
			return err
		}
	}
	return nil
}

func countNonZeroAndFindKth(values []uint8, k int) (int, int) {
	count := 0
	kth := -1

	for i, v := range values {
		if v == 0 {
			continue
		}

		count++
		if count == k {
			kth = i
			break
		}
	}

	for i := kth + 1; i < len(values); i++ {
		if values[i] == 0 {
			continue
		}
		count++
	}
	return count, kth
}

func (r *thisResult) unionToSpecificBatch(
	mp *mpool.MPool,
	idx int, bat *batch.Batch, offset int64, insertList []uint8, rowIncrease int) error {
	for i, vec := range r.toNext[idx].Vecs {
		if err := vec.UnionBatch(bat.Vecs[i], offset, len(insertList), insertList, mp); err != nil {
			return err
		}
	}
	r.toNext[idx].AddRowCount(rowIncrease)
	return nil
}

func getInitialBatchWithSameType(src *batch.Batch) *batch.Batch {
	b := batch.NewOffHeapWithSize(len(src.Vecs))
	for i := range b.Vecs {
		b.Vecs[i] = vector.NewOffHeapVecWithType(*src.Vecs[i].GetType())
	}
	b.SetRowCount(0)
	return b
}
