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

package aggexec

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

const (
	resultElementRowSize = 131072 // 2^17
)

var (
	aggEmptiesType = types.T_bool.ToType()
)

type aggBasicCommonResult struct {
	// where to allocate and release memory.
	mg AggMemoryManager
	mp *mpool.MPool

	// agg result type.
	typ types.Type

	// indicate that if we should set null to the group without any value.
	setNullToEmptyGroup bool

	// the result and empty situations of aggregation.
	// we use the slice to store them, and each element in the slice cannot more than resultElementRowSize.
	results []*vector.Vector
	empties []*vector.Vector
	// length is the max group number for now.
	// capacity is the max group number we can store.
	length, capacity int

	// the index of the group to get and set.
	// the idx2 row of results[idx1] and empties[idx1] is the result and empty situation of the group.
	idx1, idx2 int
	// quickEmpty is the pointer of empties, for quick get and set from empties.
	quickEmpty [][]bool
}

func (r *aggBasicCommonResult) init(
	mg AggMemoryManager, resultType types.Type, isEmptyGroupToBeNull bool) {
	r.typ = resultType
	r.setNullToEmptyGroup = isEmptyGroupToBeNull
	r.length, r.capacity = 0, 0
	r.mg = mg

	r.results = make([]*vector.Vector, 1)
	r.empties = make([]*vector.Vector, 1)
	if r.mg == nil {
		return
	}

	r.mp = mg.Mp()
}

// groupNumber starts from 0.
func getIdx1Idx2(groupNumber int) (int, int) {
	return groupNumber / resultElementRowSize, groupNumber % resultElementRowSize
}

func updateQuickVs[T types.FixedSizeTExceptStrType](old [][]T, src []*vector.Vector, defaultValue T) [][]T {
	if len(old) == 0 {
		ret := make([][]T, len(src))
		for i := range ret {
			ret[i] = vector.MustFixedCol[T](src[i])
			for j := range ret[i] {
				ret[i][j] = defaultValue
			}
		}
		return ret
	}

	lastIndex := len(old) - 1
	lastRow := len(old[lastIndex]) - 1

	old[lastIndex] = vector.MustFixedCol[T](src[lastIndex])
	for i := lastRow + 1; i < len(old[lastIndex]); i++ {
		old[lastIndex][i] = defaultValue
	}
	for i := lastIndex + 1; i < len(src); i++ {
		vs := vector.MustFixedCol[T](src[i])
		for j := range vs {
			vs[j] = defaultValue
		}

		old = append(old, vs)
	}
	return old
}

func safeCleanVectors(mg AggMemoryManager, vectors []*vector.Vector) {
	mp := mg.Mp()
	for _, v := range vectors {
		if v == nil {
			continue
		}

		if v.NeedDup() {
			v.Free(mp)
		} else {
			mg.PutVector(v)
		}
	}
}

func (r *aggBasicCommonResult) getEncodedAggResult() (EncodeAggResult, error) {
	e := EncodeAggResult{
		Length:   int64(r.length),
		Capacity: int64(r.capacity),
		Results:  make([][]byte, len(r.results)),
		Empties:  make([][]byte, len(r.empties)),
	}

	// set result.
	var err error
	for i := range e.Results {
		if e.Results[i], err = r.results[i].MarshalBinary(); err != nil {
			return e, err
		}
	}
	for i := range e.Empties {
		if e.Empties[i], err = r.empties[i].MarshalBinary(); err != nil {
			return e, err
		}
	}
	return e, nil
}

func (r *aggBasicCommonResult) decodeFromEncodedAggResult(e *EncodeAggResult) error {
	r.length, r.capacity = int(e.Length), int(e.Capacity)

	var err error = nil
	defer func() {
		if err != nil {
			r.free()
		}
	}()

	var mp *mpool.MPool = nil
	if r.mg != nil {
		mp = r.mg.Mp()
	}

	r.results = make([]*vector.Vector, len(r.results))
	for i := range r.results {
		r.results[i] = vector.NewVec(r.typ)
		if err = vectorUnmarshal(r.results[i], e.Results[i], mp); err != nil {
			return err
		}
	}

	r.empties = make([]*vector.Vector, len(r.empties))
	r.quickEmpty = make([][]bool, len(r.empties))
	for i := range r.empties {
		r.empties[i] = vector.NewVec(r.typ)
		if err = vectorUnmarshal(r.empties[i], e.Empties[i], mp); err != nil {
			return err
		}
		r.quickEmpty[i] = vector.MustFixedCol[bool](r.empties[i])
	}
	return nil
}

func (r *aggBasicCommonResult) setGroupNumber(groupNumber int) {
	r.idx1, r.idx2 = getIdx1Idx2(groupNumber)
}

func (r *aggBasicCommonResult) totalGroup() (int, int) {
	return r.length, r.capacity
}

// empty situation related.
func (r *aggBasicCommonResult) isEmpty(groupIndex int) bool {
	i, j := getIdx1Idx2(groupIndex)
	return r.quickEmpty[i][j]
}

func (r *aggBasicCommonResult) setNotEmpty(groupIndex int) {
	i, j := getIdx1Idx2(groupIndex)
	r.quickEmpty[i][j] = false
}

// extend related.
func (r *aggBasicCommonResult) preAllocate(more int) (err error) {
	if more == 0 {
		return
	}
	mp := r.mp
	if mp == nil {
		panic("cannot allocate memory for agg without any mpool.")
	}

	// 0. if the result and empties are nil, init them first.
	if len(r.results) == 0 {
		r.results[0] = r.mg.GetVector(r.typ)
		r.empties[0] = r.mg.GetVector(aggEmptiesType)
	}

	// 1. append the last element if the last element is not full.
	srcLastIndex := len(r.results) - 1
	lastElementLen := r.results[srcLastIndex].Length()

	more1 := resultElementRowSize - lastElementLen
	if more2 := more - more1; more2 <= 0 {
		if err = r.results[srcLastIndex].PreExtend(more, mp); err != nil {
			return err
		}
		if err = r.empties[srcLastIndex].PreExtend(more, mp); err != nil {
			return err
		}
		r.results[srcLastIndex].SetLength(r.results[srcLastIndex].Length() + more)
		r.empties[srcLastIndex].SetLength(r.empties[srcLastIndex].Length() + more)
	} else {

		if err = r.results[srcLastIndex].PreExtend(more1, mp); err != nil {
			return err
		}
		if err = r.empties[srcLastIndex].PreExtend(more1, mp); err != nil {
			return err
		}
		r.results[srcLastIndex].SetLength(resultElementRowSize)
		r.empties[srcLastIndex].SetLength(resultElementRowSize)

		// 2. append the new element if it still needs more space.
		fixedVectorNumber, nonFixedRowNumber := more2/resultElementRowSize, more2%resultElementRowSize

		for i := 0; i < fixedVectorNumber; i++ {
			r.results = append(r.results, r.mg.GetVector(r.typ))
			r.empties = append(r.results, r.mg.GetVector(aggEmptiesType))

			if err = r.results[len(r.results)-1].PreExtend(resultElementRowSize, mp); err != nil {
				return err
			}
			if err = r.empties[len(r.empties)-1].PreExtend(resultElementRowSize, mp); err != nil {
				return err
			}
			r.results[len(r.results)-1].SetLength(resultElementRowSize)
			r.empties[len(r.empties)-1].SetLength(resultElementRowSize)
		}

		if nonFixedRowNumber > 0 {
			r.results = append(r.results, r.mg.GetVector(r.typ))
			r.empties = append(r.results, r.mg.GetVector(aggEmptiesType))

			if err = r.results[len(r.results)-1].PreExtend(nonFixedRowNumber, mp); err != nil {
				return err
			}
			if err = r.empties[len(r.empties)-1].PreExtend(nonFixedRowNumber, mp); err != nil {
				return err
			}
			r.results[len(r.results)-1].SetLength(nonFixedRowNumber)
			r.empties[len(r.empties)-1].SetLength(nonFixedRowNumber)
		}
	}

	r.capacity += more
	return nil
}

func (r *aggBasicCommonResult) extend(more int) (err error) {
	need := r.length + more
	if more2 := need - r.capacity; more2 > 0 {
		if err = r.preAllocate(more2); err != nil {
			return err
		}
	}

	// set quick empty from old length to new length.
	r.quickEmpty = updateQuickVs(r.quickEmpty, r.empties, true)

	r.length = need
	return nil
}

// flush related.
func (r *aggBasicCommonResult) flushOnePart() *vector.Vector {
	if len(r.results) == 0 {
		return nil
	}

	if r.setNullToEmptyGroup {
		emptySituation := r.quickEmpty[0]

		nsp := nulls.NewWithSize(len(emptySituation))
		for i, j := uint64(0), uint64(len(emptySituation)); i < j; i++ {
			if emptySituation[i] {
				nsp.Add(i)
			}
		}
		r.results[0].SetNulls(nsp)
	}

	result := r.results[0]
	if len(r.results) > 1 {
		r.results = r.results[1:]
	} else {
		r.results = nil
	}
	if len(r.empties) > 1 {
		r.empties = r.empties[1:]
	} else {
		r.empties = nil
	}

	return result
}

func (r *aggBasicCommonResult) free() {
	// if not from any memory pool (unmarshal will cause this), just return.
	if r.mg == nil {
		return
	}
	safeCleanVectors(r.mg, r.results)
	safeCleanVectors(r.mg, r.empties)
}

// agg result which was a fixed-length type.
type aggFixedTypeResult[T types.FixedSizeTExceptStrType] struct {
	aggBasicCommonResult
	requireInit    bool
	requiredResult T

	// quickValue is the pointer of results, for quick get and set from aggBasicCommonResult.results.
	quickValue [][]T
}

func makeAggFixedTypeResult1[T types.FixedSizeTExceptStrType](
	mg AggMemoryManager, typ types.Type, setNullToEmptyGroup bool) aggFixedTypeResult[T] {
	r := aggFixedTypeResult[T]{}
	r.init(mg, typ, setNullToEmptyGroup)
	r.requireInit = false
	return r
}

func makeAggFixedTypeResult2[T types.FixedSizeTExceptStrType](
	mg AggMemoryManager, typ types.Type, setNullToEmptyGroup bool, defaultValue T) aggFixedTypeResult[T] {
	r := aggFixedTypeResult[T]{}
	r.init(mg, typ, setNullToEmptyGroup)
	r.requireInit = true
	r.requiredResult = defaultValue
	return r
}

func (r *aggFixedTypeResult[T]) marshal() ([]byte, error) {
	encoded, err := r.getEncodedAggResult()
	if err != nil {
		return nil, err
	}
	encoded.RequireInitResult = r.requireInit
	if r.requireInit {
		encoded.RequiredResult = types.EncodeFixed[T](r.requiredResult)
	}
	return encoded.Marshal()
}

func (r *aggFixedTypeResult[T]) unmarshal(data []byte) error {
	encoded := &EncodeAggResult{}
	if err := encoded.Unmarshal(data); err != nil {
		return err
	}

	if err := r.aggBasicCommonResult.decodeFromEncodedAggResult(encoded); err != nil {
		return err
	}
	r.requireInit = encoded.RequireInitResult
	if r.requireInit {
		r.requiredResult = types.DecodeFixed[T](encoded.RequiredResult)
	}

	r.quickValue = make([][]T, len(r.aggBasicCommonResult.results))
	for i := range r.quickValue {
		r.quickValue[i] = vector.MustFixedCol[T](r.aggBasicCommonResult.results[i])
	}
	return nil
}

func (r *aggFixedTypeResult[T]) grows(more int) error {
	if err := r.aggBasicCommonResult.extend(more); err != nil {
		return err
	}
	// set quick value from oldLength to newLength.
	var v T
	if r.requireInit {
		v = r.requiredResult
	}
	r.quickValue = updateQuickVs(r.quickValue, r.results, v)

	return nil
}

func (r *aggFixedTypeResult[T]) getAggResultByIdx(group int) T {
	idx1, idx2 := getIdx1Idx2(group)
	return r.quickValue[idx1][idx2]
}

func (r *aggFixedTypeResult[T]) setAggResultByIdx(group int, v T) {
	idx1, idx2 := getIdx1Idx2(group)
	r.quickValue[idx1][idx2] = v
}

func (r *aggFixedTypeResult[T]) setAggResultByInnerIdx(v T) {
	r.quickValue[r.idx1][r.idx2] = v
}

func (r *aggFixedTypeResult[T]) getAggResultByInnerIdx() T {
	return r.quickValue[r.idx1][r.idx2]
}

// agg result which was a var-length type.
type aggBytesTypeResult struct {
	aggBasicCommonResult
	requireInit    bool
	requiredResult []byte
}

func makeAggBytesTypeResult1(
	mg AggMemoryManager, typ types.Type, setNullToEmptyGroup bool) aggBytesTypeResult {
	r := aggBytesTypeResult{}
	r.init(mg, typ, setNullToEmptyGroup)
	r.requireInit = false
	r.requiredResult = nil
	return r
}

func makeAggBytesTypeResult2(
	mg AggMemoryManager, typ types.Type, setNullToEmptyGroup bool, defaultValue []byte) aggBytesTypeResult {
	r := aggBytesTypeResult{}
	r.init(mg, typ, setNullToEmptyGroup)
	r.requireInit = true
	r.requiredResult = defaultValue
	return r
}

func (r *aggBytesTypeResult) marshal() ([]byte, error) {
	encoded, err := r.getEncodedAggResult()
	if err != nil {
		return nil, err
	}
	encoded.RequireInitResult = r.requireInit
	if r.requireInit {
		encoded.RequiredResult = r.requiredResult
	}
	return encoded.Marshal()
}

func (r *aggBytesTypeResult) unmarshal(data []byte) error {
	encoded := &EncodeAggResult{}
	if err := encoded.Unmarshal(data); err != nil {
		return err
	}

	if err := r.aggBasicCommonResult.decodeFromEncodedAggResult(encoded); err != nil {
		return err
	}
	r.requiredResult = encoded.RequiredResult
	r.requireInit = encoded.RequireInitResult
	return nil
}

func (r *aggBytesTypeResult) grows(more int) error {
	oldLength := r.length

	if err := r.aggBasicCommonResult.extend(more); err != nil {
		return err
	}

	mp := r.mp
	lastIndex, lastRow := getIdx1Idx2(oldLength)
	lastVec := r.results[lastIndex]

	if r.requireInit {
		v := r.requiredResult

		for m, n := lastRow, lastVec.Length(); m < n; m++ {
			if err := vector.SetBytesAt(lastVec, m, v, mp); err != nil {
				return err
			}
		}

		for i := lastIndex + 1; i < len(r.results); i++ {
			for m, n := 0, r.results[i].Length(); m < n; m++ {
				if err := vector.SetBytesAt(r.results[i], m, v, mp); err != nil {
					return err
				}
			}
		}
	} else {
		v := []byte("")

		for m, n := lastRow, lastVec.Length(); m < n; m++ {
			_ = vector.SetBytesAt(lastVec, m, v, mp)
		}

		for i := lastIndex + 1; i < len(r.results); i++ {
			for m, n := 0, r.results[i].Length(); m < n; m++ {
				_ = vector.SetBytesAt(r.results[i], m, v, mp)
			}
		}
	}

	return nil
}

func (r *aggBytesTypeResult) getAggResultByIdx(group int) []byte {
	idx1, idx2 := getIdx1Idx2(group)
	return r.results[idx1].GetBytesAt(idx2)
}

func (r *aggBytesTypeResult) setAggResultByIdx(group int, v []byte) error {
	idx1, idx2 := getIdx1Idx2(group)
	return vector.SetBytesAt(r.results[idx1], idx2, v, r.mp)
}

func (r *aggBytesTypeResult) setAggResultByInnerIdx(v []byte) error {
	return vector.SetBytesAt(r.results[r.idx1], r.idx2, v, r.mp)
}

func (r *aggBytesTypeResult) getAggResultByInnerIdx() []byte {
	return r.results[r.idx1].GetBytesAt(r.idx2)
}
