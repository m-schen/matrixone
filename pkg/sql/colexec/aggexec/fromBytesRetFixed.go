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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

var _ AggFuncExec = &singleAggFuncExecNew3[bool]{}

// for sca.
var _ = RegisterAggFromBytesRetFixed[bool]

func RegisterAggFromBytesRetFixed[to types.FixedSizeTExceptStrType](
	basicInformation SingleColumnAggInformation,
	initCommonContext AggCommonContextInit,
	initGroupContext AggGroupContextInit,
	initResult SingleAggInitResultFixed[to],
	fill SingleAggFill3NewVersion[to],
	fills SingleAggFills3NewVersion[to],
	merge SingleAggMerge3NewVersion[to],
	flush SingleAggFlush3NewVersion[to]) {

	key := generateKeyOfSingleColumnAgg(basicInformation.id, basicInformation.arg)
	if _, ok := registeredAggFunctions[key]; ok {
		panic(fmt.Sprintf("agg function with id %d and arg %s has been registered", basicInformation.id, basicInformation.arg))
	}

	impl := aggImplementation{
		registeredAggInfo: registeredAggInfo{
			isSingleAgg:          true,
			acceptNull:           false,
			setNullForEmptyGroup: basicInformation.setNullForEmptyGroup,
		},

		ret: basicInformation.ret,

		ctx: aggContextImplementation{
			hasCommonContext:      initCommonContext != nil,
			hasGroupContext:       initGroupContext != nil,
			generateCommonContext: initCommonContext,
			generateGroupContext:  initGroupContext,
		},

		logic: aggLogicImplementation{
			init:  initResult,
			fill:  fill,
			fills: fills,
			merge: merge,
			flush: flush,
		},
	}
	if initResult == nil {
		impl.logic.init = nil
	}
	if flush == nil {
		impl.logic.flush = nil
	}

	registeredAggFunctions[key] = impl

	singleAgg[basicInformation.id] = true
}

// newSingleAggFuncExec3NewVersion creates a singleAggFuncExecNew3 from the agg information.
func newSingleAggFuncExec3NewVersion(
	mg AggMemoryManager, info singleAggInfo, impl aggImplementation) AggFuncExec {
	switch info.retType.Oid {
	case types.T_bool:
		e := &singleAggFuncExecNew3[bool]{}
		e.init(mg, info, impl)
		return e
	case types.T_bit:
		e := &singleAggFuncExecNew3[uint64]{}
		e.init(mg, info, impl)
		return e
	case types.T_int8:
		e := &singleAggFuncExecNew3[int8]{}
		e.init(mg, info, impl)
		return e
	case types.T_int16:
		e := &singleAggFuncExecNew3[int16]{}
		e.init(mg, info, impl)
		return e
	case types.T_int32:
		e := &singleAggFuncExecNew3[int32]{}
		e.init(mg, info, impl)
		return e
	case types.T_int64:
		e := &singleAggFuncExecNew3[int64]{}
		e.init(mg, info, impl)
		return e
	case types.T_uint8:
		e := &singleAggFuncExecNew3[uint8]{}
		e.init(mg, info, impl)
		return e
	case types.T_uint16:
		e := &singleAggFuncExecNew3[uint16]{}
		e.init(mg, info, impl)
		return e
	case types.T_uint32:
		e := &singleAggFuncExecNew3[uint32]{}
		e.init(mg, info, impl)
		return e
	case types.T_uint64:
		e := &singleAggFuncExecNew3[uint64]{}
		e.init(mg, info, impl)
		return e
	case types.T_float32:
		e := &singleAggFuncExecNew3[float32]{}
		e.init(mg, info, impl)
		return e
	case types.T_float64:
		e := &singleAggFuncExecNew3[float64]{}
		e.init(mg, info, impl)
		return e
	case types.T_decimal64:
		e := &singleAggFuncExecNew3[types.Decimal64]{}
		e.init(mg, info, impl)
		return e
	case types.T_decimal128:
		e := &singleAggFuncExecNew3[types.Decimal128]{}
		e.init(mg, info, impl)
		return e
	case types.T_date:
		e := &singleAggFuncExecNew3[types.Date]{}
		e.init(mg, info, impl)
		return e
	case types.T_datetime:
		e := &singleAggFuncExecNew3[types.Datetime]{}
		e.init(mg, info, impl)
		return e
	case types.T_time:
		e := &singleAggFuncExecNew3[types.Time]{}
		e.init(mg, info, impl)
		return e
	case types.T_timestamp:
		e := &singleAggFuncExecNew3[types.Timestamp]{}
		e.init(mg, info, impl)
		return e
	case types.T_TS:
		e := &singleAggFuncExecNew3[types.TS]{}
		e.init(mg, info, impl)
		return e
	case types.T_Rowid:
		e := &singleAggFuncExecNew3[types.Rowid]{}
		e.init(mg, info, impl)
		return e
	case types.T_Blockid:
		e := &singleAggFuncExecNew3[types.Blockid]{}
		e.init(mg, info, impl)
		return e
	case types.T_uuid:
		e := &singleAggFuncExecNew3[types.Uuid]{}
		e.init(mg, info, impl)
		return e
	case types.T_enum:
		e := &singleAggFuncExecNew3[types.Enum]{}
		e.init(mg, info, impl)
		return e
	}
	panic(fmt.Sprintf("unsupported result type %s for singleAggFuncExec3", info.retType))
}

// singleAggFuncExecNew3 is the aggregation executor for single aggregation function
// with a variable-length input and a fixed-length return value.
type singleAggFuncExecNew3[to types.FixedSizeTExceptStrType] struct {
	singleAggInfo
	singleAggExecExtraInformation
	distinctHash

	arg sBytesArg
	ret aggResultWithFixedType[to]

	execContext *AggContext

	fill  SingleAggFill3NewVersion[to]
	fills SingleAggFills3NewVersion[to]
	merge SingleAggMerge3NewVersion[to]
	flush SingleAggFlush3NewVersion[to]
}

func (exec *singleAggFuncExecNew3[to]) marshal() ([]byte, error) {
	d := exec.singleAggInfo.getEncoded()
	r, em, err := exec.ret.marshalToBytes()
	if err != nil {
		return nil, err
	}
	encoded := &EncodedAgg{
		Info:    d,
		Result:  r,
		Empties: em,
		Groups:  exec.execContext.getGroupContextEncodings(),
	}
	return encoded.Marshal()
}

func (exec *singleAggFuncExecNew3[to]) unmarshal(mp *mpool.MPool, result, empties, groups [][]byte) error {
	exec.execContext.decodeGroupContexts(groups, exec.singleAggInfo.retType, exec.singleAggInfo.argType)
	return exec.ret.unmarshalFromBytes(result, empties)
}

func (exec *singleAggFuncExecNew3[to]) init(
	mg AggMemoryManager,
	info singleAggInfo,
	impl aggImplementation) {

	if info.IsDistinct() {
		exec.distinctHash = newDistinctHash(mg.Mp(), false)
	}

	var v to
	if resultInitMethod := impl.logic.init; resultInitMethod != nil {
		v = resultInitMethod.(SingleAggInitResultFixed[to])(info.retType, info.argType)
	}
	exec.ret = initAggResultWithFixedTypeResult[to](mg, info.retType, info.emptyNull, v)

	exec.singleAggInfo = info
	exec.singleAggExecExtraInformation = emptyExtraInfo
	exec.execContext = newAggContextFromImpl(impl.ctx, info.retType, info.argType)

	if flushMethod := impl.logic.flush; flushMethod != nil {
		exec.flush = flushMethod.(SingleAggFlush3NewVersion[to])
	}

	exec.fill = impl.logic.fill.(SingleAggFill3NewVersion[to])
	exec.fills = impl.logic.fills.(SingleAggFills3NewVersion[to])
	exec.merge = impl.logic.merge.(SingleAggMerge3NewVersion[to])
}

func (exec *singleAggFuncExecNew3[to]) GroupGrow(more int) error {
	if err := exec.ret.grows(more); err != nil {
		return err
	}
	// deal with distinct hash.
	if exec.IsDistinct() {
		if err := exec.distinctHash.grows(more); err != nil {
			return err
		}
	}
	// deal with execContext.
	exec.execContext.growsGroupContext(more, exec.singleAggInfo.retType, exec.singleAggInfo.argType)
	return nil
}

func (exec *singleAggFuncExecNew3[to]) PreAllocateGroups(more int) error {
	exec.execContext.preAllocate(more)
	return exec.ret.preExtend(more)
}

func (exec *singleAggFuncExecNew3[to]) Fill(
	group int, row int, vectors []*vector.Vector) error {
	if vectors[0].IsNull(uint64(row)) {
		return nil
	}

	if vectors[0].IsConst() {
		row = 0
	}

	if exec.IsDistinct() {
		if need, err := exec.distinctHash.fill(group, vectors, row); !need || err != nil {
			return err
		}
	}

	x, y := exec.ret.updateNextAccessIdx(group)
	err := exec.fill(
		exec.execContext.getGroupContext(group),
		exec.execContext.getCommonContext(),
		vectors[0].GetBytesAt(row),
		exec.ret.isGroupEmpty(x, y),
		exec.ret.get, exec.ret.set)
	exec.ret.setGroupNotEmpty(x, y)
	return err
}

func (exec *singleAggFuncExecNew3[to]) BulkFill(
	group int, vectors []*vector.Vector) error {
	length := vectors[0].Length()
	if length == 0 || vectors[0].IsConstNull() {
		return nil
	}

	if exec.IsDistinct() {
		return exec.distinctBulkFill(group, vectors, length)
	}

	x, y := exec.ret.updateNextAccessIdx(group)
	getter := exec.ret.get
	setter := exec.ret.set
	groupContext := exec.execContext.getGroupContext(group)
	commonContext := exec.execContext.getCommonContext()

	if vectors[0].IsConst() {
		err := exec.fills(
			groupContext,
			commonContext,
			vectors[0].GetBytesAt(0),
			length, exec.ret.isGroupEmpty(x, y),
			getter, setter)
		exec.ret.setGroupNotEmpty(x, y)
		return err
	}

	exec.arg.prepare(vectors[0])
	bs := exec.ret.getEmptyListOnX(x)
	if exec.arg.w.WithAnyNullValue() {
		for i, j := uint64(0), uint64(length); i < j; i++ {
			v, null := exec.arg.w.GetStrValue(i)
			if !null {
				if err := exec.fill(groupContext, commonContext, v, bs[y], getter, setter); err != nil {
					return err
				}
				exec.ret.setGroupNotEmpty(x, y)
			}
		}
		return nil
	}

	for i, j := uint64(0), uint64(length); i < j; i++ {
		v, _ := exec.arg.w.GetStrValue(i)
		if err := exec.fill(groupContext, commonContext, v, bs[y], getter, setter); err != nil {
			return err
		}
		exec.ret.setGroupNotEmpty(x, y)
	}
	return nil
}

func (exec *singleAggFuncExecNew3[to]) distinctBulkFill(
	group int, vectors []*vector.Vector, length int) error {
	x, y := exec.ret.updateNextAccessIdx(group)
	getter := exec.ret.get
	setter := exec.ret.set
	groupContext := exec.execContext.getGroupContext(group)
	commonContext := exec.execContext.getCommonContext()

	if vectors[0].IsConst() {
		if need, err := exec.distinctHash.fill(group, vectors, 0); !need || err != nil {
			return err
		}
		err := exec.fill(groupContext, commonContext, vectors[0].GetBytesAt(0), exec.ret.isGroupEmpty(x, y), getter, setter)
		exec.ret.setGroupNotEmpty(x, y)
		return err
	}

	exec.arg.prepare(vectors[0])
	needs, err := exec.distinctHash.bulkFill(group, vectors)
	if err != nil {
		return err
	}

	bs := exec.ret.getEmptyListOnX(x)
	if exec.arg.w.WithAnyNullValue() {
		for i, j := uint64(0), uint64(length); i < j; i++ {
			if needs[i] {
				v, null := exec.arg.w.GetStrValue(i)
				if !null {
					if err = exec.fill(groupContext, commonContext, v, bs[y], getter, setter); err != nil {
						return err
					}
					exec.ret.setGroupNotEmpty(x, y)
				}
			}
		}
		return nil
	}

	for i, j := uint64(0), uint64(length); i < j; i++ {
		if needs[i] {
			v, _ := exec.arg.w.GetStrValue(i)
			if err = exec.fill(groupContext, commonContext, v, bs[y], getter, setter); err != nil {
				return err
			}
			exec.ret.setGroupNotEmpty(x, y)
		}
	}
	return nil
}

func (exec *singleAggFuncExecNew3[to]) BatchFill(
	offset int, groups []uint64, vectors []*vector.Vector) error {
	if len(groups) == 0 || vectors[0].IsConstNull() {
		return nil
	}

	if exec.IsDistinct() {
		return exec.distinctBatchFill(offset, groups, vectors)
	}

	getter := exec.ret.get
	setter := exec.ret.set
	commonContext := exec.execContext.getCommonContext()
	bs := exec.ret.getEmptyList()

	if vectors[0].IsConst() {
		value := vectors[0].GetBytesAt(0)
		for _, group := range groups {
			if group != GroupNotMatched {
				idx := int(group - 1)
				x, y := exec.ret.updateNextAccessIdx(idx)
				if err := exec.fill(
					exec.execContext.getGroupContext(idx), commonContext, value, bs[x][y], getter, setter); err != nil {
					return err
				}
				exec.ret.setGroupNotEmpty(x, y)
			}
		}
		return nil
	}

	exec.arg.prepare(vectors[0])
	if exec.arg.w.WithAnyNullValue() {
		for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
			if groups[idx] != GroupNotMatched {
				v, null := exec.arg.w.GetStrValue(i)
				if !null {
					groupIdx := int(groups[idx] - 1)
					x, y := exec.ret.updateNextAccessIdx(groupIdx)
					if err := exec.fill(
						exec.execContext.getGroupContext(groupIdx), commonContext, v, bs[x][y], getter, setter); err != nil {
						return err
					}
					exec.ret.setGroupNotEmpty(x, y)
				}
			}
			idx++
		}
		return nil
	}

	for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
		if groups[idx] != GroupNotMatched {
			v, _ := exec.arg.w.GetStrValue(i)
			groupIdx := int(groups[idx] - 1)
			x, y := exec.ret.updateNextAccessIdx(groupIdx)
			if err := exec.fill(
				exec.execContext.getGroupContext(groupIdx), commonContext, v, bs[x][y], getter, setter); err != nil {
				return err
			}
			exec.ret.setGroupNotEmpty(x, y)
		}
		idx++
	}
	return nil
}

func (exec *singleAggFuncExecNew3[to]) distinctBatchFill(
	offset int, groups []uint64, vectors []*vector.Vector) error {
	getter := exec.ret.get
	setter := exec.ret.set
	commonContext := exec.execContext.getCommonContext()
	bs := exec.ret.getEmptyList()

	needs, err := exec.distinctHash.batchFill(vectors, offset, groups)
	if err != nil {
		return err
	}

	if vectors[0].IsConst() {
		value := vectors[0].GetBytesAt(0)
		for i, group := range groups {
			if needs[i] && group != GroupNotMatched {
				idx := int(group - 1)
				x, y := exec.ret.updateNextAccessIdx(idx)
				if err = exec.fill(
					exec.execContext.getGroupContext(idx), commonContext, value, bs[x][y], getter, setter); err != nil {
					return err
				}
				exec.ret.setGroupNotEmpty(x, y)
			}
		}
		return nil
	}

	exec.arg.prepare(vectors[0])
	if exec.arg.w.WithAnyNullValue() {
		for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
			if needs[idx] && groups[idx] != GroupNotMatched {
				v, null := exec.arg.w.GetStrValue(i)
				if !null {
					groupIdx := int(groups[idx] - 1)
					x, y := exec.ret.updateNextAccessIdx(groupIdx)
					if err = exec.fill(
						exec.execContext.getGroupContext(groupIdx), commonContext, v, bs[x][y], getter, setter); err != nil {
						return err
					}
					exec.ret.setGroupNotEmpty(x, y)
				}
			}
			idx++
		}
		return nil
	}

	for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
		if needs[idx] && groups[idx] != GroupNotMatched {
			v, _ := exec.arg.w.GetStrValue(i)
			groupIdx := int(groups[idx] - 1)
			x, y := exec.ret.updateNextAccessIdx(groupIdx)
			if err = exec.fill(
				exec.execContext.getGroupContext(groupIdx), commonContext, v, bs[x][y], getter, setter); err != nil {
				return err
			}
			exec.ret.setGroupNotEmpty(x, y)
		}
		idx++
	}
	return nil
}

func (exec *singleAggFuncExecNew3[to]) Flush() (*vector.Vector, error) {
	getter := exec.ret.get
	setter := exec.ret.set
	commonContext := exec.execContext.getCommonContext()

	if exec.partialResult != nil {
		if value, ok := exec.partialResult.([]byte); ok {
			x, y := exec.ret.updateNextAccessIdx(exec.partialGroup)
			if err := exec.fill(
				exec.execContext.getGroupContext(exec.partialGroup), commonContext, value, exec.ret.isGroupEmpty(x, y), getter, setter); err != nil {
				return nil, err
			}
			exec.ret.setGroupNotEmpty(x, y)
		}
	}

	if exec.flush != nil {
		lim := exec.ret.getEachBlockLimitation()
		groups := exec.ret.totalGroupCount()

		if exec.ret.optInformation.shouldSetNullToEmptyGroup {
			for i, x := 0, 0; i < groups; i += lim {
				n := groups - i
				if n > lim {
					n = lim
				}

				for j, k := 0, i; j < n; j++ {
					if exec.ret.isGroupEmpty(x, j) {
						k++
						continue
					}
					exec.ret.setNextAccessDirectly(x, j)

					if err := exec.flush(exec.execContext.getGroupContext(k), commonContext, getter, setter); err != nil {
						return nil, err
					}
					k++
				}
				x++
			}

		} else {
			for i, x := 0, 0; i < groups; i += lim {
				n := groups - i
				if n > lim {
					n = lim
				}

				for j, k := 0, i; j < n; j++ {
					exec.ret.setNextAccessDirectly(x, j)

					if err := exec.flush(exec.execContext.getGroupContext(k), commonContext, getter, setter); err != nil {
						return nil, err
					}
					k++
				}
				x++
			}
		}
	}

	return exec.ret.flushAll()[0], nil
}

func (exec *singleAggFuncExecNew3[to]) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	other := next.(*singleAggFuncExecNew3[to])

	x1, y1 := exec.ret.updateNextAccessIdx(groupIdx1)
	x2, y2 := other.ret.updateNextAccessIdx(groupIdx2)
	isNextEmpty := other.ret.isGroupEmpty(x2, y2)

	if err := exec.merge(
		exec.execContext.getGroupContext(groupIdx1),
		other.execContext.getGroupContext(groupIdx2),
		exec.execContext.getCommonContext(),
		exec.ret.isGroupEmpty(x1, y1),
		isNextEmpty,
		exec.ret.get, other.ret.get, exec.ret.set); err != nil {
		return err
	}
	exec.ret.MergeAnotherEmpty(x1, x2, isNextEmpty)

	return exec.distinctHash.merge(&other.distinctHash)
}

func (exec *singleAggFuncExecNew3[to]) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*singleAggFuncExecNew3[to])
	getter1 := exec.ret.get
	getter2 := other.ret.get
	setter := exec.ret.set
	commonContext := exec.execContext.getCommonContext()

	for i := range groups {
		if groups[i] == GroupNotMatched {
			continue
		}
		groupIdx1, groupIdx2 := int(groups[i]-1), i+offset

		x1, y1 := exec.ret.updateNextAccessIdx(groupIdx1)
		x2, y2 := other.ret.updateNextAccessIdx(groupIdx2)
		isNextEmpty := other.ret.isGroupEmpty(x2, y2)

		if err := exec.merge(
			exec.execContext.getGroupContext(groupIdx1),
			other.execContext.getGroupContext(groupIdx2),
			commonContext,
			exec.ret.isGroupEmpty(x1, y1),
			isNextEmpty,
			getter1, getter2,
			setter); err != nil {
			return err
		}
		exec.ret.MergeAnotherEmpty(x1, y1, isNextEmpty)
	}

	return exec.distinctHash.merge(&other.distinctHash)
}

func (exec *singleAggFuncExecNew3[to]) Free() {
	exec.ret.free()
	exec.distinctHash.free()
}
