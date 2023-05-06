// Copyright 2021 - 2022 Matrix Origin
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

package function2

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function2/function2Util"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"strings"
	"time"
	"unsafe"
)

func builtInDateDiff(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[types.Date](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[types.Date](parameters[1])
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if null1 || null2 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(int64(v1-v2), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func builtInCurrentTimestamp(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Timestamp](result)

	resultValue := types.UnixNanoToTimestamp(proc.UnixTime)
	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.Append(resultValue, false); err != nil {
			return err
		}
	}

	return nil
}

const (
	onUpdateExpr = iota
	defaultExpr
	typNormal
	typWithLen
)

func builtInMoShowVisibleBin(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[uint8](parameters[1])

	tp, null := p2.GetValue(0)
	if null {
		return moerr.NewNotSupported(proc.Ctx, "show visible bin, the second argument must be in [0, 3], but got NULL")
	}
	if tp > 3 {
		return moerr.NewNotSupported(proc.Ctx, fmt.Sprintf("show visible bin, the second argument must be in [0, 3], but got %d", tp))
	}

	var f func(s []byte) ([]byte, error)
	rs := vector.MustFunctionResult[types.Varlena](result)
	switch tp {
	case onUpdateExpr:
		f = func(s []byte) ([]byte, error) {
			update := new(plan.OnUpdate)
			err := types.Decode(s, update)
			if err != nil {
				return nil, err
			}
			return function2Util.QuickStrToBytes(update.OriginString), nil
		}
	case defaultExpr:
		f = func(s []byte) ([]byte, error) {
			def := new(plan.Default)
			err := types.Decode(s, def)
			if err != nil {
				return nil, err
			}
			return function2Util.QuickStrToBytes(def.OriginString), nil
		}
	case typNormal:
		f = func(s []byte) ([]byte, error) {
			typ := new(types.Type)
			err := types.Decode(s, typ)
			if err != nil {
				return nil, err
			}
			return function2Util.QuickStrToBytes(typ.String()), nil
		}
	case typWithLen:
		f = func(s []byte) ([]byte, error) {
			typ := new(types.Type)
			err := types.Decode(s, typ)
			if err != nil {
				return nil, err
			}
			ret := fmt.Sprintf("%s(%d)", typ.String(), typ.Width)
			return function2Util.QuickStrToBytes(ret), nil
		}
	}

	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		if null1 || len(v1) == 0 {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			b, err := f(v1)
			if err != nil {
				return err
			}
			if b == nil {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else {
				if err = rs.AppendBytes(b, false); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func builtInInternalCharLength(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetStrValue(i)
		if !null {
			typ := types.Type{}
			if err := types.Decode(v, &typ); err != nil {
				return err
			}
			if typ.Oid.IsMySQLString() {
				if err := rs.Append(int64(typ.Width), false); err != nil {
					return err
				}
				continue
			}
		}
		if err := rs.Append(0, true); err != nil {
			return err
		}
	}
	return nil
}

func builtInInternalCharSize(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetStrValue(i)
		if !null {
			typ := types.Type{}
			if err := types.Decode(v, &typ); err != nil {
				return err
			}
			if typ.Oid.IsMySQLString() {
				if err := rs.Append(int64(typ.GetSize()*typ.Width), false); err != nil {
					return err
				}
				continue
			}
		}
		if err := rs.Append(0, true); err != nil {
			return err
		}
	}
	return nil
}

func builtInInternalNumericPrecision(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetStrValue(i)
		if !null {
			typ := types.Type{}
			if err := types.Decode(v, &typ); err != nil {
				return err
			}
			if typ.Oid.IsDecimal() {
				if err := rs.Append(int64(typ.Width), false); err != nil {
					return err
				}
				continue
			}
		}
		if err := rs.Append(0, true); err != nil {
			return err
		}
	}
	return nil
}

func builtInInternalNumericScale(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetStrValue(i)
		if !null {
			typ := types.Type{}
			if err := types.Decode(v, &typ); err != nil {
				return err
			}
			if typ.Oid.IsDecimal() {
				if err := rs.Append(int64(typ.Scale), false); err != nil {
					return err
				}
				continue
			}
		}
		if err := rs.Append(0, true); err != nil {
			return err
		}
	}
	return nil
}

func builtInInternalDatetimeScale(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetStrValue(i)
		if !null {
			typ := types.Type{}
			if err := types.Decode(v, &typ); err != nil {
				return err
			}
			if typ.Oid == types.T_datetime {
				if err := rs.Append(int64(typ.Scale), false); err != nil {
					return err
				}
				continue
			}
		}
		if err := rs.Append(0, true); err != nil {
			return err
		}
	}
	return nil
}

func builtInInternalCharacterSet(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetStrValue(i)
		if !null {
			typ := types.Type{}
			if err := types.Decode(v, &typ); err != nil {
				return err
			}
			if typ.Oid == types.T_varchar || typ.Oid == types.T_char ||
				typ.Oid == types.T_blob || typ.Oid == types.T_text {
				if err := rs.Append(int64(typ.Scale), false); err != nil {
					return err
				}
				continue
			}
		}
		if err := rs.Append(0, true); err != nil {
			return err
		}
	}
	return nil
}

func builtInConcatCheck(_ []overload, inputs []types.Type) checkResult {
	if len(inputs) > 1 {
		shouldCast := false

		ret := make([]types.Type, len(inputs))
		for i, source := range inputs {
			if !source.Oid.IsMySQLString() {
				c, _ := tryToMatch([]types.Type{source}, []types.T{types.T_varchar})
				if c == matchFailed {
					return newCheckResultWithFailure(failedFunctionParametersWrong)
				}
				if c == matchByCast {
					shouldCast = true
					ret[i] = types.T_varchar.ToType()
				}
			} else {
				ret[i] = source
			}
		}
		if shouldCast {
			return newCheckResultWithCast(0, ret)
		}
		return newCheckResultWithSuccess(0)
	}
	return newCheckResultWithFailure(failedFunctionParametersWrong)
}

func builtInConcat(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	ps := make([]vector.FunctionParameterWrapper[types.Varlena], len(parameters))
	for i := range ps {
		ps[i] = vector.GenerateFunctionStrParameter(parameters[i])
	}

	for i := uint64(0); i < uint64(length); i++ {
		var vs string
		apv := true

		for _, p := range ps {
			v, null := p.GetStrValue(i)
			if null {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
				apv = false
				break
			} else {
				vs += string(v)
			}
		}
		if apv {
			if err := rs.AppendBytes([]byte(vs), false); err != nil {
				return err
			}
		}
	}
	return nil
}

const (
	formatMask = "%Y/%m/%d"
	regexpMask = `\d{1,4}/\d{1,2}/\d{1,2}`
)

// MOLogDate parse 'YYYY/MM/DD' date from input string.
// return '0001-01-01' if input string not container 'YYYY/MM/DD' substr, until DateParse Function support return NULL for invalid date string.
func builtInMoLogDate(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Date](result)
	p1 := vector.GenerateFunctionStrParameter(parameters[0])

	op := newOpBuiltInRegexp()
	generalTime := NewGeneralTime()
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetStrValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		}
		expr := function2Util.QuickBytesToStr(v)
		match, parsedInput, err := op.regMap.regularSubstr(regexpMask, expr, 1, 1)
		if err != nil {
			return err
		}
		if !match {
			if err = rs.Append(0, true); err != nil {
				return err
			}
		} else {
			generalTime.ResetTime()
			success := coreStrToDate(proc.Ctx, generalTime, parsedInput, formatMask)
			if success && types.ValidDate(int32(generalTime.year), generalTime.month, generalTime.day) {
				val := types.DateFromCalendar(int32(generalTime.year), generalTime.month, generalTime.day)
				if err = rs.Append(val, false); err != nil {
					return err
				}
			} else {
				if err = rs.Append(0, true); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func builtInDatabase(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Varlena](result)

	for i := uint64(0); i < uint64(length); i++ {
		db := proc.SessionInfo.GetDatabase()
		if err := rs.AppendBytes(function2Util.QuickStrToBytes(db), false); err != nil {
			return err
		}
	}
	return nil
}

func builtInCurrentRole(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.AppendBytes([]byte(proc.SessionInfo.GetRole()), false); err != nil {
			return err
		}
	}
	return nil
}

func builtInCurrentAccountID(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[uint32](result)
	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.Append(proc.SessionInfo.AccountId, false); err != nil {
			return err
		}
	}
	return nil
}

func builtInCurrentAccountName(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.AppendBytes([]byte(proc.SessionInfo.Account), false); err != nil {
			return err
		}
	}
	return nil
}

func builtInCurrentRoleID(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[uint32](result)
	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.Append(proc.SessionInfo.RoleId, false); err != nil {
			return err
		}
	}
	return nil
}

func builtInCurrentRoleName(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.AppendBytes([]byte(proc.SessionInfo.Role), false); err != nil {
			return err
		}
	}
	return nil
}

func builtInCurrentUserID(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[uint32](result)
	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.Append(proc.SessionInfo.UserId, false); err != nil {
			return err
		}
	}
	return nil
}

func builtInCurrentUserName(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.AppendBytes([]byte(proc.SessionInfo.User), false); err != nil {
			return err
		}
	}
	return nil
}

func doLpad(src string, tgtLen int64, pad string) (string, bool) {
	const MaxTgtLen = int64(16 * 1024 * 1024)

	srcRune, padRune := []rune(src), []rune(pad)
	srcLen, padLen := len(srcRune), len(padRune)

	if tgtLen < 0 || tgtLen > MaxTgtLen {
		return "", true
	} else if int(tgtLen) < srcLen {
		return string(srcRune[:tgtLen]), false
	} else if int(tgtLen) == srcLen {
		return src, false
	} else if padLen == 0 {
		return "", false
	} else {
		r := int(tgtLen) - srcLen
		p, m := r/padLen, r%padLen
		return strings.Repeat(pad, p) + string(padRune[:m]) + src, false
	}
}

func doRpad(src string, tgtLen int64, pad string) (string, bool) {
	const MaxTgtLen = int64(16 * 1024 * 1024)

	srcRune, padRune := []rune(src), []rune(pad)
	srcLen, padLen := len(srcRune), len(padRune)

	if tgtLen < 0 || tgtLen > MaxTgtLen {
		return "", true
	} else if int(tgtLen) < srcLen {
		return string(srcRune[:tgtLen]), false
	} else if int(tgtLen) == srcLen {
		return src, false
	} else if padLen == 0 {
		return "", false
	} else {
		r := int(tgtLen) - srcLen
		p, m := r/padLen, r%padLen
		return src + strings.Repeat(pad, p) + string(padRune[:m]), false
	}
}

func builtInLpad(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[int64](parameters[1])
	p3 := vector.GenerateFunctionStrParameter(parameters[2])

	rs := vector.MustFunctionResult[types.Varlena](result)
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		v2, null2 := p2.GetValue(i)
		v3, null3 := p3.GetStrValue(i)
		if !(null1 || null2 || null3) {
			rval, shouldNull := doLpad(string(v1), v2, string(v3))
			if !shouldNull {
				if err := rs.AppendBytes([]byte(rval), false); err != nil {
					return err
				}
				continue
			}
		}
		if err := rs.AppendBytes(nil, true); err != nil {
			return err
		}
	}
	return nil
}

func builtInRpad(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[int64](parameters[1])
	p3 := vector.GenerateFunctionStrParameter(parameters[2])

	rs := vector.MustFunctionResult[types.Varlena](result)
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		v2, null2 := p2.GetValue(i)
		v3, null3 := p3.GetStrValue(i)
		if !(null1 || null2 || null3) {
			rval, shouldNull := doRpad(string(v1), v2, string(v3))
			if !shouldNull {
				if err := rs.AppendBytes([]byte(rval), false); err != nil {
					return err
				}
				continue
			}
		}
		if err := rs.AppendBytes(nil, true); err != nil {
			return err
		}
	}
	return nil
}

func builtInUUID(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Uuid](result)
	for i := uint64(0); i < uint64(length); i++ {
		val, err := uuid.NewUUID()
		if err != nil {
			return moerr.NewInternalError(proc.Ctx, "newuuid failed")
		}
		if err = rs.Append(types.Uuid(val), false); err != nil {
			return err
		}
	}
	return nil
}

func builtInUnixTimestamp(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[int64](result)
	if len(parameters) == 0 {
		val := types.CurrentTimestamp().Unix()
		for i := uint64(0); i < uint64(length); i++ {
			if err := rs.Append(val, false); err != nil {
				return nil
			}
		}
		return nil
	}

	p1 := vector.GenerateFunctionFixedTypeParameter[types.Timestamp](parameters[0])
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetValue(i)
		val := v1.Unix()
		if val < 0 || null1 {
			// XXX v1 < 0 need to raise error here.
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(val, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func mustTimestamp(loc *time.Location, s string) types.Timestamp {
	ts, err := types.ParseTimestamp(loc, s, 6)
	if err != nil {
		ts = 0
	}
	return ts
}

func builtInUnixTimestampVarcharToInt64(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[int64](result)

	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		if null1 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			val := mustTimestamp(proc.SessionInfo.TimeZone, string(v1)).Unix()
			if val < 0 {
				if err := rs.Append(0, true); err != nil {
					return err
				}
				continue
			}
			if err := rs.Append(val, false); err != nil {
				return err
			}
		}
	}
	return nil
}

var _ = builtInUnixTimestampVarcharToFloat64

func builtInUnixTimestampVarcharToFloat64(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[float64](result)

	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		if null1 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			val := mustTimestamp(proc.SessionInfo.TimeZone, string(v1))
			if err := rs.Append(val.UnixToFloat(), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func builtInUnixTimestampVarcharToDecimal128(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[types.Decimal128](result)

	var d types.Decimal128
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		if null1 {
			if err := rs.Append(d, true); err != nil {
				return err
			}
		} else {
			val, err := mustTimestamp(proc.SessionInfo.TimeZone, string(v1)).UnixToDecimal128()
			if err != nil {
				return err
			}
			if val.Compare(types.Decimal128{B0_63: 0, B64_127: 0}) <= 0 {
				if err := rs.Append(d, true); err != nil {
					return err
				}
			}
			if err = rs.Append(val, false); err != nil {
				return err
			}
		}
	}
	return nil
}

// XXX I just copy this function.
func builtInHash(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	fillStringGroupStr := func(keys [][]byte, vec *vector.Vector, n int, start int) {
		area := vec.GetArea()
		vs := vector.MustFixedCol[types.Varlena](vec)
		if !vec.GetNulls().Any() {
			for i := 0; i < n; i++ {
				keys[i] = append(keys[i], byte(0))
				keys[i] = append(keys[i], vs[i+start].GetByteSlice(area)...)
			}
		} else {
			nsp := vec.GetNulls()
			for i := 0; i < n; i++ {
				hasNull := nsp.Contains(uint64(i + start))
				if hasNull {
					keys[i] = append(keys[i], byte(1))
				} else {
					keys[i] = append(keys[i], byte(0))
					keys[i] = append(keys[i], vs[i+start].GetByteSlice(area)...)
				}
			}
		}
	}

	fillGroupStr := func(keys [][]byte, vec *vector.Vector, n int, sz int, start int) {
		data := unsafe.Slice((*byte)(vector.GetPtrAt(vec, 0)), (n+start)*sz)
		if !vec.GetNulls().Any() {
			for i := 0; i < n; i++ {
				keys[i] = append(keys[i], byte(0))
				keys[i] = append(keys[i], data[(i+start)*sz:(i+start+1)*sz]...)
			}
		} else {
			nsp := vec.GetNulls()
			for i := 0; i < n; i++ {
				isNull := nsp.Contains(uint64(i + start))
				if isNull {
					keys[i] = append(keys[i], byte(1))
				} else {
					keys[i] = append(keys[i], byte(0))
					keys[i] = append(keys[i], data[(i+start)*sz:(i+start+1)*sz]...)
				}
			}
		}
	}

	encodeHashKeys := func(keys [][]byte, vecs []*vector.Vector, start, count int) {
		for _, vec := range vecs {
			if vec.GetType().IsFixedLen() {
				fillGroupStr(keys, vec, count, vec.GetType().TypeSize(), start)
			} else {
				fillStringGroupStr(keys, vec, count, start)
			}
		}
		for i := 0; i < count; i++ {
			if l := len(keys[i]); l < 16 {
				keys[i] = append(keys[i], hashtable.StrKeyPadding[l:]...)
			}
		}
	}

	vec := result.GetResultVector()
	if vec.Length() < length {
		err := vec.PreExtend(length, proc.Mp())
		if err != nil {
			return err
		}
	}

	keys := make([][]byte, hashmap.UnitLimit)
	states := make([][3]uint64, hashmap.UnitLimit)
	for i := 0; i < length; i += hashmap.UnitLimit {
		n := length - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		encodeHashKeys(keys, parameters, i, n)
		hashtable.BytesBatchGenHashStates(&keys[0], &states[0], n)
		for j := 0; j < n; j++ {
			if err := vector.AppendFixed(vec, int64(states[j][0]), false, proc.Mp()); err != nil {
				return err
			}
		}
	}
	return nil
}

// Serial have a similar function named SerialWithCompacted in the index_util
// Serial func is used by users, the function make true when input vec have ten
// rows, the output vec is ten rows, when the vectors have null value, the output
// vec will set the row null
// for example:
// input vec is [[1, 1, 1], [2, 2, null], [3, 3, 3]]
// result vec is [serial(1, 2, 3), serial(1, 2, 3), null]
func builtInSerial(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	// do not support the constant vector.
	for _, v := range parameters {
		if v.IsConst() {
			return moerr.NewConstraintViolation(proc.Ctx, "serial function don't support constant")
		}
	}

	ps := types.NewPackerArray(length, proc.Mp())
	defer func() {
		for _, p := range ps {
			p.FreeMem()
		}
	}()

	bitMap := new(nulls.Nulls)

	for _, v := range parameters {
		switch v.GetType().Oid {
		case types.T_bool:
			s := vector.MustFixedCol[bool](v)
			for i, b := range s {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeBool(b)
				}
			}
		case types.T_int8:
			s := vector.MustFixedCol[int8](v)
			for i, b := range s {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeInt8(b)
				}
			}
		case types.T_int16:
			s := vector.MustFixedCol[int16](v)
			for i, b := range s {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeInt16(b)
				}
			}
		case types.T_int32:
			s := vector.MustFixedCol[int32](v)
			for i, b := range s {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeInt32(b)
				}
			}
		case types.T_int64:
			s := vector.MustFixedCol[int64](v)
			for i, b := range s {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeInt64(b)
				}
			}
		case types.T_uint8:
			s := vector.MustFixedCol[uint8](v)
			for i, b := range s {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeUint8(b)
				}
			}
		case types.T_uint16:
			s := vector.MustFixedCol[uint16](v)
			for i, b := range s {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeUint16(b)
				}
			}
		case types.T_uint32:
			s := vector.MustFixedCol[uint32](v)
			for i, b := range s {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeUint32(b)
				}
			}
		case types.T_uint64:
			s := vector.MustFixedCol[uint64](v)
			for i, b := range s {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeUint64(b)
				}
			}
		case types.T_float32:
			s := vector.MustFixedCol[float32](v)
			for i, b := range s {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeFloat32(b)
				}
			}
		case types.T_float64:
			s := vector.MustFixedCol[float64](v)
			for i, b := range s {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeFloat64(b)
				}
			}
		case types.T_date:
			s := vector.MustFixedCol[types.Date](v)
			for i, b := range s {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeDate(b)
				}
			}
		case types.T_time:
			s := vector.MustFixedCol[types.Time](v)
			for i, b := range s {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeTime(b)
				}
			}
		case types.T_datetime:
			s := vector.MustFixedCol[types.Datetime](v)
			for i, b := range s {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeDatetime(b)
				}
			}
		case types.T_timestamp:
			s := vector.MustFixedCol[types.Timestamp](v)
			for i, b := range s {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeTimestamp(b)
				}
			}
		case types.T_decimal64:
			s := vector.MustFixedCol[types.Decimal64](v)
			for i, b := range s {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeDecimal64(b)
				}
			}
		case types.T_decimal128:
			s := vector.MustFixedCol[types.Decimal128](v)
			for i, b := range s {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeDecimal128(b)
				}
			}
		case types.T_json, types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_blob, types.T_text:
			vs := vector.MustStrCol(v)
			for i := range vs {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					nulls.Add(bitMap, uint64(i))
				} else {
					ps[i].EncodeStringType([]byte(vs[i]))
				}
			}
		}
	}

	rs := vector.MustFunctionResult[types.Varlena](result)
	for i := uint64(0); i < uint64(length); i++ {
		if bitMap.Contains(i) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			if err := rs.AppendBytes(ps[i].GetBuf(), false); err != nil {
				return err
			}
		}
	}
	return nil
}
