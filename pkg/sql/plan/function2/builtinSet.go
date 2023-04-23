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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function2/function2Util"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"strings"
	"time"
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
		if null1 {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			if len(v1) == 0 {
				if err := rs.AppendBytes([]byte{}, false); err != nil {
					return err
				}
			} else {
				b, err := f(v1)
				if err != nil {
					return err
				}
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
		vs := make([]byte, 16)
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
				vs = append(vs, v...)
			}
		}
		if apv {
			if err := rs.AppendBytes(vs, false); err != nil {
				return err
			}
		}
	}
	return nil
}

const (
	ZeroDate   = "0001-01-01"
	formatMask = "%Y/%m/%d"
	regexpMask = `\d{1,4}/\d{1,2}/\d{1,2}`
)

// MOLogDate parse 'YYYY/MM/DD' date from input string.
// return '0001-01-01' if input string not container 'YYYY/MM/DD' substr, until DateParse Function support return NULL for invalid date string.
func builtInMoLogDate(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Date](result)
	p1 := vector.GenerateFunctionStrParameter(parameters[0])

	op := newOpBuiltInRegexp()
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetStrValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		}
		expr := function2Util.QuickBytesToStr(v)
		match, parsedInput, err := op.regMap.regularSubstr(expr, regexpMask, 1, 1)
		if err != nil {
			return err
		}
		if !match {
			if err = rs.Append(0, true); err != nil {
				return err
			}
		}
		val, err := types.ParseDateCast(parsedInput)
		if err != nil {
			return err
		}
		if err = rs.Append(val, false); err != nil {
			return err
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
			}
			continue
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
		for i := uint64(0); i < uint64(0); i++ {
			if err := rs.Append(val, false); err != nil {
				return nil
			}
		}
		return nil
	}

	p1 := vector.GenerateFunctionFixedTypeParameter[types.Timestamp](parameters[0])
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetValue(i)
		if v1 < 0 || null1 {
			// XXX v1 < 0 need to raise error here.
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			val := v1.Unix()
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
			val := mustTimestamp(proc.SessionInfo.TimeZone, string(v1))
			if err := rs.Append(val.Unix(), false); err != nil {
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
			val := mustTimestamp(proc.SessionInfo.TimeZone, string(v1))
			rval, err := val.UnixToDecimal128()
			if err != nil {
				return err
			}
			if err = rs.Append(rval, false); err != nil {
				return err
			}
		}
	}
	return nil
}
