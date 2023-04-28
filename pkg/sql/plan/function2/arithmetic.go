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

package function2

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function2/function2Util"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
	"math"
)

func plusOperatorSupports(typ1, typ2 types.Type) bool {
	if typ1.Oid != typ2.Oid {
		return false
	}
	switch typ1.Oid {
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
	case types.T_float32, types.T_float64:
	case types.T_decimal64, types.T_decimal128:
	default:
		return false
	}
	return true
}

func minusOperatorSupports(typ1, typ2 types.Type) bool {
	if typ1.Oid != typ2.Oid {
		return false
	}
	switch typ1.Oid {
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
	case types.T_float32, types.T_float64:
	case types.T_decimal64, types.T_decimal128:
	case types.T_date, types.T_datetime:
	default:
		return false
	}
	return true
}

func multiOperatorSupports(typ1, typ2 types.Type) bool {
	if typ1.Oid != typ2.Oid {
		return false
	}
	switch typ1.Oid {
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
	case types.T_float32, types.T_float64:
	case types.T_decimal64, types.T_decimal128:
	default:
		return false
	}
	return true
}

func divOperatorSupports(typ1, typ2 types.Type) bool {
	if typ1.Oid != typ2.Oid {
		return false
	}
	switch typ1.Oid {
	case types.T_float32, types.T_float64:
	case types.T_decimal64, types.T_decimal128:
	default:
		return false
	}
	return true
}

func integerDivOperatorSupports(typ1, typ2 types.Type) bool {
	if typ1.Oid != typ2.Oid {
		return false
	}
	switch typ1.Oid {
	case types.T_float32, types.T_float64:
	default:
		return false
	}
	return true
}

func modOperatorSupports(typ1, typ2 types.Type) bool {
	if typ1.Oid != typ2.Oid {
		return false
	}
	switch typ1.Oid {
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
	case types.T_float32, types.T_float64:
	case types.T_decimal128, types.T_decimal64:
	default:
		return false
	}
	return true
}

func plusFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	paramType := parameters[0].GetType()
	switch paramType.Oid {
	case types.T_uint8:
		return numericPlus[uint8](parameters, result, uint64(length))
	case types.T_uint16:
		return numericPlus[uint16](parameters, result, uint64(length))
	case types.T_uint32:
		return numericPlus[uint32](parameters, result, uint64(length))
	case types.T_uint64:
		return numericPlus[uint64](parameters, result, uint64(length))
	case types.T_int8:
		return numericPlus[int8](parameters, result, uint64(length))
	case types.T_int16:
		return numericPlus[int16](parameters, result, uint64(length))
	case types.T_int32:
		return numericPlus[int32](parameters, result, uint64(length))
	case types.T_int64:
		return numericPlus[int64](parameters, result, uint64(length))
	case types.T_float32:
		return numericPlus[float32](parameters, result, uint64(length))
	case types.T_float64:
		return numericPlus[float64](parameters, result, uint64(length))
	case types.T_decimal64:
		return decimal64Plus(parameters, result, uint64(length))
	case types.T_decimal128:
		return decimal128Plus(parameters, result, uint64(length))
	}
	panic("unreached code")
}

func numericPlus[T constraints.Integer | constraints.Float](parameters []*vector.Vector, result vector.FunctionResultWrapper, length uint64) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[T](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[T](parameters[1])
	rs := vector.MustFunctionResult[T](result)
	for i := uint64(0); i < length; i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if null1 || null2 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(v1+v2, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func decimal64Plus(parameters []*vector.Vector, result vector.FunctionResultWrapper, length uint64) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](parameters[1])
	rs := vector.MustFunctionResult[types.Decimal64](result)

	scale1 := p1.GetType().Scale
	scale2 := p2.GetType().Scale
	for i := uint64(0); i < length; i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if null1 || null2 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			rt, _, err := v1.Add(v2, scale1, scale2)
			if err != nil {
				return err
			}
			if err = rs.Append(rt, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func decimal128Plus(parameters []*vector.Vector, result vector.FunctionResultWrapper, length uint64) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](parameters[1])
	rs := vector.MustFunctionResult[types.Decimal128](result)

	scale1 := p1.GetType().Scale
	scale2 := p2.GetType().Scale
	emptyDec128 := types.Decimal128{}
	for i := uint64(0); i < length; i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if null1 || null2 {
			if err := rs.Append(emptyDec128, true); err != nil {
				return err
			}
		} else {
			rt, _, err := v1.Add(v2, scale1, scale2)
			if err != nil {
				return err
			}
			if err = rs.Append(rt, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func minusFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	paramType := parameters[0].GetType()
	switch paramType.Oid {
	case types.T_uint8:
		return numericMinus[uint8](parameters, result, uint64(length))
	case types.T_uint16:
		return numericMinus[uint16](parameters, result, uint64(length))
	case types.T_uint32:
		return numericMinus[uint32](parameters, result, uint64(length))
	case types.T_uint64:
		return numericMinus[uint64](parameters, result, uint64(length))
	case types.T_int8:
		return numericMinus[int8](parameters, result, uint64(length))
	case types.T_int16:
		return numericMinus[int16](parameters, result, uint64(length))
	case types.T_int32:
		return numericMinus[int32](parameters, result, uint64(length))
	case types.T_int64:
		return numericMinus[int64](parameters, result, uint64(length))
	case types.T_float32:
		return numericMinus[float32](parameters, result, uint64(length))
	case types.T_float64:
		return numericMinus[float64](parameters, result, uint64(length))
	case types.T_decimal64:
		return decimal64Minus(parameters, result, uint64(length))
	case types.T_decimal128:
		return decimal128Minus(parameters, result, uint64(length))
	case types.T_date:
		return builtInDateDiff(parameters, result, proc, length)
	case types.T_datetime:
		return datetimeMinus(parameters, result, uint64(length))
	}
	panic("unreached code")
}

func decimal64Minus(parameters []*vector.Vector, result vector.FunctionResultWrapper, length uint64) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](parameters[1])
	rs := vector.MustFunctionResult[types.Decimal64](result)

	scale1 := p1.GetType().Scale
	scale2 := p2.GetType().Scale
	for i := uint64(0); i < length; i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if null1 || null2 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			rt, _, err := v1.Sub(v2, scale1, scale2)
			if err != nil {
				return err
			}
			if err = rs.Append(rt, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func decimal128Minus(parameters []*vector.Vector, result vector.FunctionResultWrapper, length uint64) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](parameters[1])
	rs := vector.MustFunctionResult[types.Decimal128](result)

	scale1 := p1.GetType().Scale
	scale2 := p2.GetType().Scale
	emptyDec128 := types.Decimal128{}
	for i := uint64(0); i < length; i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if null1 || null2 {
			if err := rs.Append(emptyDec128, true); err != nil {
				return err
			}
		} else {
			rt, _, err := v1.Sub(v2, scale1, scale2)
			if err != nil {
				return err
			}
			if err = rs.Append(rt, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func datetimeMinus(parameters []*vector.Vector, result vector.FunctionResultWrapper, length uint64) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[types.Datetime](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[types.Datetime](parameters[1])
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < length; i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if null1 || null2 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(v1.DatetimeMinusWithSecond(v2), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func numericMinus[T constraints.Integer | constraints.Float](parameters []*vector.Vector, result vector.FunctionResultWrapper, length uint64) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[T](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[T](parameters[1])
	rs := vector.MustFunctionResult[T](result)
	for i := uint64(0); i < length; i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if null1 || null2 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(v1-v2, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func multiFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	paramType := parameters[0].GetType()
	switch paramType.Oid {
	case types.T_uint8:
		return numericMulti[uint8](parameters, result, uint64(length))
	case types.T_uint16:
		return numericMulti[uint16](parameters, result, uint64(length))
	case types.T_uint32:
		return numericMulti[uint32](parameters, result, uint64(length))
	case types.T_uint64:
		return numericMulti[uint64](parameters, result, uint64(length))
	case types.T_int8:
		return numericMulti[int8](parameters, result, uint64(length))
	case types.T_int16:
		return numericMulti[int16](parameters, result, uint64(length))
	case types.T_int32:
		return numericMulti[int32](parameters, result, uint64(length))
	case types.T_int64:
		return numericMulti[int64](parameters, result, uint64(length))
	case types.T_float32:
		return numericMulti[float32](parameters, result, uint64(length))
	case types.T_float64:
		return numericMulti[float64](parameters, result, uint64(length))
	case types.T_decimal64:
		return decimal64Multi(parameters, result, uint64(length))
	case types.T_decimal128:
		return decimal128Multi(parameters, result, uint64(length))
	}
	panic("unreached code")
}

func numericMulti[T constraints.Integer | constraints.Float](parameters []*vector.Vector, result vector.FunctionResultWrapper, length uint64) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[T](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[T](parameters[1])
	rs := vector.MustFunctionResult[T](result)
	for i := uint64(0); i < length; i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if null1 || null2 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(v1*v2, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func decimal64Multi(parameters []*vector.Vector, result vector.FunctionResultWrapper, length uint64) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](parameters[1])
	rs := vector.MustFunctionResult[types.Decimal128](result)

	scale1 := p1.GetType().Scale
	scale2 := p2.GetType().Scale
	emptyDec128 := types.Decimal128{}
	for i := uint64(0); i < length; i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if null1 || null2 {
			if err := rs.Append(emptyDec128, true); err != nil {
				return err
			}
		} else {
			x, y := function2Util.ConvertD64ToD128(v1), function2Util.ConvertD64ToD128(v2)
			rt, _, err := x.Mul(y, scale1, scale2)
			if err != nil {
				return err
			}
			if err = rs.Append(rt, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func decimal128Multi(parameters []*vector.Vector, result vector.FunctionResultWrapper, length uint64) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](parameters[1])
	rs := vector.MustFunctionResult[types.Decimal128](result)

	scale1 := p1.GetType().Scale
	scale2 := p2.GetType().Scale
	emptyDec128 := types.Decimal128{}
	for i := uint64(0); i < length; i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if null1 || null2 {
			if err := rs.Append(emptyDec128, true); err != nil {
				return err
			}
		} else {
			rt, _, err := v1.Mul(v2, scale1, scale2)
			if err != nil {
				return err
			}
			if err = rs.Append(rt, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func divFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	paramType := parameters[0].GetType()
	switch paramType.Oid {
	case types.T_float32:
		return floatDiv[float32](parameters, result, uint64(length))
	case types.T_float64:
		return floatDiv[float64](parameters, result, uint64(length))
	case types.T_decimal64:
		return decimal64Div(parameters, result, uint64(length))
	case types.T_decimal128:
		return decimal128Div(parameters, result, uint64(length))
	}
	panic("unreached code")
}

func decimal64Div(parameters []*vector.Vector, result vector.FunctionResultWrapper, length uint64) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](parameters[1])
	rs := vector.MustFunctionResult[types.Decimal128](result)

	scale1 := p1.GetType().Scale
	scale2 := p2.GetType().Scale
	emptyDec128 := types.Decimal128{}
	for i := uint64(0); i < length; i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if null1 || null2 {
			if err := rs.Append(emptyDec128, true); err != nil {
				return err
			}
		} else {
			if v2 == 0 {
				return moerr.NewDivByZeroNoCtx()
			}
			x, y := function2Util.ConvertD64ToD128(v1), function2Util.ConvertD64ToD128(v2)
			rt, _, err := x.Div(y, scale1, scale2)
			if err != nil {
				return err
			}
			if err = rs.Append(rt, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func decimal128Div(parameters []*vector.Vector, result vector.FunctionResultWrapper, length uint64) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](parameters[1])
	rs := vector.MustFunctionResult[types.Decimal128](result)

	scale1 := p1.GetType().Scale
	scale2 := p2.GetType().Scale
	emptyDec128 := types.Decimal128{}
	for i := uint64(0); i < length; i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if null1 || null2 {
			if err := rs.Append(emptyDec128, true); err != nil {
				return err
			}
		} else {
			if v2.B0_63 == 0 && v2.B64_127 == 0 {
				return moerr.NewDivByZeroNoCtx()
			}
			rt, _, err := v1.Div(v2, scale1, scale2)
			if err != nil {
				return err
			}
			if err = rs.Append(rt, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func floatDiv[T float32 | float64](parameters []*vector.Vector, result vector.FunctionResultWrapper, length uint64) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[T](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[T](parameters[1])
	rs := vector.MustFunctionResult[T](result)

	for i := uint64(0); i < length; i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if null1 || null2 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if v2 == 0 {
				return moerr.NewDivByZeroNoCtx()
			}
			if err := rs.Append(v1/v2, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func integerDivFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	paramType := parameters[0].GetType()
	if paramType.Oid == types.T_float32 {
		return floatIntegerDiv[float32](parameters, result, uint64(length))
	}
	if paramType.Oid == types.T_float64 {
		return floatIntegerDiv[float64](parameters, result, uint64(length))
	}
	panic("unreached code")
}

func floatIntegerDiv[T float32 | float64](parameters []*vector.Vector, result vector.FunctionResultWrapper, length uint64) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[T](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[T](parameters[1])
	rs := vector.MustFunctionResult[int64](result)

	for i := uint64(0); i < length; i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if null1 || null2 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if v2 == 0 {
				return moerr.NewDivByZeroNoCtx()
			}
			if err := rs.Append(int64(v1/v2), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func modFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	paramType := parameters[0].GetType()
	switch paramType.Oid {
	case types.T_uint8:
		return intMod[uint8](parameters, result, uint64(length))
	case types.T_uint16:
		return intMod[uint16](parameters, result, uint64(length))
	case types.T_uint32:
		return intMod[uint32](parameters, result, uint64(length))
	case types.T_uint64:
		return intMod[uint64](parameters, result, uint64(length))
	case types.T_int8:
		return intMod[int8](parameters, result, uint64(length))
	case types.T_int16:
		return intMod[int16](parameters, result, uint64(length))
	case types.T_int32:
		return intMod[int32](parameters, result, uint64(length))
	case types.T_int64:
		return intMod[int64](parameters, result, uint64(length))
	case types.T_float32:
		return floatMod[float32](parameters, result, uint64(length))
	case types.T_float64:
		return floatMod[float64](parameters, result, uint64(length))
	case types.T_decimal64:
		return decimal64Mod(parameters, result, uint64(length))
	case types.T_decimal128:
		return decimal128Mod(parameters, result, uint64(length))
	}
	panic("unreached code")
}

func intMod[T constraints.Integer](parameters []*vector.Vector, result vector.FunctionResultWrapper, length uint64) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[T](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[T](parameters[1])
	rs := vector.MustFunctionResult[T](result)
	for i := uint64(0); i < length; i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if null1 || null2 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if v2 == 0 {
				if err := rs.Append(v1, false); err != nil {
					return err
				}
			} else {
				if err := rs.Append(v1%v2, false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func floatMod[T constraints.Float](parameters []*vector.Vector, result vector.FunctionResultWrapper, length uint64) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[T](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[T](parameters[1])
	rs := vector.MustFunctionResult[T](result)
	for i := uint64(0); i < length; i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if null1 || null2 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if v2 == 0 {
				if err := rs.Append(v1, false); err != nil {
					return err
				}
			} else {
				if err := rs.Append(T(math.Mod(float64(v1), float64(v2))), false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func decimal64Mod(parameters []*vector.Vector, result vector.FunctionResultWrapper, length uint64) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](parameters[1])
	rs := vector.MustFunctionResult[types.Decimal64](result)
	for i := uint64(0); i < length; i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if null1 || null2 {
			if err := rs.Append(v1, true); err != nil {
				return err
			}
		} else {
			r, _, err := v1.Mod(v2, p1.GetType().Scale, p2.GetType().Scale)
			if err != nil {
				return err
			}
			if err = rs.Append(r, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func decimal128Mod(parameters []*vector.Vector, result vector.FunctionResultWrapper, length uint64) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](parameters[1])
	rs := vector.MustFunctionResult[types.Decimal128](result)
	for i := uint64(0); i < length; i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if null1 || null2 {
			if err := rs.Append(v1, true); err != nil {
				return err
			}
		} else {
			r, _, err := v1.Mod(v2, p1.GetType().Scale, p2.GetType().Scale)
			if err != nil {
				return err
			}
			if err = rs.Append(r, false); err != nil {
				return err
			}
		}
	}
	return nil
}
