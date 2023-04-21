// Copyright 2023 Matrix Origin
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
	"encoding/hex"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function2/function2Util"
	"github.com/matrixorigin/matrixone/pkg/vectorize/lengthutf8"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
	"strconv"
	"strings"
)

func HexString(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetStrValue(i)
		if null {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			res := hexEncodeString(v)
			if err := rs.AppendBytes(function2Util.QuickStrToBytes(res), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func HexInt64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			res := hexEncodeInt64(v)
			if err := rs.AppendBytes(function2Util.QuickStrToBytes(res), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func hexEncodeString(xs []byte) string {
	return hex.EncodeToString(xs)
}

func hexEncodeInt64(xs int64) string {
	return fmt.Sprintf("%X", xs)
}

func Length(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[int64](result)
	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetStrValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			res := strLength(function2Util.QuickBytesToStr(v))
			if err := rs.Append(res, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func strLength(xs string) int64 {
	return int64(len(xs))
}

func LengthUTF8(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[uint64](result)
	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetStrValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			res := strLengthUTF8(v)
			if err := rs.Append(res, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func strLengthUTF8(xs []byte) uint64 {
	return lengthutf8.CountUTF8CodePoints(xs)
}

func Ltrim(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {

	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	//TODO: Need help in handling T_blob case. Original Code: https://github.com/m-schen/matrixone/blob/8cc47db01615a6b6504822d2e63f7085c41f3a47/pkg/sql/plan/function/builtin/unary/ltrim.go#L28
	rs := vector.MustFunctionResult[types.Varlena](result)

	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetStrValue(i)
		if null {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			res := ltrim(function2Util.QuickBytesToStr(v))
			if err := rs.AppendBytes(function2Util.QuickStrToBytes(res), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func ltrim(xs string) string {
	return strings.TrimLeft(xs, " ")
}

func Rtrim(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {

	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	//TODO: Need help in handling T_blob case. Original Code: https://github.com/m-schen/matrixone/blob/3751d33a42435b21bc0416fe47df9d6f4b1060bc/pkg/sql/plan/function/builtin/unary/rtrim.go#L28
	rs := vector.MustFunctionResult[types.Varlena](result)

	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetStrValue(i)
		if null {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			res := rtrim(function2Util.QuickBytesToStr(v))
			if err := rs.AppendBytes(function2Util.QuickStrToBytes(res), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func rtrim(xs string) string {
	return strings.TrimRight(xs, " ")
}

func Reverse(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {

	ivec := vector.GenerateFunctionStrParameter(ivecs[0])

	//TODO: Need help in handling T_blob case. Original Code: https://github.com/m-schen/matrixone/blob/a8360197d569920c66b295d957fb1213b0dd1828/pkg/sql/plan/function/builtin/unary/reverse.go#L28
	rs := vector.MustFunctionResult[types.Varlena](result)

	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetStrValue(i)
		if null {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			res := reverse(function2Util.QuickBytesToStr(v))
			if err := rs.AppendBytes(function2Util.QuickStrToBytes(res), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func reverse(str string) string {
	runes := []rune(str)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}

func Oct[T constraints.Unsigned | constraints.Signed](ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	ivec := vector.GenerateFunctionFixedTypeParameter[T](ivecs[0])
	rs := vector.MustFunctionResult[types.Decimal128](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			var nilVal types.Decimal128
			if err := rs.Append(nilVal, true); err != nil {
				return err
			}
		} else {
			res, err := oct(v)
			if err != nil {
				return err
			}
			if err := rs.Append(res, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func oct[T constraints.Unsigned | constraints.Signed](val T) (types.Decimal128, error) {
	_val := uint64(val)
	return types.ParseDecimal128(fmt.Sprintf("%o", _val), 38, 0)
}

func OctFloat[T constraints.Float](ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	ivec := vector.GenerateFunctionFixedTypeParameter[T](ivecs[0])
	rs := vector.MustFunctionResult[types.Decimal128](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			var nilVal types.Decimal128
			if err := rs.Append(nilVal, true); err != nil {
				return err
			}
		} else {
			res, err := octFloat(v)
			if err != nil {
				return err
			}
			if err := rs.Append(res, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func octFloat[T constraints.Float](xs T) (types.Decimal128, error) {
	var res types.Decimal128

	if xs < 0 {
		val, err := strconv.ParseInt(fmt.Sprintf("%1.0f", xs), 10, 64)
		if err != nil {
			return res, err
		}
		res, err = oct(uint64(val))
		if err != nil {
			return res, err
		}
	} else {
		val, err := strconv.ParseUint(fmt.Sprintf("%1.0f", xs), 10, 64)
		if err != nil {
			return res, err
		}
		res, err = oct(val)
		if err != nil {
			return res, err
		}
	}
	return res, nil

}

// Month

func DateToMonth(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	ivec := vector.GenerateFunctionFixedTypeParameter[types.Date](ivecs[0])
	rs := vector.MustFunctionResult[uint8](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(v.Month(), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func DatetimeToMonth(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	ivec := vector.GenerateFunctionFixedTypeParameter[types.Datetime](ivecs[0])
	rs := vector.MustFunctionResult[uint8](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(v.Month(), false); err != nil {
				return err
			}
		}
	}
	return nil
}
func DateStringToMonth(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	rs := vector.MustFunctionResult[uint8](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetStrValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			d, e := types.ParseDateCast(function2Util.QuickBytesToStr(v))
			if e != nil {
				return e
			}
			if err := rs.Append(d.Month(), false); err != nil {
				return err
			}
		}
	}
	return nil
}

// Year

func DateToYear(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	ivec := vector.GenerateFunctionFixedTypeParameter[types.Date](ivecs[0])
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(int64(v.Year()), false); err != nil {
				return err
			}
		}
	}
	return nil
}
func DatetimeToYear(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	ivec := vector.GenerateFunctionFixedTypeParameter[types.Datetime](ivecs[0])
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(int64(v.Year()), false); err != nil {
				return err
			}
		}
	}
	return nil
}
func DateStringToYear(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetStrValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			d, e := types.ParseDateCast(function2Util.QuickBytesToStr(v))
			if e != nil {
				return e
			}
			if err := rs.Append(int64(d.Year()), false); err != nil {
				return err
			}
		}
	}
	return nil
}

// Week

func DateToWeek(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	return nil
}
func DatetimeToWeek(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	return nil
}

// Weekday

func DateToWeekday(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	return nil
}
func DatetimeToWeekday(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	return nil
}
