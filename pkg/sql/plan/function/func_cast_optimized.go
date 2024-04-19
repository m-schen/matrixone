// Copyright 2022 Matrix Origin
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

package function

import (
	"context"
	"errors"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
	"golang.org/x/exp/constraints"
	"strconv"
	"strings"
	"unicode/utf8"
)

func bytesToJson(
	from []*vector.Vector, to vector.FunctionResultWrapper,
	length int) error {
	return opUnaryBytesToBytesWithErrorCheck(
		from, to, nil, length, func(v []byte) ([]byte, error) {
			if json, err := types.ParseSliceToByteJson(v); err == nil {
				return types.EncodeJson(json)
			} else {
				return nil, err
			}
		})
}

func bytesToBit(
	ctx context.Context,
	from []*vector.Vector, to vector.FunctionResultWrapper,
	bitWidth int,
	length int) error {

	maxAcceptableValue := uint64(1<<bitWidth - 1)

	return opUnaryBytesToFixedWithErrorCheck[uint64](
		from, to, nil, length, func(v []byte) (uint64, error) {
			var val uint64
			if len(v) <= 8 {
				for i := range v {
					val = (val << 8) | uint64(v[i])
				}
				if val <= maxAcceptableValue {
					return val, nil
				}
			}
			return 0, moerr.NewOutOfRange(ctx, fmt.Sprintf("bit(%d)", bitWidth), "value %s", string(v))
		})
}

func bytesToSigned[T constraints.Signed](
	ctx context.Context,
	from []*vector.Vector, to vector.FunctionResultWrapper,
	bitSize int,
	length int) error {

	isBinary := from[0].GetIsBin()

	// fromBytesToSigned converts a byte slice to a signed integer.
	fromBytesToSigned := func(v []byte) (T, error) {
		var r int64
		var err error

		s := strings.TrimSpace(convertByteSliceToString(v))
		if len(s) >= 2 && (s[:2] == "0x" || s[:2] == "0X") {
			r, err = strconv.ParseInt(s[2:], 16, bitSize)
		} else {
			r, err = strconv.ParseInt(s, 10, bitSize)
		}
		if err != nil {
			if errors.Is(err, strconv.ErrRange) {
				return 0, moerr.NewOutOfRange(ctx, fmt.Sprintf("int%d", bitSize), "value '%s'", s)
			}
			return 0, moerr.NewInvalidArg(ctx, "cast to int", s)
		}
		return T(r), nil
	}

	// fromBinaryToSigned converts a binary type value to a signed integer.
	var hxBytes []byte
	fromBinaryToSigned := func(v []byte) (T, error) {
		functionUtil.ReusedHexEncodeToBytes(v, hxBytes)
		r, err := strconv.ParseInt(functionUtil.QuickBytesToStr(hxBytes), 16, bitSize)
		if err != nil {
			if errors.Is(err, strconv.ErrRange) {
				return 0, moerr.NewOutOfRange(ctx, "int", "")
			}
			return 0, moerr.NewInvalidArg(ctx, "cast to int", v)
		}
		return T(r), nil
	}

	if isBinary {
		return opUnaryBytesToFixedWithErrorCheck(
			from, to, nil, length, fromBinaryToSigned)
	}
	return opUnaryBytesToFixedWithErrorCheck(
		from, to, nil, length, fromBytesToSigned)
}

func bytesToUnsigned[T constraints.Unsigned](
	ctx context.Context,
	from []*vector.Vector, to vector.FunctionResultWrapper,
	bitSize int,
	length int) error {

	isBinary := from[0].GetIsBin()

	// fromBytesToUnsigned converts a byte slice to an unsigned integer.
	fromBytesToUnsigned := func(v []byte) (T, error) {
		var r uint64
		var err error

		s := strings.TrimSpace(convertByteSliceToString(v))
		if len(s) >= 2 && (s[:2] == "0x" || s[:2] == "0X") {
			r, err = strconv.ParseUint(s[2:], 16, bitSize)
		} else {
			r, err = strconv.ParseUint(s, 10, bitSize)
		}
		if err != nil {
			if errors.Is(err, strconv.ErrRange) {
				return 0, moerr.NewOutOfRange(ctx, fmt.Sprintf("uint%d", bitSize), "value '%s'", s)
			}
			return 0, moerr.NewInvalidArg(ctx, fmt.Sprintf("cast to uint%d", bitSize), s)
		}
		return T(r), nil
	}

	// fromBinaryToUnsigned converts a binary type value to an unsigned integer.
	var hxBytes []byte
	fromBinaryToUnsigned := func(v []byte) (T, error) {
		functionUtil.ReusedHexEncodeToBytes(v, hxBytes)
		r, err := strconv.ParseUint(functionUtil.QuickBytesToStr(hxBytes), 16, bitSize)
		if err != nil {
			if errors.Is(err, strconv.ErrRange) {
				return 0, moerr.NewOutOfRange(ctx, "uint", "")
			}
			return 0, moerr.NewInvalidArg(ctx, "cast to uint", v)
		}
		return T(r), nil
	}

	if isBinary {
		return opUnaryBytesToFixedWithErrorCheck(
			from, to, nil, length, fromBinaryToUnsigned)
	}
	return opUnaryBytesToFixedWithErrorCheck(
		from, to, nil, length, fromBytesToUnsigned)
}

func bytesToFloat[T constraints.Float](
	ctx context.Context,
	from []*vector.Vector, to vector.FunctionResultWrapper,
	length int) error {

	isBinary := from[0].GetIsBin()
	resultType := to.GetResultVector().GetType()

	fixedFloat64c, isFixFloat := functionUtil.NewFixedFloat64Convert(int(resultType.Scale), int(resultType.Width))

	// fromBytesToFloat64 converts a byte slice to a float64.
	fromBytesToFloat64 := func(v []byte) (float64, error) {
		r, err := strconv.ParseFloat(convertByteSliceToString(v), 64)
		if err != nil {
			return 0, err
		}
		if isFixFloat {
			value, outOfRange := fixedFloat64c.Convert(r)
			if outOfRange {
				return 0, moerr.NewOutOfRange(ctx, "float", "Value %s", convertByteSliceToString(v))
			}
			return value, nil
		}
		return r, nil
	}

	// fromBinaryToFloat64 converts a binary type value to a float64.
	var hxBytes []byte
	fromBinaryToFloat64 := func(v []byte) (float64, error) {
		functionUtil.ReusedHexEncodeToBytes(v, hxBytes)
		r, err := strconv.ParseUint(
			convertByteSliceToString(hxBytes),
			16, 64)
		if err != nil {
			if errors.Is(err, strconv.ErrRange) {
				return 0, moerr.NewOutOfRange(ctx, "float", "value '%s'", string(hxBytes))
			}
			return 0, moerr.NewInvalidArg(ctx, "cast to float", string(hxBytes))
		}
		if isFixFloat {
			value, outOfRange := fixedFloat64c.Convert(float64(r))
			if outOfRange {
				return 0, moerr.NewOutOfRange(ctx, "float", "value '%s'", string(hxBytes))
			}
			return value, nil
		}
		return float64(r), nil
	}

	if isBinary {
		return opUnaryBytesToFixedWithErrorCheck[T](
			from, to, nil, length, func(v []byte) (T, error) {
				if f64, err := fromBinaryToFloat64(v); err == nil {
					return T(f64), nil
				} else {
					return 0, err
				}
			})
	}

	return opUnaryBytesToFixedWithErrorCheck(
		from, to, nil, length, func(v []byte) (T, error) {
			if f64, err := fromBytesToFloat64(v); err == nil {
				return T(f64), nil
			} else {
				return 0, err
			}
		})
}

func bytesToDecimal64(
	_ context.Context,
	from []*vector.Vector, to vector.FunctionResultWrapper,
	length int) error {

	resultType := to.GetResultVector().GetType()
	isBinary := to.GetResultVector().GetIsBin()
	width, scale := resultType.Width, resultType.Scale

	if isBinary {
		return opUnaryStrToFixedWithErrorCheck[types.Decimal64](
			from, to, nil, length, func(v string) (types.Decimal64, error) {
				return types.ParseDecimal64FromByte(v, width, scale)
			})
	}

	return opUnaryStrToFixedWithErrorCheck[types.Decimal64](
		from, to, nil, length, func(v string) (types.Decimal64, error) {
			return types.ParseDecimal64(v, width, scale)
		})
}

func bytesToDecimal128(
	_ context.Context,
	from []*vector.Vector, to vector.FunctionResultWrapper,
	length int) error {

	resultType := to.GetResultVector().GetType()
	isBinary := to.GetResultVector().GetIsBin()
	width, scale := resultType.Width, resultType.Scale

	if isBinary {
		return opUnaryStrToFixedWithErrorCheck[types.Decimal128](
			from, to, nil, length, func(v string) (types.Decimal128, error) {
				return types.ParseDecimal128FromByte(v, width, scale)
			})
	}

	return opUnaryStrToFixedWithErrorCheck[types.Decimal128](
		from, to, nil, length, func(v string) (types.Decimal128, error) {
			return types.ParseDecimal128(v, width, scale)
		})
}

func fromBytesToBinaryWithWidth(
	v []byte, width int) []byte {
	if width == -1 {
		return v
	}
	// cast as binary(n)
	// truncate or right pad with 0
	if len(v) > width {
		return v[:width]
	} else if len(v) < width {
		r := make([]byte, width)
		copy(r, v)
		return r
	}
	return v
}

func bytesToBytes(
	ctx context.Context,
	from []*vector.Vector, to vector.FunctionResultWrapper,
	length int) error {

	resultType := *to.GetResultVector().GetType()
	int32Width := int(resultType.Width)

	if resultType.Oid == types.T_binary {
		if int32Width != 0 || resultType.Scale != 0 {
			return opUnaryBytesToBytes(from, to, nil, length, func(v []byte) []byte {
				return fromBytesToBinaryWithWidth(v, int32Width)
			})
		}
	}

	if resultType.Oid != types.T_text && int32Width != 0 {
		return opUnaryBytesToBytesWithErrorCheck(
			from, to, nil, length, func(v []byte) ([]byte, error) {
				if utf8.RuneCount(v) > int32Width {
					return nil, formatCastError(
						ctx, from[0], resultType, fmt.Sprintf("Src length %v is larger than Dest length %v", len(v), resultType))
				}
				return v, nil
			})
	}

	return opUnaryBytesToBytes(from, to, nil, length, func(v []byte) []byte {
		return v
	})
}
