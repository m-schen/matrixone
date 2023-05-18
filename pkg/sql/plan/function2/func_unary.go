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
	"context"
	"encoding/hex"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/builtin/binary"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function2/function2Util"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vectorize/get_timestamp"
	"github.com/matrixorigin/matrixone/pkg/vectorize/json_quote"
	"github.com/matrixorigin/matrixone/pkg/vectorize/json_unquote"
	"github.com/matrixorigin/matrixone/pkg/vectorize/lengthutf8"
	"github.com/matrixorigin/matrixone/pkg/vectorize/pi"
	"github.com/matrixorigin/matrixone/pkg/version"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
	"io"
	"strconv"
	"strings"
	"time"
)

func AbsUInt64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToFixed[uint64, uint64](ivecs, result, proc, length, func(v uint64) uint64 {
		return v
	})
}

func absSigned[T constraints.Signed | constraints.Float](v T) (T, error) {
	if v < 0 {
		v = -v
	}
	if v < 0 {
		return 0, moerr.NewOutOfRangeNoCtx("int", "'%v'", v)
	}
	return v, nil
}

func AbsInt64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToFixedWithErrorCheck[int64, int64](ivecs, result, proc, length, func(v int64) (int64, error) {
		return absSigned[int64](v)
	})
}

func AbsFloat64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToFixedWithErrorCheck[float64, float64](ivecs, result, proc, length, func(v float64) (float64, error) {
		return absSigned[float64](v)
	})
}

func absDecimal128(v types.Decimal128) types.Decimal128 {
	if v.Sign() {
		v = v.Minus()
	}
	return v
}

func AbsDecimal128(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToFixed[types.Decimal128, types.Decimal128](ivecs, result, proc, length, func(v types.Decimal128) types.Decimal128 {
		return absDecimal128(v)
	})
}

func StringSingle(val []byte) uint8 {
	if len(val) == 0 {
		return 0
	}
	return val[0]
}

func AsciiString(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	return opUnaryBytesToFixed[uint8](ivecs, result, proc, length, func(v []byte) uint8 {
		return StringSingle(v)
	})
}

var (
	intStartMap = map[types.T]int{
		types.T_int8:   3,
		types.T_uint8:  3,
		types.T_int16:  2,
		types.T_uint16: 2,
		types.T_int32:  1,
		types.T_uint32: 1,
		types.T_int64:  0,
		types.T_uint64: 0,
	}
	ints  = []int64{1e16, 1e8, 1e4, 1e2, 1e1}
	uints = []uint64{1e16, 1e8, 1e4, 1e2, 1e1}
)

func IntSingle[T types.Ints](val T, start int) uint8 {
	if val < 0 {
		return '-'
	}
	i64Val := int64(val)
	for _, v := range ints[start:] {
		if i64Val >= v {
			i64Val /= v
		}
	}
	return uint8(i64Val) + '0'
}

func AsciiInt[T types.Ints](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	start := intStartMap[ivecs[0].GetType().Oid]

	return opUnaryFixedToFixed[T, uint8](ivecs, result, proc, length, func(v T) uint8 {
		return IntSingle[T](v, start)
	})
}

func UintSingle[T types.UInts](val T, start int) uint8 {
	u64Val := uint64(val)
	for _, v := range uints[start:] {
		if u64Val >= v {
			u64Val /= v
		}
	}
	return uint8(u64Val) + '0'
}

func AsciiUint[T types.UInts](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	start := intStartMap[ivecs[0].GetType().Oid]

	return opUnaryFixedToFixed[T, uint8](ivecs, result, proc, length, func(v T) uint8 {
		return UintSingle[T](v, start)
	})
}

func uintToBinary(x uint64) string {
	if x == 0 {
		return "0"
	}
	b, i := [64]byte{}, 63
	for x > 0 {
		if x&1 == 1 {
			b[i] = '1'
		} else {
			b[i] = '0'
		}
		x >>= 1
		i -= 1
	}

	return string(b[i+1:])
}

func binInteger[T constraints.Unsigned | constraints.Signed](v T, proc *process.Process) (string, error) {
	return uintToBinary(uint64(v)), nil
}

func binFloat[T constraints.Float](v T, proc *process.Process) (string, error) {
	if err := binary.NumericToNumericOverflow(proc.Ctx, []T{v}, []int64{}); err != nil {
		return "", err
	}
	return uintToBinary(uint64(int64(v))), nil
}

func Bin[T constraints.Unsigned | constraints.Signed](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToStrWithErrorCheck[T](ivecs, result, proc, length, func(v T) (string, error) {
		val, err := binInteger[T](v, proc)
		if err != nil {
			return "", moerr.NewInvalidInput(proc.Ctx, "The input value is out of range")
		}
		return val, err
	})
}

func BinFloat[T constraints.Float](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToStrWithErrorCheck[T](ivecs, result, proc, length, func(v T) (string, error) {
		val, err := binFloat[T](v, proc)
		if err != nil {
			return "", moerr.NewInvalidInput(proc.Ctx, "The input value is out of range")
		}
		return val, err
	})
}

func BitLengthFunc(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryStrToFixed[int64](ivecs, result, proc, length, func(v string) int64 {
		return int64(len(v) * 8)
	})
}

func CurrentDate(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	var err error

	loc := proc.SessionInfo.TimeZone
	if loc == nil {
		logutil.Warn("missing timezone in session info")
		loc = time.Local
	}
	ts := types.UnixNanoToTimestamp(proc.UnixTime)
	dateTimes := make([]types.Datetime, 1)
	dateTimes, err = types.TimestampToDatetime(loc, []types.Timestamp{ts}, dateTimes)
	if err != nil {
		return err
	}
	r := dateTimes[0].ToDate()

	return opNoneParamToFixed[types.Date](result, proc, length, func() types.Date {
		return r
	})
}

func DateToDate(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToFixed[types.Date, types.Date](ivecs, result, proc, length, func(v types.Date) types.Date {
		return v
	})
}

func DatetimeToDate(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToFixed[types.Datetime, types.Date](ivecs, result, proc, length, func(v types.Datetime) types.Date {
		return v.ToDate()
	})
}

func TimeToDate(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToFixed[types.Time, types.Date](ivecs, result, proc, length, func(v types.Time) types.Date {
		return v.ToDate()
	})
}

// DateStringToDate can still speed up if vec is const. but we will do the constant fold. so it does not matter.
func DateStringToDate(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryBytesToFixedWithErrorCheck[types.Date](ivecs, result, proc, length, func(v []byte) (types.Date, error) {
		d, e := types.ParseDatetime(function2Util.QuickBytesToStr(v), 6)
		if e != nil {
			return 0, moerr.NewOutOfRangeNoCtx("date", "'%s'", v)
		}
		return d.ToDate(), nil
	})
}

func DateToDay(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToFixed[types.Date, uint8](ivecs, result, proc, length, func(v types.Date) uint8 {
		return v.Day()
	})
}

func DatetimeToDay(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToFixed[types.Datetime, uint8](ivecs, result, proc, length, func(v types.Datetime) uint8 {
		return v.Day()
	})
}

func DayOfYear(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToFixed[types.Date, uint16](ivecs, result, proc, length, func(v types.Date) uint16 {
		return v.DayOfYear()
	})
}

func Empty(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryBytesToFixed[bool](ivecs, result, proc, length, func(v []byte) bool {
		return len(v) == 0
	})
}

func JsonQuote(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryStrToBytesWithErrorCheck(ivecs, result, proc, length, json_quote.Single)
}

func JsonUnquote(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	fSingle := json_unquote.JsonSingle
	if ivecs[0].GetType().Oid.IsMySQLString() {
		fSingle = json_unquote.StringSingle
	}

	return opUnaryBytesToStrWithErrorCheck(ivecs, result, proc, length, fSingle)
}

const (
	blobsize = 65536 // 2^16-1
)

func ReadFromFile(Filepath string, fs fileservice.FileService) (io.ReadCloser, error) {
	fs, readPath, err := fileservice.GetForETL(fs, Filepath)
	if fs == nil || err != nil {
		return nil, err
	}
	var r io.ReadCloser
	ctx := context.TODO()
	vec := fileservice.IOVector{
		FilePath: readPath,
		Entries: []fileservice.IOEntry{
			0: {
				Offset:            0,
				Size:              -1,
				ReadCloserForRead: &r,
			},
		},
	}
	err = fs.Read(ctx, &vec)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// Too confused.
func LoadFile(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	Filepath, null := ivec.GetStrValue(0)
	if null {
		if err := rs.AppendBytes(nil, true); err != nil {
			return err
		}
		return nil
	}
	fs := proc.FileService
	r, err := ReadFromFile(string(Filepath), fs)
	if err != nil {
		return err
	}
	defer r.Close()
	ctx, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if len(ctx) > blobsize {
		return moerr.NewInternalError(proc.Ctx, "Data too long for blob")
	}
	if len(ctx) == 0 {
		if err = rs.AppendBytes(nil, true); err != nil {
			return err
		}
		return nil
	}

	if err = rs.AppendBytes(ctx, false); err != nil {
		return err
	}
	return nil
}

func MoMemUsage(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	if len(ivecs) != 1 {
		return moerr.NewInvalidInput(proc.Ctx, "no mpool name")
	}
	if !ivecs[0].IsConst() {
		return moerr.NewInvalidInput(proc.Ctx, "mo mem usage can only take scalar input")
	}

	return opUnaryStrToBytesWithErrorCheck(ivecs, result, proc, length, func(v string) ([]byte, error) {
		memUsage := mpool.ReportMemUsage(v)
		return function2Util.QuickStrToBytes(memUsage), nil
	})
}

func moMemUsageCmd(cmd string, ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	if len(ivecs) != 1 {
		return moerr.NewInvalidInput(proc.Ctx, "no mpool name")
	}
	if !ivecs[0].IsConst() {
		return moerr.NewInvalidInput(proc.Ctx, "mo mem usage can only take scalar input")
	}

	return opUnaryStrToBytesWithErrorCheck(ivecs, result, proc, length, func(v string) ([]byte, error) {
		ok := mpool.MPoolControl(v, cmd)
		return function2Util.QuickStrToBytes(ok), nil
	})
}

func MoEnableMemUsageDetail(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return moMemUsageCmd("enable_detail", ivecs, result, proc, length)
}

func MoDisableMemUsageDetail(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return moMemUsageCmd("disable_detail", ivecs, result, proc, length)
}

const (
	MaxAllowedValue = 8000
)

func FillSpaceNumber[T types.BuiltinNumber](v T) (string, error) {
	var ilen int
	if v < 0 {
		ilen = 0
	} else {
		ilen = int(v)
		if ilen > MaxAllowedValue || ilen < 0 {
			return "", moerr.NewInvalidInputNoCtx("the space count is greater than max allowed value %d", MaxAllowedValue)
		}
	}
	return strings.Repeat(" ", ilen), nil
}

func SpaceNumber[T types.BuiltinNumber](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToStrWithErrorCheck[T](ivecs, result, proc, length, FillSpaceNumber[T])
}

func TimeToTime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToFixed[types.Time, types.Time](ivecs, result, proc, length, func(v types.Time) types.Time {
		return v
	})
}

func DateToTime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToFixed[types.Date, types.Time](ivecs, result, proc, length, func(v types.Date) types.Time {
		return v.ToTime()
	})
}

func DatetimeToTime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	scale := ivecs[0].GetType().Scale
	return opUnaryFixedToFixed[types.Datetime, types.Time](ivecs, result, proc, length, func(v types.Datetime) types.Time {
		return v.ToTime(scale)
	})
}

func Int64ToTime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToFixedWithErrorCheck[int64, types.Time](ivecs, result, proc, length, func(v int64) (types.Time, error) {
		t, e := types.ParseInt64ToTime(v, 0)
		if e != nil {
			return 0, moerr.NewOutOfRangeNoCtx("time", "'%d'", v)
		}
		return t, nil
	})
}

func DateStringToTime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryBytesToFixedWithErrorCheck[types.Time](ivecs, result, proc, length, func(v []byte) (types.Time, error) {
		t, e := types.ParseTime(string(v), 6)
		if e != nil {
			return 0, moerr.NewOutOfRangeNoCtx("time", "'%s'", string(v))
		}
		return t, nil
	})
}

func Decimal128ToTime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	scale := ivecs[0].GetType().Scale
	return opUnaryFixedToFixedWithErrorCheck[types.Decimal128, types.Time](ivecs, result, proc, length, func(v types.Decimal128) (types.Time, error) {
		t, e := types.ParseDecimal128ToTime(v, scale, 6)
		if e != nil {
			return 0, moerr.NewOutOfRangeNoCtx("time", "'%s'", v.Format(0))
		}
		return t, nil
	})
}

func DateToTimestamp(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToFixed[types.Date, types.Timestamp](ivecs, result, proc, length, func(v types.Date) types.Timestamp {
		return v.ToTimestamp(proc.SessionInfo.TimeZone)
	})
}

func DatetimeToTimestamp(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToFixed[types.Datetime, types.Timestamp](ivecs, result, proc, length, func(v types.Datetime) types.Timestamp {
		return v.ToTimestamp(proc.SessionInfo.TimeZone)
	})
}

func TimestampToTimestamp(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToFixed[types.Timestamp, types.Timestamp](ivecs, result, proc, length, func(v types.Timestamp) types.Timestamp {
		return v
	})
}

func DateStringToTimestamp(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryStrToFixedWithErrorCheck[types.Timestamp](ivecs, result, proc, length, func(v string) (types.Timestamp, error) {
		val, err := types.ParseTimestamp(proc.SessionInfo.TimeZone, v, 6)
		if err != nil {
			return 0, err
		}
		return val, nil
	})
}

func Values(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	fromVec := parameters[0]
	toVec := result.GetResultVector()
	toVec.Reset(*toVec.GetType())

	sels := make([]int32, fromVec.Length())
	for j := 0; j < len(sels); j++ {
		sels[j] = int32(j)
	}

	err := toVec.Union(fromVec, sels, proc.GetMPool())
	return err
}

func TimestampToHour(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToFixed[types.Timestamp, uint8](ivecs, result, proc, length, func(v types.Timestamp) uint8 {
		return uint8(v.ToDatetime(proc.SessionInfo.TimeZone).Hour())
	})
}

func DatetimeToHour(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToFixed[types.Datetime, uint8](ivecs, result, proc, length, func(v types.Datetime) uint8 {
		return uint8(v.Hour())
	})
}

func TimestampToMinute(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToFixed[types.Timestamp, uint8](ivecs, result, proc, length, func(v types.Timestamp) uint8 {
		return uint8(v.ToDatetime(proc.SessionInfo.TimeZone).Minute())
	})
}

func DatetimeToMinute(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToFixed[types.Datetime, uint8](ivecs, result, proc, length, func(v types.Datetime) uint8 {
		return uint8(v.Minute())
	})
}

func TimestampToSecond(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToFixed[types.Timestamp, uint8](ivecs, result, proc, length, func(v types.Timestamp) uint8 {
		return uint8(v.ToDatetime(proc.SessionInfo.TimeZone).Sec())
	})
}

func DatetimeToSecond(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToFixed[types.Datetime, uint8](ivecs, result, proc, length, func(v types.Datetime) uint8 {
		return uint8(v.Sec())
	})
}

func doBinary(orig []byte) []byte {
	if len(orig) > types.MaxBinaryLen {
		return orig[:types.MaxBinaryLen]
	} else {
		return orig
	}
}

func Binary(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryBytesToBytes(ivecs, result, proc, length, doBinary)
}

func Charset(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	r := proc.SessionInfo.GetCharset()
	return opNoneParamToBytes(result, proc, length, func() []byte {
		return function2Util.QuickStrToBytes(r)
	})
}

func Collation(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	r := proc.SessionInfo.GetCollation()
	return opNoneParamToBytes(result, proc, length, func() []byte {
		return function2Util.QuickStrToBytes(r)
	})
}

func ConnectionID(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	r := proc.SessionInfo.ConnectionID
	return opNoneParamToFixed[uint64](result, proc, length, func() uint64 {
		return r
	})
}

func HexString(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryBytesToStr(ivecs, result, proc, length, hexEncodeString)
}

func HexInt64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToStr[int64](ivecs, result, proc, length, hexEncodeInt64)
}

func hexEncodeString(xs []byte) string {
	return hex.EncodeToString(xs)
}

func hexEncodeInt64(xs int64) string {
	return fmt.Sprintf("%X", xs)
}

func Length(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryStrToFixed[int64](ivecs, result, proc, length, strLength)
}

func strLength(xs string) int64 {
	return int64(len(xs))
}

func LengthUTF8(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryBytesToFixed[uint64](ivecs, result, proc, length, strLengthUTF8)
}

func strLengthUTF8(xs []byte) uint64 {
	return lengthutf8.CountUTF8CodePoints(xs)
}

func Ltrim(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryStrToStr(ivecs, result, proc, length, ltrim)
}

func ltrim(xs string) string {
	return strings.TrimLeft(xs, " ")
}

func Rtrim(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryStrToStr(ivecs, result, proc, length, rtrim)
}

func rtrim(xs string) string {
	return strings.TrimRight(xs, " ")
}

func Reverse(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryStrToStr(ivecs, result, proc, length, reverse)
}

func reverse(str string) string {
	runes := []rune(str)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}

func Oct[T constraints.Unsigned | constraints.Signed](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToFixedWithErrorCheck[T, types.Decimal128](ivecs, result, proc, length, oct[T])
}

func oct[T constraints.Unsigned | constraints.Signed](val T) (types.Decimal128, error) {
	_val := uint64(val)
	return types.ParseDecimal128(fmt.Sprintf("%o", _val), 38, 0)
}

func OctFloat[T constraints.Float](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToFixedWithErrorCheck[T, types.Decimal128](ivecs, result, proc, length, octFloat[T])
}

func octFloat[T constraints.Float](xs T) (types.Decimal128, error) {
	var res types.Decimal128

	if xs < 0 {
		val, err := strconv.ParseInt(fmt.Sprintf("%1.0f", xs), 10, 64)
		if err != nil {
			return res, moerr.NewInternalErrorNoCtx("the input value is out of integer range")
		}
		res, err = oct(uint64(val))
		if err != nil {
			return res, err
		}
	} else {
		val, err := strconv.ParseUint(fmt.Sprintf("%1.0f", xs), 10, 64)
		if err != nil {
			return res, moerr.NewInternalErrorNoCtx("the input value is out of integer range")
		}
		res, err = oct(val)
		if err != nil {
			return res, err
		}
	}
	return res, nil
}

func DateToMonth(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToFixed[types.Date, uint8](ivecs, result, proc, length, func(v types.Date) uint8 {
		return v.Month()
	})
}

func DatetimeToMonth(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToFixed[types.Datetime, uint8](ivecs, result, proc, length, func(v types.Datetime) uint8 {
		return v.Month()
	})
}

// TODO: I will support template soon.
func DateStringToMonth(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	//return opUnaryStrToFixedWithErrorCheck[uint8](ivecs, result, proc, length, func(v string) (uint8, error) {
	//	d, e := types.ParseDateCast(v)
	//	if e != nil {
	//		return 0, e
	//	}
	//	return d.Month(), nil
	//})

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
				if err := rs.Append(0, true); err != nil {
					return err
				}
			} else {
				if err := rs.Append(d.Month(), false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func DateToYear(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToFixed[types.Date, int64](ivecs, result, proc, length, func(v types.Date) int64 {
		return int64(v.Year())
	})
}

func DatetimeToYear(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToFixed[types.Datetime, int64](ivecs, result, proc, length, func(v types.Datetime) int64 {
		return int64(v.Year())
	})
}

func DateStringToYear(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryStrToFixedWithErrorCheck[int64](ivecs, result, proc, length, func(v string) (int64, error) {
		d, e := types.ParseDateCast(v)
		if e != nil {
			return 0, e
		}
		return int64(d.Year()), nil
	})
}

func DateToWeek(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToFixed[types.Date, uint8](ivecs, result, proc, length, func(v types.Date) uint8 {
		return v.WeekOfYear2()
	})
}

func DatetimeToWeek(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToFixed[types.Datetime, uint8](ivecs, result, proc, length, func(v types.Datetime) uint8 {
		return v.ToDate().WeekOfYear2()
	})
}

func DateToWeekday(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToFixed[types.Date, int64](ivecs, result, proc, length, func(v types.Date) int64 {
		return int64(v.DayOfWeek2())
	})
}

func DatetimeToWeekday(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToFixed[types.Datetime, int64](ivecs, result, proc, length, func(v types.Datetime) int64 {
		return int64(v.ToDate().DayOfWeek2())
	})
}

func FoundRows(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opNoneParamToFixed[uint64](result, proc, length, func() uint64 {
		return 0
	})
}

func ICULIBVersion(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opNoneParamToBytes(result, proc, length, func() []byte {
		return function2Util.QuickStrToBytes("")
	})
}

func LastInsertID(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opNoneParamToFixed[uint64](result, proc, length, func() uint64 {
		return proc.SessionInfo.LastInsertID
	})
}

// TODO: may support soon.
func LastQueryIDWithoutParam(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	rs := vector.MustFunctionResult[types.Varlena](result)

	for i := uint64(0); i < uint64(length); i++ {
		cnt := int64(len(proc.SessionInfo.QueryId))
		if cnt == 0 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		}
		var idx int
		idx, err = makeQueryIdIdx(-1, cnt, proc)
		if err != nil {
			return err
		}

		if err = rs.AppendBytes(function2Util.QuickStrToBytes(proc.SessionInfo.QueryId[idx]), false); err != nil {
			return err
		}
	}
	return nil
}

func makeQueryIdIdx(loc, cnt int64, proc *process.Process) (int, error) {
	// https://docs.snowflake.com/en/sql-reference/functions/last_query_id.html
	var idx int
	if loc < 0 {
		if loc < -cnt {
			return 0, moerr.NewInvalidInput(proc.Ctx, "index out of range: %d", loc)
		}
		idx = int(loc + cnt)
	} else {
		if loc > cnt {
			return 0, moerr.NewInvalidInput(proc.Ctx, "index out of range: %d", loc)
		}
		idx = int(loc)
	}
	return idx, nil
}

func LastQueryID(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	rs := vector.MustFunctionResult[types.Varlena](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[0])

	//TODO: Not at all sure about this. Should we do null check
	// Validate: https://github.com/m-schen/matrixone/blob/9e8ef37e2a6f34873ceeb3c101ec9bb14a82a8a7/pkg/sql/plan/function/builtin/unary/infomation_function.go#L245
	loc, _ := ivec.GetValue(0)
	for i := uint64(0); i < uint64(length); i++ {
		cnt := int64(len(proc.SessionInfo.QueryId))
		if cnt == 0 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		}
		var idx int
		idx, err = makeQueryIdIdx(loc, cnt, proc)
		if err != nil {
			return err
		}

		if err = rs.AppendBytes(function2Util.QuickStrToBytes(proc.SessionInfo.QueryId[idx]), false); err != nil {
			return err
		}
	}
	return nil
}

func RolesGraphml(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opNoneParamToBytes(result, proc, length, func() []byte {
		return function2Util.QuickStrToBytes("")
	})
}

func RowCount(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opNoneParamToFixed[uint64](result, proc, length, func() uint64 {
		return 0
	})
}

func User(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opNoneParamToBytes(result, proc, length, func() []byte {
		return function2Util.QuickStrToBytes(proc.SessionInfo.GetUserHost())
	})
}

func Pi(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	r := pi.GetPi()

	return opNoneParamToFixed[float64](result, proc, length, func() float64 {
		return r
	})
}

func DisableFaultInjection(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	fault.Disable()

	return opNoneParamToFixed[bool](result, proc, length, func() bool {
		return true
	})
}

func EnableFaultInjection(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	fault.Enable()

	return opNoneParamToFixed[bool](result, proc, length, func() bool {
		return true
	})
}

func RemoveFaultPoint(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	if !ivecs[0].IsConst() || ivecs[0].IsConstNull() {
		return moerr.NewInvalidArg(proc.Ctx, "RemoveFaultPoint", "not scalar")
	}

	return opUnaryStrToFixedWithErrorCheck[bool](ivecs, result, proc, length, func(v string) (bool, error) {
		err = fault.RemoveFaultPoint(proc.Ctx, v)
		return true, err
	})
}

func TriggerFaultPoint(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	if !ivecs[0].IsConst() || ivecs[0].IsConstNull() {
		return moerr.NewInvalidArg(proc.Ctx, "TriggerFaultPoint", "not scalar")
	}

	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	rs := vector.MustFunctionResult[int64](result)

	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetStrValue(i)
		if null {
			if err = rs.Append(0, true); err != nil {
				return err
			}
		} else {
			iv, _, ok := fault.TriggerFault(function2Util.QuickBytesToStr(v))
			if !ok {
				if err = rs.Append(0, true); err != nil {
					return err
				}
			} else {
				if err = rs.Append(iv, false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func UTCTimestamp(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opNoneParamToFixed[types.Datetime](result, proc, length, func() types.Datetime {
		return get_timestamp.GetUTCTimestamp()
	})
}

func sleepSeconds(proc *process.Process, sec float64) (uint8, error) {
	if sec < 0 {
		return 0, moerr.NewInvalidArg(proc.Ctx, "sleep", "input contains negative")
	}

	sleepNano := time.Nanosecond * time.Duration(sec*1e9)
	select {
	case <-time.After(sleepNano):
		return 0, nil
	case <-proc.Ctx.Done(): //query aborted
		return 1, nil
	}
}

func Sleep[T uint64 | float64](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[uint8](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[T](ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			return moerr.NewInvalidArg(proc.Ctx, "sleep", "input contains null")
		} else {
			res, err := sleepSeconds(proc, float64(v))
			if err == nil {
				err = rs.Append(res, false)
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func Version(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	versionStr := proc.SessionInfo.GetVersion()

	return opNoneParamToBytes(result, proc, length, func() []byte {
		return function2Util.QuickStrToBytes(versionStr)
	})
}

func GitVersion(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	s := "unknown"
	if version.CommitID != "" {
		s = version.CommitID
	}

	return opNoneParamToBytes(result, proc, length, func() []byte {
		return function2Util.QuickStrToBytes(s)
	})
}

func BuildVersion(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	t, err := strconv.ParseInt(version.BuildTime, 10, 64)
	if err != nil {
		return err
	}
	buildT := types.UnixToTimestamp(t)

	return opNoneParamToFixed[types.Timestamp](result, proc, length, func() types.Timestamp {
		return buildT
	})
}
