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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

// TIMEDIFF

func initTimeDiffInTimeTestCase() []tcTemp {
	//Test Set 1
	t11, _ := types.ParseTime("22:22:22", 6)
	t12, _ := types.ParseTime("11:11:11", 6)
	r1, _ := types.ParseTime("11:11:11", 6)

	t21, _ := types.ParseTime("22:22:22", 6)
	t22, _ := types.ParseTime("-11:11:11", 6)
	r2, _ := types.ParseTime("33:33:33", 6)

	t31, _ := types.ParseTime("-22:22:22", 6)
	t32, _ := types.ParseTime("11:11:11", 6)
	r3, _ := types.ParseTime("-33:33:33", 6)

	t41, _ := types.ParseTime("-22:22:22", 6)
	t42, _ := types.ParseTime("-11:11:11", 6)
	r4, _ := types.ParseTime("-11:11:11", 6)

	//Test Set 2
	t51, _ := types.ParseTime("11:11:11", 6)
	t52, _ := types.ParseTime("22:22:22", 6)
	r5, _ := types.ParseTime("-11:11:11", 6)

	t61, _ := types.ParseTime("11:11:11", 6)
	t62, _ := types.ParseTime("-22:22:22", 6)
	r6, _ := types.ParseTime("33:33:33", 6)

	t71, _ := types.ParseTime("-11:11:11", 6)
	t72, _ := types.ParseTime("22:22:22", 6)
	r7, _ := types.ParseTime("-33:33:33", 6)

	t81, _ := types.ParseTime("-11:11:11", 6)
	t82, _ := types.ParseTime("-22:22:22", 6)
	r8, _ := types.ParseTime("11:11:11", 6)

	//Test Set 3
	t91, _ := types.ParseTime("-2562047787:59:59", 6)
	t92, _ := types.ParseTime("-2562047787:59:59", 6)
	r9, _ := types.ParseTime("00:00:00", 6)

	t101, _ := types.ParseTime("2562047787:59:59", 6)
	t102, _ := types.ParseTime("2562047787:59:59", 6)
	r10, _ := types.ParseTime("00:00:00", 6)

	return []tcTemp{
		{
			info: "test timediff time 1",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_time.ToType(), []types.Time{t11, t21, t31, t41, t51, t61, t71, t81, t91, t101}, []bool{}),
				testutil.NewFunctionTestInput(types.T_time.ToType(), []types.Time{t12, t22, t32, t42, t52, t62, t72, t82, t92, t102}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_time.ToType(), false, []types.Time{r1, r2, r3, r4, r5, r6, r7, r8, r9, r10}, []bool{}),
		},
	}
}

func TestTimeDiffInTime(t *testing.T) {
	testCases := initTimeDiffInTimeTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, TimeDiff[types.Time])
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initTimeDiffInDatetimeTestCase() []tcTemp {
	// Test case 1
	t11, _ := types.ParseDatetime("2012-12-12 22:22:22", 6)
	t12, _ := types.ParseDatetime("2012-12-12 11:11:11", 6)
	r1, _ := types.ParseTime("11:11:11", 0)

	// Test case 2
	t21, _ := types.ParseDatetime("2012-12-12 11:11:11", 6)
	t22, _ := types.ParseDatetime("2012-12-12 22:22:22", 6)
	r2, _ := types.ParseTime("-11:11:11", 0)

	// Test case 3
	t31, _ := types.ParseDatetime("2012-12-12 22:22:22", 6)
	t32, _ := types.ParseDatetime("2000-12-12 11:11:11", 6)
	r3, _ := types.ParseTime("105203:11:11", 0)

	// Test case 4
	t41, _ := types.ParseDatetime("2000-12-12 11:11:11", 6)
	t42, _ := types.ParseDatetime("2012-12-12 22:22:22", 6)
	r4, _ := types.ParseTime("-105203:11:11", 0)

	// Test case 5
	t51, _ := types.ParseDatetime("2012-12-12 22:22:22", 6)
	t52, _ := types.ParseDatetime("2012-10-10 11:11:11", 6)
	r5, _ := types.ParseTime("1523:11:11", 0)

	// Test case 6
	t61, _ := types.ParseDatetime("2012-10-10 11:11:11", 6)
	t62, _ := types.ParseDatetime("2012-12-12 22:22:22", 6)
	r6, _ := types.ParseTime("-1523:11:11", 0)

	// Test case 7
	t71, _ := types.ParseDatetime("2012-12-12 22:22:22", 6)
	t72, _ := types.ParseDatetime("2012-12-10 11:11:11", 6)
	r7, _ := types.ParseTime("59:11:11", 0)

	// Test case 8
	t81, _ := types.ParseDatetime("2012-12-10 11:11:11", 6)
	t82, _ := types.ParseDatetime("2012-12-12 22:22:22", 6)
	r8, _ := types.ParseTime("-59:11:11", 0)

	// Test case 9
	t91, _ := types.ParseDatetime("2012-12-10 11:11:11", 6)
	t92, _ := types.ParseDatetime("2012-12-10 11:11:11", 6)
	r9, _ := types.ParseTime("00:00:00", 0)

	return []tcTemp{
		{
			info: "test Datetimediff Datetime 1",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{t11, t21, t31, t41, t51, t61, t71, t81, t91}, []bool{}),
				testutil.NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{t12, t22, t32, t42, t52, t62, t72, t82, t92}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_time.ToType(), false, []types.Time{r1, r2, r3, r4, r5, r6, r7, r8, r9}, []bool{}),
		},
	}
}

func TestTimeDiffInDateTime(t *testing.T) {
	testCases := initTimeDiffInDatetimeTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, TimeDiff[types.Datetime])
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// TIMESTAMPDIFF

func initTimestampDiffTestCase() []tcTemp {
	// FIXME: Migrating the testcases as it was from the original functions code. May refactor it later. Original code: https://github.com/m-schen/matrixone/blob/0c480ca11b6302de26789f916a3e2faca7f79d47/pkg/sql/plan/function/builtin/multi/timestampdiff_test.go#L35
	cases := []struct {
		name   string
		inputs []string
		want   int64
	}{
		{
			name:   "TEST01",
			inputs: []string{"2017-12-01 12:15:12", "2018-01-01 7:18:20", "microsecond"},
			want:   2660588000000,
		},
		{
			name:   "TEST02",
			inputs: []string{"2017-12-01 12:15:12", "2018-01-01 7:18:20", "second"},
			want:   2660588,
		},
		{
			name:   "TEST03",
			inputs: []string{"2017-12-01 12:15:12", "2018-01-01 7:18:20", "minute"},
			want:   44343,
		},
		{
			name:   "TEST04",
			inputs: []string{"2017-12-01 12:15:12", "2018-01-01 7:18:20", "hour"},
			want:   739,
		},
		{
			name:   "TEST05",
			inputs: []string{"2017-12-01 12:15:12", "2018-01-01 7:18:20", "day"},
			want:   30,
		},
		{
			name:   "TEST06",
			inputs: []string{"2017-12-01 12:15:12", "2018-01-08 12:15:12", "week"},
			want:   5,
		},
		{
			name:   "TEST07",
			inputs: []string{"2017-11-01 12:15:12", "2018-01-01 12:15:12", "month"},
			want:   2,
		},
		{
			name:   "TEST08",
			inputs: []string{"2017-01-01 12:15:12", "2018-01-01 12:15:12", "quarter"},
			want:   4,
		},
		{
			name:   "TEST09",
			inputs: []string{"2017-01-01 12:15:12", "2018-01-01 12:15:12", "year"},
			want:   1,
		},
		{
			name:   "TEST10",
			inputs: []string{"2018-01-01 7:18:20", "2017-12-01 12:15:12", "microsecond"},
			want:   -2660588000000,
		},
		{
			name:   "TEST11",
			inputs: []string{"2018-01-01 7:18:20", "2017-12-01 12:15:12", "second"},
			want:   -2660588,
		},
		{
			name:   "TEST12",
			inputs: []string{"2018-01-01 7:18:20", "2017-12-01 12:15:12", "minute"},

			want: -44343,
		},
		{
			name:   "TEST13",
			inputs: []string{"2018-01-01 7:18:20", "2017-12-01 12:15:12", "hour"},

			want: -739,
		},
		{
			name:   "TEST14",
			inputs: []string{"2018-01-01 7:18:20", "2017-12-01 12:15:12", "day"},

			want: -30,
		},
		{
			name:   "TEST15",
			inputs: []string{"2018-01-08 12:15:12", "2017-12-01 12:15:12", "week"},
			want:   -5,
		},
		{
			name:   "TEST16",
			inputs: []string{"2018-01-01 12:15:12", "2017-11-01 12:15:12", "month"},
			want:   -2,
		},
		{
			name:   "TEST17",
			inputs: []string{"2018-01-01 12:15:12", "2017-01-01 12:15:12", "quarter"},
			want:   -4,
		},
		{
			name:   "TEST18",
			inputs: []string{"2018-01-01 12:15:12", "2017-01-01 12:15:12", "year"},
			want:   -1,
		},
	}

	var testInputs []tcTemp
	for _, c := range cases {

		i1 := c.inputs[2]
		i2, _ := types.ParseDatetime(c.inputs[0], 6)
		i3, _ := types.ParseDatetime(c.inputs[1], 6)

		o := c.want

		testInputs = append(testInputs, tcTemp{

			info: "test TimestampDiff " + c.name,
			inputs: []testutil.FunctionTestInput{
				// Create a input entry <String, Datetime1, Datetime2>
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{i1}, []bool{}),
				testutil.NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{i2}, []bool{}),
				testutil.NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{i3}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false, []int64{o}, []bool{}),
		})
	}

	return testInputs
}

func TestTimestampDiff(t *testing.T) {
	testCases := initTimestampDiffTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, TimestampDiff)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initAddFaultPointTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test space",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"a"}, []bool{false}),
				testutil.NewFunctionTestConstInput(types.T_varchar.ToType(), []string{":5::"}, []bool{false}),
				testutil.NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"return"}, []bool{false}),
				testutil.NewFunctionTestConstInput(types.T_int64.ToType(), []int64{0}, []bool{false}),
				testutil.NewFunctionTestConstInput(types.T_varchar.ToType(), []string{""}, []bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_bool.ToType(), true,
				[]bool{true},
				[]bool{false}),
		},
	}
}

func TestAddFaultPoint(t *testing.T) {
	testCases := initAddFaultPointTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, AddFaultPoint)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initCeilTestCase() []tcTemp {
	rfs := []float64{1, -1, -2, math.MinInt64 + 1, math.MinInt64 + 2, -100, -1, 1,
		0, 2, 5, 9, 17, 33, 65, math.MaxInt64, math.MaxFloat64, 0}
	fs := []float64{math.SmallestNonzeroFloat64, -1.2, -2.3, math.MinInt64 + 1, math.MinInt64 + 2, -100.2, -1.3, 0.9, 0,
		1.5, 4.4, 8.5, 16.32, 32.345, 64.09, math.MaxInt64, math.MaxFloat64, 0}
	bs := make([]bool, len(fs))
	return []tcTemp{
		{
			info: "test ceil",
			typ:  types.T_uint64,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{1, 4, 8, 16, 32, math.MaxUint64, 0},
					[]bool{false, false, false, false, false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{1, 4, 8, 16, 32, math.MaxUint64, 0},
				[]bool{false, false, false, false, false, false, false}),
		},
		{
			info: "test ceil",
			typ:  types.T_int64,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{math.MinInt64 + 1, math.MinInt64 + 2, -100, -1, 0, 1, 4, 8, 16, 32, 64, math.MaxInt64, 0},
					[]bool{false, false, false, false, false, false, false, false, false, false, false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{math.MinInt64 + 1, math.MinInt64 + 2, -100, -1, 0, 1, 4, 8, 16, 32, 64, math.MaxInt64, 0},
				[]bool{false, false, false, false, false, false, false, false, false, false, false, false, false}),
		},
		{
			info: "test ceil",
			typ:  types.T_float64,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(),
					fs,
					bs),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				rfs,
				bs),
		},
	}
}

func TestCeil(t *testing.T) {
	testCases := initCeilTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		var fcTC testutil.FunctionTestCase
		switch tc.typ {
		case types.T_uint64:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, CeilUint64)
		case types.T_int64:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, CeilInt64)
		case types.T_float64:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, CeilFloat64)
		}
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initEndsWithTestCase() []tcTemp {
	// FIXME: Migrating the testcases as it was from the original functions code. May refactor it later. Original code:https://github.com/m-schen/matrixone/blob/0c480ca11b6302de26789f916a3e2faca7f79d47/pkg/sql/plan/function/builtin/binary/endswith_test.go#L29
	var charVecBase = []string{"123-", "321", "123+", "8", ""}
	var charVecBase2 = []string{"-", "+", "1", ""}
	var nsp1, nsp2 []uint64
	var origVecs = make([]testutil.FunctionTestInput, 2)
	n1, n2 := len(charVecBase), len(charVecBase2)
	inputVec := make([]string, n1*n2)
	inputVec2 := make([]string, len(inputVec))
	for i := 0; i < len(inputVec); i++ {
		inputVec[i] = charVecBase[i/n2]
		inputVec2[i] = charVecBase2[i%n2]
		if (i / n2) == (n1 - 1) {
			nsp1 = append(nsp1, uint64(i))
		}
		if (i % n2) == (n2 - 1) {
			nsp2 = append(nsp2, uint64(i))
		}
	}

	makeFunctionTestInputEndsWith := func(values []string, nsp []uint64) testutil.FunctionTestInput {
		totalCount := len(values)
		strs := make([]string, totalCount)
		nulls := make([]bool, totalCount)
		for i := 0; i < totalCount; i++ {
			strs[i] = values[i]
		}
		for i := 0; i < len(nsp); i++ {
			idx := nsp[i]
			nulls[idx] = true
		}
		return testutil.NewFunctionTestInput(types.T_varchar.ToType(), strs, nulls)
	}

	origVecs[0] = makeFunctionTestInputEndsWith(inputVec, nsp1)
	origVecs[1] = makeFunctionTestInputEndsWith(inputVec2, nsp2)

	return []tcTemp{
		{
			info: "test EndsWith",
			inputs: []testutil.FunctionTestInput{
				origVecs[0],
				origVecs[1],
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{1, 0, 0, 1, 0, 0, 1, 1, 0, 1, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1},
				[]bool{false, false, false, true, false, false, false, true, false, false, false, true, false, false, false, true, true, true, true, true}),
		},
	}
}

func TestEndsWith(t *testing.T) {
	testCases := initEndsWithTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, EndsWith)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initFindInSetTestCase() []tcTemp {

	return []tcTemp{
		{
			info: "test findinset",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{
					"abc",
					"xyz",
					"z",
					"abc", //TODO: Ignoring the scalar checks. Please fix. Original code:https://github.com/m-schen/matrixone/blob/0c480ca11b6302de26789f916a3e2faca7f79d47/pkg/sql/plan/function/builtin/binary/findinset_test.go#L67
					"abc",
					"abc",
					"",
					"abc",
				},
					[]bool{
						false,
						false,
						false,
						false,
						false,
						false,
						true,
						false,
					}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{
					"abc,def",
					"dec,xyz,abc",
					"a,e,c,z",
					"abc,def",
					"abc,def",
					"abc,def",
					"abc",
					"",
				},
					[]bool{
						false,
						false,
						false,
						false,
						false,
						false,
						false,
						true,
					}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{
					1,
					2,
					4,
					1,
					1,
					1,
					0,
					0,
				},
				[]bool{
					false,
					false,
					false,
					false,
					false,
					false,
					true,
					true,
				},
			),
		},
	}
}

func TestFindInSet(t *testing.T) {
	testCases := initFindInSetTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, FindInSet)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// INSTR
func initInstrTestCase() []tcTemp {
	cases := []struct {
		strs    []string
		substrs []string
		wants   []int64
	}{
		{
			strs:    []string{"abc", "abc", "abc", "abc", "abc"},
			substrs: []string{"bc", "b", "abc", "a", "dca"},
			wants:   []int64{2, 2, 1, 1, 0},
		},
		{
			strs:    []string{"abc", "abc", "abc", "abc", "abc"},
			substrs: []string{"", "", "a", "b", "c"},
			wants:   []int64{1, 1, 1, 2, 3},
		},
		//TODO: @m-schen. Please fix these. Original code: https://github.com/m-schen/matrixone/blob/0c480ca11b6302de26789f916a3e2faca7f79d47/pkg/sql/plan/function/builtin/binary/instr_test.go#L43
		//{
		//	strs:    []string{"abc", "abc", "abc", "abc", "abc"},
		//	substrs: []string{"bc"},
		//	wants:   []int64{2, 2, 2, 2, 2},
		//},
		//{
		//	strs:    []string{"abc"},
		//	substrs: []string{"bc", "b", "abc", "a", "dca"},
		//	wants:   []int64{2, 2, 1, 1, 0},
		//},
	}

	var testInputs []tcTemp
	for _, c := range cases {

		testInputs = append(testInputs, tcTemp{

			info: "test instr ",
			inputs: []testutil.FunctionTestInput{
				// Create a input entry <strs, substrs>
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), c.strs, []bool{}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), c.substrs, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false, c.wants, []bool{}),
		})
	}

	return testInputs

}

func TestInstr(t *testing.T) {
	testCases := initInstrTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, Instr)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// Left
func initLeftTestCase() []tcTemp {
	cases := []struct {
		s    string
		len  int64
		want string
	}{
		{
			"abcde",
			3,
			"abc",
		},
		{
			"abcde",
			0,
			"",
		},
		{
			"abcde",
			-1,
			"",
		},
		{
			"abcde",
			100,
			"abcde",
		},
		{
			"foobarbar",
			5,
			"fooba",
		},

		// TestLeft1
		{
			"是都方式快递费",
			3,
			"是都方",
		},
		{
			"ｱｲｳｴｵ",
			3,
			"ｱｲｳ",
		},
		{
			"ｱｲｳｴｵ ",
			3,
			"ｱｲｳ",
		},
		{
			"ｱｲｳｴｵ  ",
			3,
			"ｱｲｳ",
		},
		{
			"ｱｲｳｴｵ   ",
			3,
			"ｱｲｳ",
		},
		{
			"あいうえお",
			3,
			"あいう",
		},
		{
			"あいうえお ",
			3,
			"あいう",
		},
		{
			"あいうえお  ",
			3,
			"あいう",
		},
		{
			"あいうえお   ",
			3,
			"あいう",
		},
		{
			"龔龖龗龞龡",
			3,
			"龔龖龗",
		},
		{
			"龔龖龗龞龡 ",
			3,
			"龔龖龗",
		},
		{
			"龔龖龗龞龡  ",
			3,
			"龔龖龗",
		},
		{
			"龔龖龗龞龡   ",
			3,
			"龔龖龗",
		},
		{
			"2017-06-15    ",
			8,
			"2017-06-",
		},
		{
			"2019-06-25    ",
			8,
			"2019-06-",
		},
		{
			"    2019-06-25  ",
			8,
			"    2019",
		},
		{
			"   2019-06-25   ",
			8,
			"   2019-",
		},
		{
			"    2012-10-12   ",
			8,
			"    2012",
		},
		{
			"   2004-04-24.   ",
			8,
			"   2004-",
		},
		{
			"   2008-12-04.  ",
			8,
			"   2008-",
		},
		{
			"    2012-03-23.   ",
			8,
			"    2012",
		},
		{
			"    2013-04-30  ",
			8,
			"    2013",
		},
		{
			"  1994-10-04  ",
			8,
			"  1994-1",
		},
		{
			"   2018-06-04  ",
			8,
			"   2018-",
		},
		{
			" 2012-10-12  ",
			8,
			" 2012-10",
		},
		{
			"1241241^&@%#^*^!@#&*(!&    ",
			12,
			"1241241^&@%#",
		},
		{
			" 123 ",
			2,
			" 1",
		},
	}

	var testInputs []tcTemp
	for _, c := range cases {

		testInputs = append(testInputs, tcTemp{

			//TODO: Avoiding TestLeft2. Original code: https://github.com/m-schen/matrixone/blob/0c480ca11b6302de26789f916a3e2faca7f79d47/pkg/sql/plan/function/builtin/binary/left_test.go#L247
			info: "test left ",
			inputs: []testutil.FunctionTestInput{
				// Create a input entry <str, int>
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{c.s}, []bool{}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{c.len}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false, []string{c.want}, []bool{}),
		})
	}

	return testInputs

}

func TestLeft(t *testing.T) {
	testCases := initLeftTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, Left)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// StartsWith

func initStartsWithTestCase() []tcTemp {
	// FIXME: Migrating the testcases as it was from the original functions code. May refactor it later. Original code:https://github.com/m-schen/matrixone/blob/0c480ca11b6302de26789f916a3e2faca7f79d47/pkg/sql/plan/function/builtin/binary/startswith_test.go#L28
	var charVecBase = []string{"-123", "123", "+123", "8", ""}
	var charVecBase2 = []string{"-", "+", "1", ""}
	var nsp1, nsp2 []uint64
	var origVecs = make([]testutil.FunctionTestInput, 2)
	n1, n2 := len(charVecBase), len(charVecBase2)
	inputVec := make([]string, n1*n2)
	inputVec2 := make([]string, len(inputVec))
	for i := 0; i < len(inputVec); i++ {
		inputVec[i] = charVecBase[i/n2]
		inputVec2[i] = charVecBase2[i%n2]
		if (i / n2) == (n1 - 1) {
			nsp1 = append(nsp1, uint64(i))
		}
		if (i % n2) == (n2 - 1) {
			nsp2 = append(nsp2, uint64(i))
		}
	}

	makeFunctionTestInputEndsWith := func(values []string, nsp []uint64) testutil.FunctionTestInput {
		totalCount := len(values)
		strs := make([]string, totalCount)
		nulls := make([]bool, totalCount)
		for i := 0; i < totalCount; i++ {
			strs[i] = values[i]
		}
		for i := 0; i < len(nsp); i++ {
			idx := nsp[i]
			nulls[idx] = true
		}
		return testutil.NewFunctionTestInput(types.T_varchar.ToType(), strs, nulls)
	}

	origVecs[0] = makeFunctionTestInputEndsWith(inputVec, nsp1)
	origVecs[1] = makeFunctionTestInputEndsWith(inputVec2, nsp2)

	return []tcTemp{
		{
			info: "test StartsWith",
			inputs: []testutil.FunctionTestInput{
				origVecs[0],
				origVecs[1],
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{1, 0, 0, 1, 0, 0, 1, 1, 0, 1, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1},
				[]bool{false, false, false, true, false, false, false, true, false, false, false, true, false, false, false, true, true, true, true, true}),
		},
	}
}

func TestStartsWith(t *testing.T) {
	testCases := initStartsWithTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, StartsWith)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}
