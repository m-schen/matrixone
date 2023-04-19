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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/builtin/unary"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var supportedBuiltins = []FuncNew{
	// function `current_timestamp`
	{
		functionId: CURRENT_TIMESTAMP,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 0 {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				retType: func(parameters []types.Type) types.Type {
					return types.T_timestamp.ToType()
				},
				NewOp: builtInCurrentTimestamp,
			},
		},
	},

	// function `mo_show_visible_bin`
	{
		functionId: MO_SHOW_VISIBLE_BIN,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				volatile:   true,
				args:       []types.T{types.T_varchar, types.T_uint8},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: builtInMoShowVisibleBin,
			},
		},
	},

	// function `internal_char_length`
	{
		functionId: INTERNAL_CHAR_LENGTH,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				volatile:   true,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: builtInInternalCharLength,
			},
		},
	},

	// function `internal_char_size`
	{
		functionId: INTERNAL_CHAR_SIZE,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				volatile:   true,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: builtInInternalCharSize,
			},
		},
	},

	// function `internal_numeric_precision`
	{
		functionId: INTERNAL_NUMERIC_PRECISION,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				volatile:   true,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: builtInInternalNumericPrecision,
			},
		},
	},

	// function `internal_numeric_scale`
	{
		functionId: INTERNAL_NUMERIC_SCALE,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				volatile:   true,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: builtInInternalNumericScale,
			},
		},
	},

	// function `internal_datetime_scale`
	{
		functionId: INTERNAL_DATETIME_SCALE,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				volatile:   true,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: builtInInternalDatetimeScale,
			},
		},
	},

	// function `internal_column_character_set`
	{
		functionId: INTERNAL_COLUMN_CHARACTER_SET,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				volatile:   true,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				NewOp: builtInInternalCharacterSet,
			},
		},
	},

	// function `internal_auto_increment`
	// 'internal_auto_increment' is used to obtain the current auto_increment column value of the table under the specified database
	{
		functionId: INTERNAL_AUTO_INCREMENT,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				volatile:   true,
				args:       []types.T{types.T_varchar, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				NewOp: builtInInternalAutoIncrement,
			},
		},
	},

	// function `concat`
	{
		functionId: CONCAT,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    builtInConcatCheck,

		Overloads: []overload{
			{
				overloadId: 0,
				retType: func(parameters []types.Type) types.Type {
					for _, p := range parameters {
						if p.Oid == types.T_binary || p.Oid == types.T_varbinary || p.Oid == types.T_blob {
							return types.T_blob.ToType()
						}
					}
					return types.T_varchar.ToType()
				},
				NewOp: builtInConcat,
			},
		},
	},

	// function `mo_log_date`
	{
		functionId: MO_LOG_DATE,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				volatile:   true,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_date.ToType()
				},
				NewOp: builtInMoLogDate,
			},
		},
	},

	// function `regexp_substr`
	{
		functionId: REGEXP_SUBSTR,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar, types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: builtInRegexpSubstr,
			},

			{
				overloadId: 1,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: builtInRegexpSubstr,
			},

			{
				overloadId: 2,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: builtInRegexpSubstr,
			},
		},
	},

	// function `Database`
	{
		functionId: DATABASE,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				volatile:   true,
				args:       nil,
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				NewOp: builtInDatabase,
			},
		},
	},

	// function `str_to_date`, `to_date`
	{
		functionId: STR_TO_DATE,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_datetime},
				retType: func(parameters []types.Type) types.Type {
					return types.T_datetime.ToType()
				},

				NewOp: builtInStrToDatetime,
			},

			{
				overloadId: 1,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_date},
				retType: func(parameters []types.Type) types.Type {
					return types.T_date.ToType()
				},

				NewOp: builtInStrToDate,
			},

			{
				overloadId: 2,
				args:       []types.T{types.T_varchar, types.T_varchar, types.T_time},
				retType: func(parameters []types.Type) types.Type {
					return types.T_time.ToType()
				},

				NewOp: builtInStrToTime,
			},
		},
	},

	// function `sin`
	{
		functionId: SIN,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: unary.Sin,
			},
		},
	},

	// function `cos`
	{
		functionId: COS,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: unary.Cos,
			},
		},
	},

	// function `cot`
	{
		functionId: COT,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: unary.Cot,
			},
		},
	},

	// function `tan`
	{
		functionId: TAN,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: unary.Tan,
			},
		},
	},

	// function `sinh`
	{
		functionId: SINH,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: unary.Sinh,
			},
		},
	},

	// function `acos`
	{
		functionId: ACOS,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: unary.Acos,
			},
		},
	},

	// function `exp`
	{
		functionId: EXP,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: unary.Exp,
			},
		},
	},

	// function `ln`
	{
		functionId: LN,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: unary.Ln,
			},
		},
	},

	// function `atan`
	{
		functionId: ATAN,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: builtInATan,
			},

			{
				overloadId: 1,
				args:       []types.T{types.T_float64, types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: unary.Atan,
			},
		},
	},

	// function `rand`, `rand(1)`
	{
		functionId: RANDOM,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
					r := new(opBuiltInRand)
					return r.builtInRand(parameters, result, proc, length)
				},
			},

			{
				overloadId: 1,
				args:       nil,
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				NewOp: builtInRand,
			},
		},
	},
}
