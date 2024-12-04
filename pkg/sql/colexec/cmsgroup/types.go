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

package cmsgroup

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	H0 = iota
	H8
	HStr
)

type ExprEvalVector struct {
	Executor []colexec.ExpressionExecutor
	Vec      []*vector.Vector
	Typ      []types.Type
}

func MakeEvalVector(proc *process.Process, expressions []*plan.Expr) (ev ExprEvalVector, err error) {
	if len(expressions) == 0 {
		return
	}

	ev.Executor, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, expressions)
	if err != nil {
		return
	}
	ev.Vec = make([]*vector.Vector, len(ev.Executor))
	ev.Typ = make([]types.Type, len(ev.Executor))
	for i, expr := range expressions {
		ev.Typ[i] = types.New(types.T(expr.Typ.Id), expr.Typ.Width, expr.Typ.Scale)
	}
	return
}

// Group
// the group operator using new implement.
type Group struct {
	vm.OperatorBase
	colexec.Projection

	ctr          container
	NeedEval     bool
	PreAllocSize uint64

	// group-by column.
	Exprs        []*plan.Expr
	GroupingFlag []bool
	// agg info and agg column.
	Aggs []aggexec.AggFuncExecExpression
}

func (group *Group) evaluateGroupByAndAgg(proc *process.Process, bat *batch.Batch) (err error) {
	input := []*batch.Batch{bat}

	// group.
	for i := range group.ctr.groupByEvaluate.Vec {
		if group.ctr.groupByEvaluate.Vec[i], err = group.ctr.groupByEvaluate.Executor[i].Eval(proc, input, nil); err != nil {
			return err
		}
	}

	// agg.
	for i := range group.ctr.aggregateEvaluate {
		for j := range group.ctr.aggregateEvaluate[i].Vec {
			if group.ctr.aggregateEvaluate[i].Vec[j], err = group.ctr.aggregateEvaluate[i].Executor[j].Eval(proc, input, nil); err != nil {
				return err
			}
		}
	}

	return nil
}

// container
// running context.
type container struct {
	state             vm.CtrState
	dataSourceIsEmpty bool

	// hash.
	hr          ResHashRelated
	mtyp        int
	keyWidth    int
	keyNullable bool

	// x, y of `group by x, y`.
	// m, n of `select agg1(m, n), agg2(m, n)`.
	groupByEvaluate   ExprEvalVector
	aggregateEvaluate []ExprEvalVector

	// result if NeedEval is true.
	result1 GroupResultBuffer
	// result if NeedEval is false.
	result2 GroupResultNoneBlock
}

func (ctr *container) isDataSourceEmpty() bool {
	return ctr.dataSourceIsEmpty
}

func (group *Group) Free(proc *process.Process, _ bool, _ error) {
	group.ctr.hr.Free0()
	group.ctr.result1.Free0(proc.Mp())
	group.ctr.result2.Free0(proc.Mp())
	group.ctr.freeAggEvaluate()
	group.ctr.freeGroupEvaluate()
}

func (ctr *container) freeAggEvaluate() {
	for i := range ctr.aggregateEvaluate {
		for j := range ctr.aggregateEvaluate[i].Executor {
			ctr.aggregateEvaluate[i].Executor[j].Free()
		}
	}
	ctr.aggregateEvaluate = nil
}

func (ctr *container) freeGroupEvaluate() {
	for i := range ctr.groupByEvaluate.Executor {
		ctr.groupByEvaluate.Executor[i].Free()
	}
	ctr.groupByEvaluate = ExprEvalVector{}
}
