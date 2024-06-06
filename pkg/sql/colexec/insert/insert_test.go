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

package insert

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

type mockRelation struct {
	engine.Relation
	result *batch.Batch
}

func (e *mockRelation) Write(_ context.Context, b *batch.Batch) error {
	e.result = b
	return nil
}

func TestInsertOperator(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.TODO()
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(ctx).Return(nil).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()

	proc := testutil.NewProc()
	proc.TxnClient = txnClient
	proc.Ctx = ctx
	mp := proc.Mp()

	batch1 := batch.NewWithSize(5)
	batch1.Vecs[0] = testutil.NewInt64Vector(3, types.T_int64.ToType(), mp, false, []int64{1, 2, 0})
	batch1.Vecs[1], _ = vector.NewConstFixed[int64](types.T_int64.ToType(), 3, 3, mp)
	batch1.Vecs[2] = testutil.NewStringVector(3, types.T_varchar.ToType(), mp, false, []string{"a", "b", "c"})
	batch1.Vecs[3], _ = vector.NewConstBytes(types.T_varchar.ToType(), []byte("d"), 3, mp)
	batch1.Vecs[4] = vector.NewConstNull(types.T_int64.ToType(), 3, mp)

	batch2 := batch.NewWithSize(5)
	batch2.Vecs[0] = testutil.NewInt64Vector(3, types.T_int64.ToType(), mp, false, []int64{1, 2, 0})
	batch2.Vecs[1], _ = vector.NewConstFixed[int64](types.T_int64.ToType(), 3, 3, mp)
	batch2.Vecs[2] = testutil.NewStringVector(3, types.T_varchar.ToType(), mp, false, []string{"a", "b", "c"})
	batch2.Vecs[3], _ = vector.NewConstBytes(types.T_varchar.ToType(), []byte("d"), 3, mp)
	batch2.Vecs[4] = vector.NewConstNull(types.T_int64.ToType(), 3, mp)

	inputs := []*batch.Batch{batch1, batch2, nil}

	argument1 := Argument{
		InsertCtx: &InsertCtx{
			Rel: &mockRelation{},
			Ref: &plan.ObjectRef{
				Obj:        0,
				SchemaName: "testDb",
				ObjName:    "testTable",
			},
			AddAffectedRows: true,
			Attrs:           []string{"int64_column", "scalar_int64", "varchar_column", "scalar_varchar", "int64_column"},
		},
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx:     0,
				IsFirst: false,
				IsLast:  false,
			},
		},
		ctr: &container{
			state: vm.Build,
		},
	}
	resetChildren(&argument1, inputs)
	require.NoError(t, argument1.Prepare(proc))

	for {
		result, err := argument1.Call(proc)
		require.NoError(t, err)

		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
	}

	argument1.Free(proc, false, nil)
	argument1.GetChildren(0).Free(proc, false, nil)
	proc.FreeVectors()
	require.Equal(t, int64(0), mp.CurrNB())
}

func resetChildren(arg *Argument, bs []*batch.Batch) {
	arg.SetChildren(
		[]vm.Operator{
			&value_scan.Argument{
				Batchs: bs,
			},
		})

	arg.ctr.state = vm.Build
}
