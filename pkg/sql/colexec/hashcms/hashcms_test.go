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

package hashcms

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"testing"
)

// memoryDiskForTest write data to memory, and support read from it.
// this is just for test the package.
type memoryDiskForTest struct {
	data map[string][]byte
}

func (md *memoryDiskForTest) Write(ctx context.Context, vector fileservice.IOVector) error {
	md.data[vector.FilePath] = vector.Entries[0].Data
	return nil
}
func (md *memoryDiskForTest) Read(ctx context.Context, vector *fileservice.IOVector) error {
	if v, ok := md.data[vector.FilePath]; ok {
		vector.Entries[0].Data = v
		return nil
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("cannot found data with filepath %s", vector.FilePath))
}
func (md *memoryDiskForTest) Delete(ctx context.Context, filePaths ...string) error {
	for _, filePath := range filePaths {
		delete(md.data, filePath)
	}
	return nil
}
func (md *memoryDiskForTest) Close() {
	md.data = nil
}

// TestSpilledHashMap1 do test for SpilledHashMap's basic write and read.
//
// we should ensure that, SpilledHashMap can read a batch with same content as the wrote one.
func TestSpilledHashMap1(t *testing.T) {
	proc := testutil.NewProcess()
	mp := proc.Mp()
	usrCtx := context.TODO()
	srv := &memoryDiskForTest{
		data: make(map[string][]byte, 10),
	}
	spilledHm := InitSpilledHashMap(
		usrCtx, srv, mp, true, 8, false)

	// src batch.
	src := &batch.Batch{}
	v1 := testutil.NewInt64Vector(10, types.T_int64.ToType(), mp, false,
		[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	v2 := testutil.NewBoolVector(10, types.T_bool.ToType(), mp, false,
		[]bool{true, true, true, true, true, false, false, false, false, false})
	src.Attrs = []string{"column1", "column2"}
	src.Vecs = []*vector.Vector{v1, v2}
	src.SetRowCount(10)

	col := &plan.Expr{
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 0,
				ColPos: 0,
			},
		},
		Typ: plan.Type{
			Id:          int32(types.T_int64),
			NotNullable: true,
		},
	}

	executor, err := colexec.NewExpressionExecutor(proc, col)
	require.NoError(t, err)

	// test write.
	require.NoError(t, spilledHm.StoreBatch(proc, src, executor))
	require.Equal(t, 1, spilledHm.Blocks())

	// test read.
	var dst *batch.Batch
	dst, err = spilledHm.ReadBatchByIndex(0)
	require.NoError(t, err)

	// compare src and dst.
	{
		require.NotNil(t, dst)
		require.Equal(t, src.RowCount(), dst.RowCount())
		require.Equal(t, len(src.Vecs), len(dst.Vecs))
		for i, originVec := range src.Vecs {
			gotVec := dst.Vecs[i]
			t1, t2 := *originVec.GetType(), *gotVec.GetType()
			require.Equal(t, t1, t2)

			switch t1.Oid {
			case types.T_int64:
				vs1 := vector.MustFixedColNoTypeCheck[int64](originVec)
				vs2 := vector.MustFixedColNoTypeCheck[int64](gotVec)
				require.Equal(t, len(vs1), len(vs2))
				for j := range vs1 {
					require.Equal(t, vs1[j], vs2[j])
				}

			case types.T_bool:
				vs1 := vector.MustFixedColNoTypeCheck[bool](originVec)
				vs2 := vector.MustFixedColNoTypeCheck[bool](gotVec)
				require.Equal(t, len(vs1), len(vs2))
				for j := range vs1 {
					require.Equal(t, vs1[j], vs2[j])
				}
			}
		}
	}

	// test close.
	executor.Free()
	spilledHm.Close()
}
