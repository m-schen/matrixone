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
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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

	executor := gColumnExprExecutor(proc, types.T_int64, 0, true)

	// test write.
	require.NoError(t, spilledHm.StoreBatch(proc, src, executor))
	require.Equal(t, 1, spilledHm.Blocks())
	require.Equal(t, uint64(src.RowCount()), spilledHm.blocks[0].rowCount)

	// test read.
	dst, err := spilledHm.ReadBatchByIndex(0)
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

// TestSpilledHashMap2 do test for building hash map from a unique column.
//
// we should ensure that, SpilledHashMap can build a right kv map from spilled batches.
func TestSpilledHashMap2(t *testing.T) {
	proc := testutil.NewProcess()
	mp := proc.Mp()
	usrCtx := context.Background()
	srv := &memoryDiskForTest{
		data: make(map[string][]byte, 10),
	}

	// from a small batch ( row count < 8192).
	{
		spilledHm := InitSpilledHashMap(
			usrCtx, srv, mp, true, 8, false)

		src := &batch.Batch{}
		vs := make([]int64, 3000)
		for i := range vs {
			vs[i] = int64(i)
		}
		v1 := testutil.NewInt64Vector(len(vs), types.T_int64.ToType(), mp, false, vs)
		src.Vecs = []*vector.Vector{v1}

		executor := gColumnExprExecutor(proc, types.T_int64, 0, false)
		require.NoError(t, spilledHm.StoreBatch(proc, src, executor))

		m, itr, err := spilledHm.BuildHashMapFromIdxes(0)
		require.NoError(t, err)

		err = doInt64HashTableTest(mp, itr,
			vs, []int64{-1, -2, 3001, 3002})
		require.NoError(t, err)

		m.Free()
		spilledHm.Close()
		src.Clean(mp)

		require.Equal(t, int64(0), mp.CurrNB())
	}

	// from a big batch (row count >= 8192).
	{
		spilledHm := InitSpilledHashMap(
			usrCtx, srv, mp, true, 8, false)

		// first input.
		src1 := &batch.Batch{}
		vs1 := make([]int64, 8192)
		for i := range vs1 {
			vs1[i] = int64(i)
		}
		v1 := testutil.NewInt64Vector(len(vs1), types.T_int64.ToType(), mp, false, vs1)
		src1.Vecs = []*vector.Vector{v1}

		// second input.
		src2 := &batch.Batch{}
		vs2 := make([]int64, 8192)
		j := int64(8193)
		for i := range vs2 {
			vs2[i] = j
			j++
		}
		v2 := testutil.NewInt64Vector(len(vs2), types.T_int64.ToType(), mp, false, vs2)
		src2.Vecs = []*vector.Vector{v2}

		executor := gColumnExprExecutor(proc, types.T_int64, 0, false)
		require.NoError(t, spilledHm.StoreBatch(proc, src1, executor))
		require.NoError(t, spilledHm.StoreBatch(proc, src2, executor))

		m, itr, err := spilledHm.BuildHashMapFromIdxes(0, 1)
		require.NoError(t, err)

		err = doInt64HashTableTest(mp, itr,
			append(vs1, vs2...), []int64{-1, -2, 123456})
		require.NoError(t, err)

		m.Free()
		spilledHm.Close()
		src1.Clean(mp)
		src2.Clean(mp)

		require.Equal(t, int64(0), mp.CurrNB())
	}

	srv.Close()
}

// TestSpilledHashMap3 do test for building hash map from a non-unique-constraint column.
//
// we should ensure that, SpilledHashMap can build a right kv map from spilled batches.
func TestSpilledHashMap3(t *testing.T) {

}

func gColumnExprExecutor(
	proc *process.Process,
	typ types.T, columnIdx int32, nullable bool) colexec.ExpressionExecutor {
	col := &plan.Expr{
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 0,
				ColPos: columnIdx,
			},
		},
		Typ: plan.Type{
			Id:          int32(typ),
			NotNullable: nullable,
		},
	}

	executor, err := colexec.NewExpressionExecutor(proc, col)
	if err != nil {
		panic(err)
	}
	return executor
}

func doInt64HashTableTest(
	mp *mpool.MPool,
	itr hashmap.Iterator,
	requireKey []int64,
	notRequireKey []int64) error {

	var vec1, vec2 *vector.Vector
	defer func() {
		if vec1 != nil {
			vec1.Free(mp)
		}
		if vec2 != nil {
			vec2.Free(mp)
		}
	}()

	cannotFound := uint64(0)

	if len(requireKey) > 0 {
		vec1 = testutil.NewInt64Vector(len(requireKey), types.T_int64.ToType(), mp, false, requireKey)

		for i := 0; i < len(requireKey); i += hashmap.UnitLimit {
			n := len(requireKey) - i
			if n > hashmap.UnitLimit {
				n = hashmap.UnitLimit
			}

			zs, _ := itr.Find(i, n, []*vector.Vector{vec1})
			for _, v := range zs {
				if v == cannotFound {
					return moerr.NewInternalErrorNoCtxf("require %d but cannot find it from hashmap", v)
				}
			}
		}
	}

	if len(notRequireKey) > 0 {
		vec2 = testutil.NewInt64Vector(len(notRequireKey), types.T_int64.ToType(), mp, false, notRequireKey)

		for i := 0; i < len(notRequireKey); i += hashmap.UnitLimit {
			n := len(notRequireKey) - i
			if n > hashmap.UnitLimit {
				n = hashmap.UnitLimit
			}

			zs, _ := itr.Find(i, n, []*vector.Vector{vec2})
			for _, v := range zs {
				if v != cannotFound {
					return moerr.NewInternalErrorNoCtxf("require no %d but find it from hashmap", v)
				}
			}
		}
	}

	return nil
}
