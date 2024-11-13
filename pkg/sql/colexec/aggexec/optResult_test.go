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

package aggexec

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
	"testing"
)

// there is very important to check the result's extend first.
//
// we do test for the following three cases:
// 1. the using block is enough to save all the data.
// 2. the pre-allocated block is enough to save all the data.
// 3. the unused space is not enough, and we need to append new block.
func TestOptResultExtend(t *testing.T) {
	blockLimitation := 100

	mg := SimpleAggMemoryManager{mp: mpool.MustNewZeroNoFixed()}
	{
		osr := optSplitResult{}
		osr.init(mg, types.T_bool.ToType())
		osr.optInformation.eachSplitCapacity = blockLimitation
		osr.noNeedToCountEmptyGroup()

		// pre extend 130 rows.
		require.NoError(t, osr.preAllocate(130))
		checkRowDistribution(t, []int{0, 0}, osr.resultList)
		checkCapSituation(t, []int{100, 30}, osr.resultList, osr.optInformation.eachSplitCapacity)

		// case 1 : extend 50 only use the first block.
		require.NoError(t, osr.extend(50))
		checkRowDistribution(t, []int{50, 0}, osr.resultList)
		checkCapSituation(t, []int{100, 30}, osr.resultList, osr.optInformation.eachSplitCapacity)

		// case 2 : extend 75 will full the first block and set 1 row to the second block.
		require.NoError(t, osr.extend(75))
		checkRowDistribution(t, []int{100, 25}, osr.resultList)
		checkCapSituation(t, []int{100, 30}, osr.resultList, osr.optInformation.eachSplitCapacity)

		// case 3 : extend 200 will full the last block and append 2 more blocks.
		require.NoError(t, osr.extend(200))
		checkRowDistribution(t, []int{100, 100, 100, 25}, osr.resultList)
		checkCapSituation(t, []int{100, 100, 100, 25}, osr.resultList, osr.optInformation.eachSplitCapacity)
	}
}

func checkRowDistribution(t *testing.T, expected []int, src []*vector.Vector) {
	require.Equal(t, len(expected), len(src))

	for i := range src {
		require.NotNil(t, src[i])
	}
	for i := range expected {
		require.Equal(t, expected[i], src[i].Length())
	}
}

func checkCapSituation(t *testing.T, expected []int, src []*vector.Vector, ourLimitation int) {
	require.Equal(t, len(expected), len(src))

	for i := range src {
		require.NotNil(t, src[i])
	}
	for i := range expected {
		require.LessOrEqual(t, expected[i], min(src[i].Capacity(), ourLimitation))
	}
}
