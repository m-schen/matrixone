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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
)

// 哈希表 应该包含以下2部分
// 1. 长期处于内存中的一个哈希表，这对于刚好超过 max memory limit的哈希表有很大的作用。
// 2. 外溢到磁盘上的落盘数据，它们可以随时被读取上来构建一个额外的哈希表。需要注意的是，这部分数据需要在查询结束时及时删除。
//
// 对外提供以下能力：
// 1. 提供 Probe() 能力，
//  优先探测部分1，不存在的数据再探测数据2. 需要尽量做优化，对一次探测来说，需要减少IO次数。
//	可能需要对外输出 read io 大小。
//
// 2. 提供 PutBatch() 能力，
//  在内存足够的情况下更新部分1，
//  在内存不够的情况下计算哈希列，然后spill. spill的时候根据一个基础的key值进行分块，暂时定为spill为16个部分。
//	可能需要对外输出 write io 大小。
//
// 3. 提供 Close() 能力，
//  对哈希表所有数据进行清除。
//
// 4. 提供 AnySpill() 方法
//	帮助	Join 算子判断哈希表是否有发生spill，如果否，可以直接执行原先的Join流程。
//
// 对内提供以下能力：
// 1. writeBatchToFileService : 生成唯一的文件名并落盘, 同时更新部分2。
// 2. removeBatchFromFileService : 根据文件名删除磁盘上的文件。
// 3. readBatchFromFileService : 根据文件名读取文件，并转换成 batch 的格式。
// 4. appendToBasicHashMap : 追加数据在哈希表的部分1。
// 5. buildHashMapByIndexes : 根据index list读取并生成哈希表, 这里需要尽量做优化，包括预分配，批处理等。

type HashPro struct {
	mp *mpool.MPool

	hasSpilledPart bool

	// 哈希表的基本信息，
	// 1. 帮助我们更快地构建哈希表。
	// 2. 帮助我们确定是要使用 int hash map 还是 str hash map.
	hashInformation struct {
	}

	// The Part 1.
	inMemory MemoryHashTable

	// The Part 2.
	inDisk SpilledHashTable
}

func (hp *HashPro) Probe(ctx context.Context) (probeSucceed []bool, readIO int64, err error) {
	return nil, 0, nil
}

func (hp *HashPro) PutBatch(
	ctx context.Context,
	data *batch.Batch) (writeIO int64, err error) {
	return 0, nil
}

func (hp *HashPro) Close() {
	// clean part 1.
	hp.inMemory.close(hp.mp)
	// clean part 2.
	hp.inDisk.close()
}

func (hp *HashPro) AnySpill() bool {
	return hp.hasSpilledPart
}
