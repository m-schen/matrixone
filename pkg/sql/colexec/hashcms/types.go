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
    "fmt"
    "github.com/matrixorigin/matrixone/pkg/container/batch"
    "sync"
    "time"
)

// 哈希表 应该包含以下2部分
// 1. 长期处于内存中的一个哈希表，这对于刚好超过 max memory limit的哈希表有很大的作用。
// 2. 外溢到磁盘上的落盘数据，它们可以随时被读取上来构建一个额外的哈希表。需要注意的是，这部分数据需要在查询结束时及时删除。
//
// 对外提供以下能力：
// 1. 提供 probe() 能力，优先探测部分1，不存在的数据再探测数据2. 需要尽量做优化，对一次探测来说，需要减少IO次数。
// 2. 提供 putBatch() 能力，在内存足够的情况下更新部分1，在内存不够的情况下开始spill. spill的时候根据一个基础的key值进行分块，暂时定为spill为16个部分。
// 3. 提供 close() 能力，对哈希表所有数据进行清除。
//
// 对内提供以下能力：
// 1. WriteBatchToFileService : 生成唯一的文件名并落盘, 同时更新部分2。
// 2. RemoveBatchFromFileService : 根据文件名删除磁盘上的文件。
// 3. ReadBatchFromFileService : 根据文件名读取文件，并转换成 batch 的格式。
// 4. AppendToBasicHashMap : 追加数据在哈希表的部分1。
// 5. BuildHashMapByIndexes : 根据index list读取并生成哈希表, 这里需要尽量做优化，包括预分配，批处理等。

type HashPro struct {
    // The Part 1.
    inMemory MemoryHashTable

    // The Part 2.
    inDisk SpilledHashTable
}

// MemoryHashTable is the first part of HashPro, it is the basic hashtable always save in the memory.
type MemoryHashTable struct {

}

// SpilledHashTable is the second part of HashPro, it records spilled block information,
// and support the ability to rebuild hashtable.
// but should be careful, there will be more than 1 user using this part.
// so we should set a max-using-memory limitation for rebuild hashtable at a same time.
type SpilledHashTable struct {
    // fileSrv is the file service used to write / read / delete.
    fileSrv ReadWriteImplementer

    // each the probe phase, the consumer of this hash table, should do lock once it want to use the hash table.
    //
    // use hash table to probe :
    //  1. get the lock.
    //  2. check if wanted hash table was `InMemory`, and increase the reference. if it was not built, try to build it. (once memory is not enough, unlock and back to step 1.)
    //  3. decrease the reference, if reference == 0, reset the memory and flag this hash table to `InDisk`.
    probeLock sync.Mutex

    memLimitation int64
    memSizeBeUsed int64

    // unique path item.
    // format: sht_pointerPatch_Time_blockIndex.
    formatTemplate string

    // origin data.
    blocks []spilledBatchInfo
}

// spilledBatchInfo is the information about spilled block.
type spilledBatchInfo struct {
    filePath string

    // size and rowCount records the information about spilled file.
    // they're helpful for us to build the hashmap.
    size     int64
    rowCount int
}

func (sh *SpilledHashTable) initFormatTemplate() {
    sh.formatTemplate = fmt.Sprintf("sht_%p_%s", sh, time.Now().String()) + "_%d"
}

// generateUniqueSpillPath generate an unique spill path.
func (sh *SpilledHashTable) generateUniqueSpillPath() string {
    return fmt.Sprintf(sh.formatTemplate, len(sh.blocks))
}

func (sh *SpilledHashTable) AppendSpilledBlock(data *batch.Batch) error {
    filePath := sh.generateUniqueSpillPath()
    entry, err := getIOEntryToWriteBatch(data)
    if err != nil {
        return err
    }
}
