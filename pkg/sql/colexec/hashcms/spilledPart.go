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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"sync"
	"time"
)

// 该文件置放 落盘部分的block 的相关代码。

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

func (sh *SpilledHashTable) writeBatchToFileService(ctx context.Context, data *batch.Batch) (int64, error) {
	// step 1： generate the IO data for writing.
	filePath := sh.generateUniqueSpillPath()
	entry, err := getIOEntryToWriteBatch(data)
	if err != nil {
		return 0, err
	}
	IOdata := fileservice.IOVector{
		FilePath: filePath,
		Entries:  []fileservice.IOEntry{entry},
	}
	writeSize := int64(len(IOdata.Entries[0].Data))

	// step 2: write data to file service.
	if err = sh.fileSrv.Write(ctx, IOdata); err != nil {
		return 0, err
	}

	// step 3: update the block list.
	info := spilledBatchInfo{
		filePath: filePath,
		size:     int64(data.Allocated()),
		rowCount: data.RowCount(),
	}
	sh.blocks = append(sh.blocks, info)
	return writeSize, nil
}

func (sh *SpilledHashTable) removeBatchFromFileService(filePath string) {
	// file path will be always correct.
	_ = sh.fileSrv.Delete(context.TODO(), filePath)
}

func (sh *SpilledHashTable) readBatchFromFileService(ctx context.Context, mp *mpool.MPool, filePath string) (*batch.Batch, int64, error) {
	// step 1: generate the IO vector for storing data.
	entry := getIOEntryToReadBatch()
	IOdata := fileservice.IOVector{
		FilePath: filePath,
		Entries:  []fileservice.IOEntry{entry},
	}

	// step 2: read data from file service.
	if err := sh.fileSrv.Read(ctx, &IOdata); err != nil {
		return nil, 0, err
	}

	// step 3: unmarshal.
	bat := batch.NewOffHeapEmpty()
	readSize := int64(len(IOdata.Entries[0].Data))
	if err := bat.UnmarshalBinaryWithAnyMp(IOdata.Entries[0].Data, mp); err != nil {
		return nil, readSize, err
	}
	return bat, readSize, nil
}

func (sh *SpilledHashTable) close() {
	for _, info := range sh.blocks {
		sh.removeBatchFromFileService(info.filePath)
	}
}
