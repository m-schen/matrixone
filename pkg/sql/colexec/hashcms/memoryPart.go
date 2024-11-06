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

import "github.com/matrixorigin/matrixone/pkg/common/mpool"

// 该文件放置 内存部分的哈希表 的相关代码。
//
// 由于落盘文件读取上来后，也是一个在内存中的哈希表。对于从这类哈希表中 probe 的代码尽量不要依赖于 MemoryHashTable 结构体。

// MemoryHashTable is the first part of HashPro, it is the basic hashtable always save in the memory.
type MemoryHashTable struct {
}

func (mh *MemoryHashTable) close(mp *mpool.MPool) {
}
