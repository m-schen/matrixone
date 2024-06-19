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

package mpool

import (
	"fmt"
	"runtime/debug"
)

func (mp *MPool) Details() string {
	d := mp.details

	d.mu.Lock()
	defer d.mu.Unlock()

	str := "detail is:"
	cnt := 1
	for _, v := range mp.details.aliveMemory {
		str += fmt.Sprintf("\n %d. %s", cnt, v)
		cnt++
	}
	return str
}

func (d *mpoolDetails) recordMemoryAllocate(header *memHdr) {
	stackPath := debug.Stack()
	str := string(stackPath)

	d.mu.Lock()
	defer d.mu.Unlock()

	d.aliveMemory[header] = str
}

func (d *mpoolDetails) recordMemoryFree(header *memHdr) {
	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.aliveMemory, header)
}
