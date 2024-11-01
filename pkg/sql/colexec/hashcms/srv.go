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
    "github.com/matrixorigin/matrixone/pkg/container/batch"
    "github.com/matrixorigin/matrixone/pkg/fileservice"
)

// 暂时用来放与file service交互的代码.

// ReadWriteImplementer is a subset of fileservice.FileService,
// providing Read, Write, and Delete method externally.
// The reason we did not use fileservice.FileService directly is that it facilitates easier testing.
type ReadWriteImplementer interface {
    Write(ctx context.Context, vector fileservice.IOVector) error
    Read(ctx context.Context, vector *fileservice.IOVector) error
    Delete(ctx context.Context, filePaths ...string) error
}

// getIOEntryToWriteBatch
// 1. encode the batch.
// 2. generate an io entry to write all the whole data.
func getIOEntryToWriteBatch(b *batch.Batch) (fileservice.IOEntry, error) {
    bs, err := b.MarshalBinary()

    return fileservice.IOEntry{
        Offset: 0,
        Size:   -1,
        Data:   bs,
    }, err
}

// getIOEntryToReadBatch return an IOEntry to get all the data.
func getIOEntryToReadBatch() fileservice.IOEntry {
    return fileservice.IOEntry{
        Offset: 0,
        Size:   -1,
    }
}