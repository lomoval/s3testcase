// MIT License
//
// Copyright (c) 2025 Aleksandr A. Lomov
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software
// and associated documentation files (the “Software”), to deal in the Software without
// restriction, including without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or
// substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
// THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
// OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
// ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.

package fileprocessor

import (
	"context"
	"io"
	"time"

	"github.com/google/uuid"
	"s3testcase/internal/storagelocator"
)

type FileData struct {
	Name   string
	Size   int64
	Type   string
	Reader io.Reader
}

type File struct {
	UUID   uuid.UUID
	Name   string
	Size   int64
	Reader io.Reader
	Type   string
	Parts  []filePart

	readyPartCh chan int // For notification if we have a new part
}

func (f *File) Copy(w io.Writer) error {
	if f.Size == 0 {
		return nil
	}
	_, err := io.Copy(w, f.Reader)
	return err
}

func (f *File) Close() {}

type processingFile struct {
	FileData
	UUID  uuid.UUID
	Parts []filePart
}

type filePartInterval struct {
	StartIndex int64
	EndIndex   int64
}

func (p filePartInterval) Size() int64 {
	return p.EndIndex - p.StartIndex
}

type filePart struct {
	ProcessedTime time.Time
	FilePath      string
	Error         error
}

type fileUploadPartData struct {
	filePartInterval
	// Number of file part (1..N).
	FileUUID   uuid.UUID
	Number     int
	FilePath   string
	LocationID int64
	ResultCh   chan FileTaskResult
	Ctx        context.Context
	ErrCount   int
	Storage    storagelocator.StorageInfo
}
