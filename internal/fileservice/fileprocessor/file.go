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
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

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
	mutex       sync.Mutex
	useAsycCopy bool
}

func (f *File) Copy(ctx context.Context, w io.Writer) error {
	if f.Size == 0 {
		return nil
	}
	if f.useAsycCopy {
		return f.copyAsync(ctx, w)
	}
	return f.copy(ctx, w)
}

func (f *File) Close() {
	for _, part := range f.Parts {
		if part.FilePath != "" {
			os.Remove(part.FilePath)
		}
	}
	f.Parts = nil
}

func (f *File) getPart(index int) filePart {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return f.Parts[index]
}

func (f *File) setPart(index int, p filePart) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.Parts[index] = p
}

func (f *File) copyAsync(ctx context.Context, w io.Writer) error {
	partsCount := cap(f.Parts)
	for i := 0; i < partsCount; {
		part := f.getPart(i)
		if part.FilePath == "" {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case num, ok := <-f.readyPartCh:
				switch {
				case !ok:
					if len(f.Parts) == cap(f.Parts) {
						log.Debug().Msgf("all parts are process for file %s-%s", f.UUID.String(), f.Name)
					} else {
						log.Debug().Msgf("loading of parts faileds for file %s-%s", f.UUID.String(), f.Name)
					}
				case num == -1:
					log.Debug().Msgf("part failed for file, stop copy %s-%s", f.UUID.String(), f.Name)
					return fmt.Errorf("part failed, file cannot be combined")
				case num-1 == i:
					// If we have a new part that should be process then process it, otherwise waiting.
					part = f.getPart(i)
				default:
					continue
				}
			}
		}
		log.Debug().Msgf("process part %d %s", i+1, part.FilePath)
		i++
		f, err := os.Open(part.FilePath)
		if err != nil {
			return err
		}
		cr := &ctxReader{ctx: ctx, r: f}
		written, err := io.Copy(w, cr)
		if err != nil {
			f.Close()
			return err
		}
		log.Debug().Msgf("written %d bytes - %s", written, part.FilePath)
		f.Close()
	}
	return nil
}

func (f *File) copy(ctx context.Context, w io.Writer) error {
	for _, part := range f.Parts {
		f, err := os.Open(part.FilePath)
		if err != nil {
			return err
		}
		cr := &ctxReader{ctx: ctx, r: f}
		written, err := io.Copy(w, cr)
		if err != nil {
			f.Close()
			return err
		}
		log.Info().Msgf("written %d bytes - %s", written, part.FilePath)
		f.Close()
	}
	return nil
}

type processingFile struct {
	FileData
	UUID  uuid.UUID
	Parts []filePart
}

type filePart struct {
	ProcessedTime time.Time
	FilePath      string
	Error         error
}

type fileUploadPartData struct {
	// Number of file part (1..N).
	FileUUID   uuid.UUID
	Number     int
	FilePath   string
	StartIndex int64
	EndIndex   int64
	Size       int64
	LocationID int64
	ResultCh   chan FileTaskResult
	Ctx        context.Context
	ErrCount   int
	Storage    storagelocator.StorageInfo
}

func (p *fileUploadPartData) ToWorkerTask() FileUploadTask {
	return FileUploadTask{
		Number:     p.Number,
		FilePath:   p.FilePath,
		StartIndex: p.StartIndex,
		EndIndex:   p.EndIndex,
		UploadURL:  fmt.Sprintf("http://%s/files/%s-%d", p.Storage.Addr, p.FileUUID.String(), p.Number),
		ResultCh:   p.ResultCh,
		Ctx:        p.Ctx,
		ErrCount:   p.ErrCount,
	}
}
