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
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"s3testcase/internal/fileservice/filedatastore"
	"s3testcase/internal/storagelocator"
)

const (
	maxErrorsOnPart = 3
	cleanTimeout    = 5 * time.Minute
)

var (
	ErrUploadFailed      = errors.New("upload failed")
	ErrDownloadFailed    = errors.New("download failed")
	ErrFileNotFound      = errors.New("file not found")
	ErrIncorrectFileSize = errors.New("incorrect file size")
)

type roundIterator struct {
	Storages []storagelocator.StorageInfo
	i        int
}

func (r *roundIterator) Next() storagelocator.StorageInfo {
	item := r.Storages[r.i]
	r.i++
	if r.i >= len(r.Storages) {
		r.i = 0
	}
	return item
}

type FileProcessor struct {
	workerPool     *UploadWorkerPool
	filePartsCount int
	store          *filedatastore.Store
	locator        *storagelocator.Locator
}

func NewFileProcessor(
	cfg Config,
	store *filedatastore.Store,
	locator *storagelocator.Locator,
) *FileProcessor {
	wp := NewUploadWorkerPool(cfg.UploadWorkersCount, cfg.DownloadWorkersCount)
	return &FileProcessor{
		filePartsCount: cfg.FilePartsCount,
		workerPool:     wp,
		store:          store,
		locator:        locator,
	}
}

func (fp *FileProcessor) Run() {
	fp.workerPool.Run()
}

func (fp *FileProcessor) Shutdown(ctx context.Context) error {
	if err := fp.workerPool.Shutdown(ctx); err != nil {
		log.Err(err).Msg("failed to shutdown worker pool")
		return err
	}
	log.Info().Msg("shutdown complete")
	return nil
}

// Get retrieves a file from the storage.
// Download flow:
//  1. File metadata and parts locations are read from the File Datastore.
//  2. Worker tasks are created to fetch file chunks from the Storage Services.
//  3. The system waits for all workers to finish and assembles the file.
//  4. For large files, streaming is supported:
//     The response can begin sending as soon as the first chunk is received,
//     while the remaining chunks continue to load and stream sequentially.
func (fp *FileProcessor) Get(ctx context.Context, fileName string) (*File, error) {
	file, err := fp.store.GetLastFileWithLocationsByName(ctx, fileName)
	if err != nil {
		return nil, err
	}
	if file == nil {
		return nil, ErrFileNotFound
	}
	f := File{
		UUID:        file.UUID,
		Name:        file.FileName,
		Size:        file.Size,
		Type:        file.ContentType,
		Parts:       make([]filePart, len(file.Locations)),
		readyPartCh: make(chan int, len(file.Locations)),
	}

	if f.Size == 0 {
		log.Debug().Msgf("file %s-%s is empty, return without processing", f.UUID.String(), f.Name)
		return &f, nil
	}

	pr, pw := io.Pipe()
	f.Reader = pr
	go func() {
		defer pw.Close()
		for i, l := range file.Locations {
			select {
			case <-ctx.Done():
				if err := pw.CloseWithError(ctx.Err()); err != nil {
					log.Err(err).Msgf("failed to close pipe writer")
					return
				}
				return
			default:
			}

			s, err := fp.locator.StorageByUUID(l.LocationUUID)
			if err != nil {
				log.Err(err).Msgf("failed to get storage by uuid %s", l.LocationUUID)
				return
			}
			storageFileName := fmt.Sprintf("%s-%d", f.UUID.String(), l.PartNumber)

			url := fmt.Sprintf("http://%s/files/%s", s.Addr, storageFileName)
			if err := downloadFile(ctx, url, pw); err != nil {
				if err := pw.CloseWithError(fmt.Errorf("failed download file %d: %w", i+1, err)); err != nil {
					log.Err(err).Msgf("failed to close pipe writer")
					return
				}
			}
		}
	}()

	return &f, nil
}

// Save saves a file to the storages.
//
//	Each uploaded file is first registered in the File Datastore (the outbox table)
//	before any processing begins. This ensures that if an error occurs during handling,
//	the Cleaner component can roll back and remove any partial data or metadata.
//
// Upload flow:
//  1. When upload starts, a record is created in the File Datastore outbox table.
//     If an error occurs during processing, the Cleaner will delete the file data
//     from storage and remove all related database records.
//  2. A file record is created in the File Datastore with metadata.
//     At this stage, the file is not processed (no processed timestamp) and can not be downloaded.
//  3. Processing of file:
//     - The file is written to disk.
//     - As soon as the size of the local file exceeds a single chunk size,
//     worker tasks are created to upload those chunks to the Storage Service.
//     - Waits for all workers to complete
//     and validates that all chunks were successfully stored.
//  3. If some chunks fail an error is returned.
//  4. If all chunks are successfully processed, a processing timestamp is set,
//     and available for download.
func (fp *FileProcessor) Save(ctx context.Context, fd FileData) error {
	if fd.Size < 0 {
		return ErrIncorrectFileSize
	}

	file := &processingFile{
		FileData: fd,
		UUID:     uuid.New(),
	}

	if err := fp.store.InsertCleanItemData(ctx, file.UUID, time.Now().Add(cleanTimeout)); err != nil {
		log.Err(err).Msgf("failed to insert clean outbox data")
		return err
	}
	defer func() {
		if err := fp.store.SetCleanItemCleanAfterToNow(ctx, file.UUID); err != nil {
			log.Err(err).Msgf("failed to set clean time for item")
		}
	}()

	_, err := fp.store.InsertFileInfo(ctx, file.UUID, file.Name, file.Type, file.Size)
	if err != nil {
		return err
	}
	return fp.save(ctx, file)
}

func (fp *FileProcessor) save(ctx context.Context, file *processingFile) error {
	log.Debug().Msgf("processing file %s-%s %s %d", file.UUID, file.Name, file.Type, file.Size)

	partsIntervals := partsFileIntervals(file.Size, fp.filePartsCount)
	partCount := len(partsIntervals)
	log.Debug().Msgf(
		"file %s-%s - size %d, type %s, parts count %d, parts intervals %v",
		file.UUID.String(),
		file.Name,
		file.Size,
		file.Type,
		partCount,
		partsIntervals,
	)
	storagesIter := fp.storagesIterator()
	var lastTime time.Time
	for i, pi := range partsIntervals {
		partNumber := i + 1
		s := storagesIter.Next()
		id, err := fp.store.AddLocation(ctx, file.UUID, s.UUID, partNumber, pi.Size())
		if err != nil {
			return err
		}

		log.Printf("sending part %d (%d bytes) to  %s - %s", i+1, pi.Size(), s.UUID, s.Addr)
		limited := io.LimitReader(file.Reader, pi.Size())
		req, err := http.NewRequestWithContext(
			ctx,
			"PUT",
			fmt.Sprintf("http://%s/files/%s-%d", s.Addr, file.UUID.String(), partNumber),
			limited,
		)
		if err != nil {
			return err
		}
		req.ContentLength = pi.Size()
		req.Header.Set("Content-Type", "application/octet-stream")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			log.Error().Msgf("failed to upload part %d: %s", partNumber, resp.Status)
			return fmt.Errorf("failed to upload part %d: %s", partNumber, resp.Status)
		}
		lastTime = time.Now()
		if err := fp.store.SetLocationProcessedTime(ctx, id, lastTime); err != nil {
			return err
		}
	}

	if err := fp.store.SetFileProcessedTime(ctx, file.UUID, lastTime); err != nil {
		log.Err(err).Msgf("failed to set file processed time")
		return err
	}

	log.Debug().Msgf("file %s-%s processed", file.UUID.String(), file.Name)
	return nil
}

func (fp *FileProcessor) storagesIterator() roundIterator {
	storages := fp.locator.Storages()
	sort.Slice(storages, func(i, j int) bool {
		return storages[i].Size < storages[j].Size
	})
	return roundIterator{Storages: storages}
}

// partsFileIntervals splits the range [0, size) into approximately equal parts.
// The first parts are longer, the last part is less than or equal to the previous ones.
// If size <= partsCount, it returns 'size' parts of length 1 each.
func partsFileIntervals(size int64, partsCount int) []filePartInterval {
	if size <= 0 || partsCount <= 0 {
		return nil
	}

	// If size <= partsCount, return 'size' parts of length 1
	if size <= int64(partsCount) {
		parts := make([]filePartInterval, size)
		for i := int64(0); i < size; i++ {
			parts[i] = filePartInterval{StartIndex: i, EndIndex: i + 1}
		}
		return parts
	}

	parts := make([]filePartInterval, 0, partsCount)
	baseSize := size / int64(partsCount)
	remainder := size % int64(partsCount)

	var start int64
	for i := 0; i < partsCount; i++ {
		partLen := baseSize
		if remainder > 0 {
			partLen++
			remainder--
		}
		parts = append(parts, filePartInterval{StartIndex: start, EndIndex: start + partLen})
		start += partLen
	}

	return parts
}

func downloadFile(ctx context.Context, url string, pw *io.PipeWriter) error {
	client := &http.Client{}
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status %d for part %s", resp.StatusCode, url)
	}
	if _, err := io.Copy(pw, resp.Body); err != nil {
		return err
	}
	return nil
}
