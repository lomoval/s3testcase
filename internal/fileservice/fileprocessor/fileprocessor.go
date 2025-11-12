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
	"os"
	"path"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"s3testcase/internal/fileservice/filedatastore"
	"s3testcase/internal/storagelocator"
)

const (
	maxErrorsOnPart = 3
	copyAsyncSize   = 1024 * 1024 * 10
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
	uploadDir      string
	downloadDir    string
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
		uploadDir:      cfg.UploadDir,
		downloadDir:    cfg.DownloadDir,
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
//  1. File metadata and chunk locations are read from the File Datastore.
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

	if f.Size > copyAsyncSize {
		return fp.GetAsync(ctx, &f, file.Locations)
	}

	tasks := make(map[int]FileDownloadTask)
	resCh := make(chan FileTaskResult, len(file.Locations))
	done, cancel := context.WithCancel(ctx)
	defer cancel()
	for i, l := range file.Locations {
		s, err := fp.locator.StorageByUUID(l.LocationUUID)
		if err != nil {
			log.Err(err).Msgf("storage with '%s' not found", s.UUID.String())
			return nil, err
		}

		storageFileName := fmt.Sprintf("%s-%d", f.UUID.String(), l.PartNumber)
		localFileName := fmt.Sprintf("%s-%d-%s", f.UUID.String(), l.PartNumber, uuid.New().String())

		tasks[i] = FileDownloadTask{
			FilePath:    path.Join(fp.downloadDir, localFileName),
			Number:      l.PartNumber,
			ResultCh:    resCh,
			DownloadURL: fmt.Sprintf("http://%s/files/%s", s.Addr, storageFileName),
			Ctx:         done,
		}
		fp.workerPool.AddDownloadTask(tasks[i])
	}

	for i := 0; i < len(file.Locations); {
		select {
		case res, ok := <-resCh:
			if !ok {
				log.Error().Msgf("failed to get result from channel - chanel closed")
				return nil, errors.New("failed to get result from channel")
			}
			task := tasks[res.Number-1]
			if res.Err == nil {
				log.Debug().Msgf("part %d is ready - %s", res.Number, task.FilePath)
				i++
				f.setPart(res.Number-1, filePart{FilePath: task.FilePath})
			} else {
				task.ErrCount++
				if task.ErrCount > 3 {
					log.Err(ErrDownloadFailed).Msgf("too many errors with task %s-%s %d",
						file.UUID.String(),
						file.FileName,
						res.Number,
					)
					return nil, fmt.Errorf(
						"%s-%s %d: %w",
						file.UUID.String(),
						file.FileName,
						res.Number,
						ErrDownloadFailed,
					)
				}
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return &f, nil
}

func (fp *FileProcessor) GetAsync(
	ctx context.Context,
	f *File,
	locations []filedatastore.FileLocation,
) (*File, error) {
	log.Debug().Msgf("file %s-%s is large, use async copy", f.UUID.String(), f.Name)
	f.useAsycCopy = true
	go func() {
		defer close(f.readyPartCh)
		tasks := make(map[int]FileDownloadTask)
		resCh := make(chan FileTaskResult, len(locations))
		done, cancel := context.WithCancel(ctx)
		defer cancel()
		for i, l := range locations {
			s, err := fp.locator.StorageByUUID(l.LocationUUID)
			if err != nil {
				log.Err(err).Msgf("storage with '%s' not found", s.UUID.String())
				return
			}
			storageFileName := fmt.Sprintf("%s-%d", f.UUID.String(), l.PartNumber)
			localFileName := fmt.Sprintf("%s-%d-%s", f.UUID.String(), l.PartNumber, uuid.New().String())

			tasks[i] = FileDownloadTask{
				FilePath:    path.Join(fp.downloadDir, localFileName),
				Number:      l.PartNumber,
				ResultCh:    resCh,
				DownloadURL: fmt.Sprintf("http://%s/files/%s", s.Addr, storageFileName),
				Ctx:         done,
			}
			fp.workerPool.AddDownloadTask(tasks[i])
		}

		for i := 0; i < len(locations); {
			select {
			case res, ok := <-resCh:
				if !ok {
					log.Error().Msgf("failed to get result from channel - chanel closed")
					f.readyPartCh <- -1
					return
				}
				task := tasks[res.Number-1]
				if res.Err == nil {
					log.Debug().Msgf("part %d is ready - %s", res.Number, task.FilePath)
					i++
					f.setPart(res.Number-1, filePart{FilePath: task.FilePath})
					f.readyPartCh <- res.Number
				} else {
					log.Err(res.Err).Msgf(
						"error with task %s-%s %d",
						f.UUID.String(),
						f.Name,
						res.Number,
					)
					f.readyPartCh <- -1
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return f, nil
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
	return fp.processWithSize(ctx, file)
}

func (fp *FileProcessor) storagesIterator() roundIterator {
	storages := fp.locator.Storages()
	sort.Slice(storages, func(i, j int) bool {
		return storages[i].Size < storages[j].Size
	})
	return roundIterator{Storages: storages}
}

func (fp *FileProcessor) processWithSize(ctx context.Context, file *processingFile) error {
	log.Debug().Msgf("processing file %s-%s %s %d", file.UUID, file.Name, file.Type, file.Size)

	filePath := path.Join(fp.uploadDir, file.UUID.String())
	tmpFile, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("cannot create temp file: %w", err)
	}
	defer os.Remove(filePath)
	defer tmpFile.Close()

	buf := make([]byte, 32*1024)
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
	var written int64
	var partIndex int

	storagesIter := fp.storagesIterator()

	parts := make(map[int]*fileUploadPartData)
	resCh := make(chan FileTaskResult, partCount)
	taskDoneCtx, taskCancel := context.WithCancel(ctx)
	defer taskCancel()
	for {
		n, err := file.Reader.Read(buf)
		if n > 0 {
			_, werr := tmpFile.Write(buf[:n])
			if werr != nil {
				return werr
			}
			written += int64(n)

			for partIndex < partCount && written >= partsIntervals[partIndex].EndIndex {
				partNumber := partIndex + 1

				s := storagesIter.Next()
				id, err := fp.store.AddLocation(ctx, file.UUID, s.UUID, partNumber, partsIntervals[partIndex].Size())
				if err != nil {
					return err
				}

				part := fileUploadPartData{
					FileUUID:         file.UUID,
					Number:           partNumber,
					FilePath:         filePath,
					filePartInterval: partsIntervals[partIndex],
					Storage:          s,
					LocationID:       id,
					ResultCh:         resCh,
					Ctx:              taskDoneCtx,
				}
				parts[partNumber] = &part
				fp.workerPool.AddUploadTask(part.ToWorkerTask())
				partIndex++
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
	}

	if err := fp.waitProcessingParts(ctx, file, parts, storagesIter); err != nil {
		return err
	}
	log.Info().Msgf("file %s-%s processed", file.UUID.String(), file.Name)
	return nil
}

func (fp *FileProcessor) waitProcessingParts(
	ctx context.Context,
	file *processingFile,
	parts map[int]*fileUploadPartData,
	storagesIter roundIterator,
) error {
	// Processed time for file is max time of processed time of parts
	var processedTime time.Time
	for i := 0; i < len(parts); {
		select {
		case <-ctx.Done():
		case <-parts[i+1].Ctx.Done():
		case res := <-parts[i+1].ResultCh:
			if res.Err == nil {
				if processedTime.Before(res.ProcessedTime) {
					processedTime = res.ProcessedTime
				}
				if err := fp.store.SetLocationProcessedTime(ctx, parts[i+1].LocationID, res.ProcessedTime); err != nil {
					return err
				}
				i++
				continue
			}

			part := parts[res.Number]
			log.Err(res.Err).Msgf("part %s-%s %d failed", file.UUID.String(), file.Name, res.Number)
			if err := fp.store.DeleteLocation(ctx, parts[i+1].LocationID); err != nil {
				return err
			}

			part.ErrCount++
			if part.ErrCount > maxErrorsOnPart {
				log.Err(res.Err).Msgf(
					"uploading failed - too many errors with tasks %s-%s %d",
					file.UUID.String(),
					file.Name,
					res.Number,
				)
				return fmt.Errorf("%s-%s %d: %w", file.UUID.String(), file.Name, res.Number, ErrUploadFailed)
			}

			part.Storage = storagesIter.Next()
			log.Debug().Msgf("trying to upload %s-%s %d to other storage %s",
				file.UUID.String(),
				file.Name,
				res.Number,
				part.Storage.UUID.String())
			id, err := fp.store.AddLocation(ctx, file.UUID, part.Storage.UUID, i+1, part.Size())
			if err != nil {
				return err
			}
			part.LocationID = id
			fp.workerPool.AddUploadTask(part.ToWorkerTask())
		}
	}

	if !processedTime.IsZero() {
		if err := fp.store.SetFileProcessedTime(ctx, file.UUID, processedTime); err != nil {
			return err
		}
		if err := fp.store.SetCleanItemCleanAfterToNow(ctx, file.UUID); err != nil {
			log.Err(err).Msgf("failed to set clean outbox clean time")
		}
	} else {
		return fmt.Errorf("failed processing of file '%s'-'%s'", file.Name, file.UUID.String())
	}
	return nil
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
