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
	ulog "s3testcase/internal/utils/log"
)

const (
	cleanTimeout = 5 * time.Minute
)

var (
	ErrDownloadFailed    = errors.New("download failed")
	ErrFileNotFound      = errors.New("file not found")
	ErrIncorrectFileSize = errors.New("incorrect file size")
)

type FileProcessor struct {
	filePartsCount int
	usePrefetch    bool
	store          *filedatastore.Store
	locator        *storagelocator.Locator
	httpClient     *http.Client
}

type roundIterator struct {
	Storages []storagelocator.StorageInfo
	i        int
}

func NewFileProcessor(
	cfg Config,
	store *filedatastore.Store,
	locator *storagelocator.Locator,
) *FileProcessor {
	return &FileProcessor{
		filePartsCount: cfg.FilePartsCount,
		usePrefetch:    cfg.UsePrefetch,
		store:          store,
		locator:        locator,
		httpClient:     &http.Client{},
	}
}

func (fp *FileProcessor) Run() {
}

func (fp *FileProcessor) Shutdown(_ context.Context) error {
	log.Info().Msg("shutdown complete")
	return nil
}

// Get retrieves a file from the storage.
func (fp *FileProcessor) Get(ctx context.Context, fileName string) (*File, error) {
	file, err := fp.store.GetLastFileWithLocationsByName(ctx, fileName)
	if err != nil {
		return nil, err
	}
	if file == nil {
		return nil, ErrFileNotFound
	}
	f := File{
		UUID: file.UUID,
		FileData: FileData{
			Name:   file.FileName,
			Size:   file.Size,
			Type:   file.ContentType,
			Reader: nil,
		},
	}

	if f.Size == 0 {
		log.Debug().Msgf("file %s-%s is empty, return without processing", f.UUID.String(), f.Name)
		return &f, nil
	}

	switch fp.usePrefetch {
	case true:
		// It's for own testing purpose (cn be ignored) - to check comparison with sequentially downloading.
		// On same local machine it not useful: sequentially works faster for most files;
		// with big files in most cases speed is the same.
		fp.getPrefetched(ctx, file, &f)
	default:
		fp.getSequentially(ctx, file, &f)
	}
	return &f, nil
}

// Save saves a file to the storages.
//
//	Each uploaded file is first registered in the File Datastore (the outbox table)
//	before any processing begins. This ensures that if an error occurs during handling,
//	the Cleaner component can roll back and remove any partial data or metadata (from db or storages).
//
// Upload flow:
//  1. When upload starts, a record is created in the File Datastore outbox table.
//     If an error occurs during processing, the Cleaner will delete the file data
//     from storage and remove all related database records.
//  2. A file record is created in the File Datastore with metadata.
//     At this stage, the file is not processed (no processed timestamp) and can not be downloaded by clients.
//  3. Processing of file:
//     for file uploads, intervals corresponding to part sizes are used.
//     A connection to the storage is opened, and data begins streaming there according to the part size.
//     Then, the next connection is opened to handle the following part, and so on, until all the data has
//     been processed.
//  4. After all parts are processed, the file is marked as processed (has a processed timestamp in the DB).
//     At this point, the file can be downloaded by clients.
func (fp *FileProcessor) Save(ctx context.Context, fd FileData) error {
	if fd.Size < 0 {
		return ErrIncorrectFileSize
	}

	file := &File{
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

	if file.Size == 0 {
		if err := fp.store.SetFileProcessedTime(ctx, file.UUID, time.Now()); err != nil {
			log.Err(err).Msgf("failed to set file processed time")
			return err
		}
		return nil
	}

	return fp.save(ctx, file)
}

// Get retrieves a file from the storage.
// It sequentially downloads each part of the file and writes it by pipe, data can be read by file reader.
func (fp *FileProcessor) getSequentially(ctx context.Context, dsFile *filedatastore.File, file *File) {
	pr, pw := io.Pipe()
	file.Reader = pr
	go func() {
		defer pw.Close()
		for i, l := range dsFile.Locations {
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

			url := s.URL + "/files/" + storageFilename(file, l.PartNumber)
			log.Debug().Msgf("download file '%s', part %d from %s", file.Name, i+1, url)
			if err := fp.readHTTPOKResponse(ulog.ContextWithHTTPTracer(ctx, "download-file"), url, pw); err != nil {
				if err := pw.CloseWithError(fmt.Errorf("failed download file %d: %w", i+1, err)); err != nil {
					log.Err(err).Msgf("failed to close pipe writer")
					return
				}
			}
		}
	}()
}

func (fp *FileProcessor) save(ctx context.Context, file *File) error {
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
			ulog.ContextWithHTTPTracer(ctx, "upload-file"),
			"PUT",
			s.URL+"/files/"+storageFilename(file, partNumber),
			limited,
		)
		if err != nil {
			return err
		}
		req.ContentLength = pi.Size()
		req.Header.Set("Content-Type", "application/octet-stream")
		resp, err := fp.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body) // to ignore body and place connection to pool.

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

func (fp *FileProcessor) readHTTPOKResponse(ctx context.Context, url string, pw io.Writer) error {
	resp, err := fp.getHTTPOKResponse(ctx, url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if _, err := io.Copy(pw, resp.Body); err != nil {
		return err
	}
	return nil
}

func (fp *FileProcessor) storagesIterator() roundIterator {
	storages := fp.locator.Storages()
	sort.Slice(storages, func(i, j int) bool {
		return storages[i].Size < storages[j].Size
	})
	return roundIterator{Storages: storages}
}

func (r *roundIterator) Next() storagelocator.StorageInfo {
	item := r.Storages[r.i]
	r.i++
	if r.i >= len(r.Storages) {
		r.i = 0
	}
	return item
}

func storageFilename(f *File, partNumber int) string {
	return fmt.Sprintf("%s-%d", f.UUID.String(), partNumber)
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
