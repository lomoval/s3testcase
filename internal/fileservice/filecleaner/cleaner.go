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

package filecleaner

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"path/filepath"
	repository "s3testcase/internal/fileservice/filedatastore"
	"s3testcase/internal/storagelocator"
)

type Cleaner struct {
	UploadDir   string
	DownloadDir string
	MaxAge      time.Duration
	store       *repository.Store
	locator     *storagelocator.Locator
	ctx         context.Context
	cancel      context.CancelFunc
	workers     sync.WaitGroup
}

func NewCleaner(
	cfg Config,
	store *repository.Store,
	locator *storagelocator.Locator,
) *Cleaner {
	ctx, cancel := context.WithCancel(context.Background())
	return &Cleaner{
		UploadDir:   cfg.UploadDir,
		DownloadDir: cfg.DownloadDir,
		MaxAge:      cfg.FilesMaxAge,
		store:       store,
		locator:     locator,
		ctx:         ctx,
		cancel:      cancel,
	}
}

func (c *Cleaner) Run() {
	c.workers.Add(1)
	go func() {
		defer c.workers.Done()
		select {
		case <-c.ctx.Done():
			return
		case <-time.After(time.Minute):
			c.cleanupDirs()
		}
	}()
	c.workers.Add(1)
	go func() {
		defer c.workers.Done()
		for {
			_, inBatch, err := c.cleanOutbox()
			if err != nil || inBatch == 0 {
				select {
				case <-c.ctx.Done():
					return
				case <-time.After(10 * time.Second):
				}
			}
		}
	}()
}

func (c *Cleaner) Shutdown(ctx context.Context) error {
	c.cancel()

	done := make(chan struct{})
	go func() {
		c.workers.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Info().Msg("shutdown completed")
		return nil
	case <-ctx.Done():
		log.Err(ctx.Err()).Msg("shutdown failed")
		return ctx.Err()
	}
}

func (c *Cleaner) stopped() bool {
	select {
	case <-c.ctx.Done():
		return true
	default:
		return false
	}
}

func (c *Cleaner) cleanupDirs() {
	for _, dir := range []string{c.UploadDir, c.DownloadDir} {
		if c.stopped() {
			return
		}
		_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if c.stopped() {
				return nil
			}
			if err != nil {
				if os.IsNotExist(err) {
					return nil
				}
				log.Error().Err(err).Msgf("error on cleaning %s", dir)
				return nil
			}
			if info.IsDir() {
				return nil
			}
			if time.Since(info.ModTime()) > c.MaxAge {
				log.Info().Msgf("deleting %s", path)
				if err := os.Remove(path); err != nil {
					log.Error().Err(err).Msgf("error on deleting %s", path)
				}
			}
			return nil
		})
	}
}

func (c *Cleaner) cleanOutbox() (int, int, error) {
	ctx := c.ctx
	items, err := c.store.GetCleaningItemsBatch(ctx, 100)
	if err != nil {
		log.Err(err).Msg("error on getting cleaning items")
		return 0, 0, err
	}
	count := 0
	for _, item := range items {
		if c.stopped() {
			return count, len(items), err
		}
		log.Debug().Msgf("cleaning Id %d, uuid %s", item.ID, item.FileUUID.String())

		if err := c.cleanLocations(ctx, item); err != nil {
			log.Err(err).Msgf("failed to clean locations for file %s", item.FileUUID.String())
			return count, len(items), err
		}

		// If file processed and active than we need to add for cleaning all older files with the same name.
		if item.IsActive() {
			if err := c.store.AddOldFilesToCleanItems(ctx, item.FileUUID); err != nil {
				log.Err(err).Msgf("failed to add old files to clean outbox for file %s", item.FileUUID.String())
				return count, len(items), err
			}
			if err := c.store.DeleteCleanItem(ctx, item.FileUUID); err != nil {
				log.Err(err).Msgf("failed to delete clean data for %s", item.FileUUID.String())
				return count, len(items), err
			}
		} else {
			if err := c.store.DeleteFileWithCleanItem(ctx, item.FileUUID); err != nil {
				log.Err(err).Msgf("failed to delete file %s", item.FileUUID.String())
			}
		}
		log.Debug().Msgf("cleaning done for Id %d, uuid %s", item.ID, item.FileUUID.String())
		count++
	}
	return count, len(items), nil
}

// Clean locations for file. For processed file only unprocessed locations will be cleaned.
func (c *Cleaner) cleanLocations(ctx context.Context, item repository.CleanItem) error {
	locations, err := c.store.GetFileLocations(ctx, item.FileUUID, item.IsActive())
	if err != nil {
		return err
	}
	for _, location := range locations {
		err := c.deleteFromLocation(
			ctx,
			location.LocationUUID,
			item.FileUUID.String()+"-"+strconv.Itoa(location.PartNumber),
		)
		if err != nil {
			return err
		}
		if err := c.store.DeleteFileLocationByID(ctx, location.ID); err != nil {
			return err
		}
	}
	return nil
}

func (c *Cleaner) deleteFromLocation(ctx context.Context, locationUUID uuid.UUID, fileName string) error {
	s, err := c.locator.StorageByUUID(locationUUID)
	if err != nil {
		log.Err(err).Msgf("failed to get storage for location %s", locationUUID.String())
		return err
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodDelete,
		"http://"+s.Addr+"/files/"+fileName,
		nil,
	)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Err(err).Msgf("failed to delete file %s from location %s", fileName, locationUUID.String())
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to delete file %s from location %s", fileName, locationUUID.String())
	}
	return nil
}
