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

package filestorage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"s3testcase/internal/storage"
	"s3testcase/internal/utils/sys"
)

const (
	tmpDirName  = "tmp"
	dataDirName = "data"
)

var ErrFileNotFound = errors.New("file does not exist")

type Config struct {
	StorageDir      string        `yaml:"storageDir"`
	TmpDir          string        `yaml:"tempDir"`
	DataDir         string        `yaml:"dataDir"`
	CleanupInterval time.Duration `yaml:"cleanupInterval"`
	CleanupAge      time.Duration `yaml:"cleanupAge"`
}

func LoadConfig() Config {
	dir := usys.GetEnv("STORAGE_DIR", path.Join(os.TempDir(), "storage"))
	return Config{
		StorageDir:      dir,
		TmpDir:          usys.GetEnv("STORAGE_TMP_DIR", path.Join(dir, tmpDirName)),
		DataDir:         usys.GetEnv("STORAGE_DATA_DIR", path.Join(dir, dataDirName)),
		CleanupInterval: 5 * time.Minute,
		CleanupAge:      15 * time.Minute,
	}
}

type Storage struct {
	storageDir      string
	tmpDir          string
	dataDir         string
	maxAge          time.Duration
	cleanupInterval time.Duration
	stopCh          chan struct{}
	wg              sync.WaitGroup
	size            int64
}

func NewStorage(cfg Config) *Storage {
	return &Storage{
		storageDir:      cfg.StorageDir,
		tmpDir:          path.Join(cfg.StorageDir, tmpDirName),
		dataDir:         path.Join(cfg.StorageDir, dataDirName),
		maxAge:          cfg.CleanupAge,
		cleanupInterval: cfg.CleanupInterval,
		wg:              sync.WaitGroup{},
	}
}

func (s *Storage) Run() error {
	err := os.MkdirAll(s.dataDir, 0o755)
	if err != nil {
		return err
	}

	err = os.MkdirAll(s.tmpDir, 0o755)
	if err != nil {
		return err
	}

	s.size, err = getDirSize(context.Background(), s.dataDir, runtime.NumCPU())
	if err != nil {
		return err
	}

	s.stopCh = make(chan struct{})
	if s.cleanupInterval > 0 {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if s.cleanupTempFiles(s.maxAge) != nil {
				log.Err(err).Msg("cleanup failed")
			}

			t := time.NewTicker(s.cleanupInterval)
			defer t.Stop()
			for {
				select {
				case <-t.C:
					if s.cleanupTempFiles(s.maxAge) != nil {
						log.Err(err).Msg("cleanup failed")
					}
				case <-s.stopCh:
					log.Debug().Msgf("cleanup stopped")
					return
				}
			}
		}()
	}
	return nil
}

func (s *Storage) Shutdown(ctx context.Context) error {
	close(s.stopCh)
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		log.Info().Msg("shutdown completed")
	case <-ctx.Done():
		log.Err(ctx.Err()).Msg("shutdown failed")
		return ctx.Err()
	}
	return nil
}

func (s *Storage) Save(ctx context.Context, key string, r io.Reader) error {
	tmpPath := filepath.Join(s.tmpDir, key)
	dataPath := filepath.Join(s.dataDir, key)

	size, err := s.saveReader(ctx, tmpPath, r)
	if err != nil {
		return err
	}
	defer func() {
		if err := removeFileIgnoreNotExist(tmpPath); err != nil {
			log.Err(err).Msgf("failed to remove tmp file '%s'", tmpPath)
		}
	}()

	existSize, err := fileSizeIfExist(dataPath)
	if err != nil {
		return err
	}

	if err := os.Rename(tmpPath, dataPath); err != nil {
		return err
	}
	s.addSize(size - existSize)
	return nil
}

func (s *Storage) Get(key string) (storage.FileData, error) {
	dataPath := filepath.Join(s.dataDir, key)
	info, err := os.Stat(dataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return storage.FileData{}, ErrFileNotFound
		}
		return storage.FileData{}, fmt.Errorf("failed to stat file: %w", err)
	}
	if info.IsDir() {
		return storage.FileData{}, fmt.Errorf("path is a directory, not a file")
	}
	f, err := os.Open(dataPath)
	if err != nil {
		return storage.FileData{}, err
	}
	return storage.FileData{
		Reader: f,
		Size:   info.Size(),
	}, nil
}

func (s *Storage) Delete(key string) error {
	fp := filepath.Join(s.dataDir, key)
	info, err := os.Stat(fp)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	err = os.Remove(fp)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	s.addSize(-info.Size())
	return nil
}

func (s *Storage) Size() int64 {
	return atomic.LoadInt64(&s.size)
}

func (s *Storage) addSize(size int64) {
	atomic.AddInt64(&s.size, size)
}

func (s *Storage) saveReader(ctx context.Context, path string, r io.Reader) (size int64, err error) {
	out, err := os.Create(path)
	if err != nil {
		return -1, err
	}

	defer func() {
		if closeErr := out.Close(); closeErr != nil {
			log.Err(closeErr).Msgf("failed to close file '%s'", path)
			if err == nil {
				err = closeErr
			}
		}
		if err != nil {
			if remErr := removeFileIgnoreNotExist(path); remErr != nil {
				log.Err(remErr).Msgf("failed to remove file '%s'", path)
			}
		}
	}()

	return writeWithContext(ctx, r, out, size)
}

func writeWithContext(ctx context.Context, r io.Reader, out *os.File, size int64) (int64, error) {
	buf := make([]byte, 32*1024)
	for {
		select {
		case <-ctx.Done():
			return -1, ctx.Err()
		default:
			nr, readErr := r.Read(buf)
			if nr > 0 {
				nw, writeErr := out.Write(buf[:nr])
				if writeErr != nil {
					return -1, writeErr
				}
				if nw != nr {
					return -1, io.ErrShortWrite
				}
				size += int64(nw)
			}
			if readErr != nil {
				if readErr == io.EOF {
					return size, nil
				}
				return -1, readErr
			}
		}
	}
}

func (s *Storage) cleanupTempFiles(maxAge time.Duration) error {
	return filepath.Walk(s.tmpDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if time.Since(info.ModTime()) > maxAge {
			if err := os.Remove(path); err != nil {
				return err
			}
		}
		return nil
	})
}

func getDirSize(ctx context.Context, root string, workers int) (int64, error) {
	var total int64
	paths := make(chan string, 256)

	wg := startSizeWorkers(ctx, paths, &total, workers)
	walkErr := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case paths <- path:
			return nil
		}
	})

	close(paths)
	wg.Wait()

	if walkErr != nil {
		return -1, walkErr
	}

	return total, nil
}

func startSizeWorkers(ctx context.Context, paths <-chan string, total *int64, workers int) *sync.WaitGroup {
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case p, ok := <-paths:
					if !ok {
						return
					}

					info, err := os.Lstat(p)
					if err != nil {
						log.Err(err).Msgf("failed to get file info for '%s'", p)
						continue
					}
					if !info.IsDir() {
						atomic.AddInt64(total, info.Size())
					}
				}
			}
		}()
	}

	return &wg
}

func removeFileIgnoreNotExist(filePath string) error {
	err := os.Remove(filePath)
	if errors.Is(err, os.ErrNotExist) { // проверка на отсутствие файла
		return nil
	}
	return err
}

func fileSizeIfExist(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return -1, err
	}
	return info.Size(), nil
}
