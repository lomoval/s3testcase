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

package fileservice

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"
	"s3testcase/internal/fileservice/db"
	"s3testcase/internal/fileservice/filecleaner"
	"s3testcase/internal/fileservice/filedatastore"
	"s3testcase/internal/fileservice/fileprocessor"
	"s3testcase/internal/storagelocator"
)

const (
	fileNameHeader = "X-File-Name"
	maxBodySize    = 10 << 30 // 10 GB
)

type Service struct {
	cfg       Config
	srv       *http.Server
	db        *sql.DB
	locator   *storagelocator.Locator
	processor *fileprocessor.FileProcessor
	cleaner   *filecleaner.Cleaner
	errStopCh chan error
	running   bool
	locatorCh chan error
	done      chan struct{}
}

func New(cfg Config, l *storagelocator.Locator) (*Service, error) {
	if err := os.MkdirAll(cfg.FileProcessor.UploadDir, 0755); err != nil {
		return nil, err
	}

	if err := os.MkdirAll(cfg.FileProcessor.DownloadDir, 0755); err != nil {
		return nil, err
	}
	bd, err := db.InitDB(cfg.DB.DSN())
	if err != nil {
		log.Err(err).Msgf("failed to connect to DB %s", cfg.DB.SafeDSN())
		return nil, err
	}
	if err := db.RunMigrations(cfg.MigrationsDir, cfg.DB.DSN()); err != nil {
		return nil, err
	}

	store := filedatastore.NewStore(bd)
	fileProcessor := fileprocessor.NewFileProcessor(fileprocessor.LoadConfig(), store, l)
	cleaner := filecleaner.NewCleaner(filecleaner.LoadConfig(), store, l)

	r := chi.NewRouter()
	srv := &Service{
		cfg: cfg,
		srv: &http.Server{
			Addr:              cfg.ListenAddr,
			Handler:           r,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       5 * time.Minute,
		},
		db:        bd,
		errStopCh: make(chan error, 1),
		locator:   l,
		processor: fileProcessor,
		cleaner:   cleaner,
		done:      make(chan struct{}),
	}
	r.Post("/upload", srv.uploadFileHandler)
	r.Get("/files/{name}", srv.downloadFileHandler)

	return srv, nil
}

func (s *Service) Run() chan error {
	if s.running {
		return nil
	}
	s.running = true

	go func() {
		defer close(s.errStopCh)
		s.locator.Run()
		s.waitStorages()
		s.cleaner.Run()
		s.processor.Run()

		log.Info().Msgf("listening on %s", s.srv.Addr)
		if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.errStopCh <- err
		}
	}()
	return s.errStopCh
}

func (s *Service) Shutdown(ctx context.Context) error {
	close(s.done)
	var allErrors error
	if s.running {
		if err := s.srv.Shutdown(ctx); err != nil {
			allErrors = err
			log.Error().Err(err).Msg("failed to shutdown http server")
		}
	}

	if s.locator != nil {
		if err := s.locator.Shutdown(ctx); err != nil {
			errors.Join(allErrors, err)
			log.Err(err).Msg("failed to shutdown locator")
		}
	}

	if s.cleaner != nil {
		if err := s.cleaner.Shutdown(ctx); err != nil {
			errors.Join(allErrors, err)
			log.Err(err).Msg("failed to shutdown cleaner")
		}
	}

	if s.processor != nil {
		if err := s.processor.Shutdown(ctx); err != nil {
			errors.Join(allErrors, err)
			log.Err(err).Msg("failed to shutdown processor")
		}
	}

	if s.db != nil {
		var err error
		done := make(chan struct{})
		go func() {
			if err = s.db.Close(); err != nil {
				log.Error().Err(err).Msg("db close error")
			}
			close(done)
		}()
		select {
		case <-ctx.Done():
			log.Err(ctx.Err()).Msg("timeout reached before db closed")
			return ctx.Err()
		case <-done:
			if err == nil {
				errors.Join(allErrors, err)
			}
		}
	}

	if allErrors != nil {
		log.Info().Msg("shutdown failed")
		return allErrors
	}
	log.Info().Msg("shutdown done")
	return nil
}

func (s *Service) uploadFileHandler(w http.ResponseWriter, r *http.Request) {
	fileName := r.Header.Get(fileNameHeader)
	if fileName == "" {
		http.Error(w, "Missing X-File-Name header", http.StatusBadRequest)
		return
	}

	if r.ContentLength > maxBodySize {
		http.Error(w, "Request body too large", http.StatusRequestEntityTooLarge)
		return
	}

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
	defer r.Body.Close()
	err := s.processor.Save(r.Context(), fileprocessor.FileData{
		Name:   fileName,
		Size:   r.ContentLength,
		Reader: r.Body,
		Type:   contentType,
	})
	if err != nil {
		log.Err(err).Msgf("failed to save file %s", fileName)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Debug().Msgf("file saved: %s", fileName)
}

func (s *Service) downloadFileHandler(w http.ResponseWriter, r *http.Request) {
	fileName := chi.URLParam(r, "name")
	if fileName == "" {
		http.Error(w, "missing file name", http.StatusBadRequest)
		return
	}

	f, err := s.processor.Get(r.Context(), fileName)
	if err != nil {
		if errors.Is(err, fileprocessor.ErrFileNotFound) {
			log.Debug().Msgf("file not found: %s", fileName)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		log.Err(err).Msgf("failed to get file %s", fileName)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer f.Close()

	if err != nil {
		if errors.Is(err, context.Canceled) {
			log.Debug().Msg("request canceled")
			return
		}
		if errors.Is(err, context.DeadlineExceeded) {
			log.Error().Msgf("request timeout")
			http.Error(w, "Request timeout", http.StatusGatewayTimeout)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Length", strconv.FormatInt(f.Size, 10))
	w.Header().Set("Content-Type", f.Type)
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, f.Name))

	if err := f.Copy(r.Context(), w); err != nil {
		log.Err(err).Msgf("failed copy to body - file %s-%s", f.UUID.String(), fileName)
		return
	}

	log.Debug().Msgf("file sent: %s", fileName)
}

func (s *Service) waitStorages() {
	if s.locator.StoragesCount() >= s.cfg.FileProcessor.FilePartsCount {
		return
	}
	for {
		select {
		case <-s.done:
			return
		case <-time.After(5 * time.Second):
			c := s.locator.StoragesCount()
			if c >= s.cfg.FileProcessor.FilePartsCount {
				return
			}
			log.Warn().Msgf("waiting for storages (registered %d)", c)
		}
	}
}
