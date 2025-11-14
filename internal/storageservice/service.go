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

package storageservice

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"

	"s3testcase/internal/storage"
	"s3testcase/internal/storagelocator"
	usys "s3testcase/internal/utils/sys"
)

const (
	serviceName = "storage-service"
	maxBodySize = 10 << 30 // 10 GB
)

type Config struct {
	ListenAddr string `yaml:"listenAddr"`
	StorageDir string `yaml:"storageDir"`
}

type Service struct {
	srv     *http.Server
	fs      storage.Storage
	ls      *storagelocator.LiveSender
	running bool
	errChan chan error
}

func LoadConfig() Config {
	return Config{
		ListenAddr: usys.GetEnv("LISTEN_ADDR", ":8001"),
		StorageDir: usys.GetEnv("STORAGE_DIR", path.Join(os.TempDir(), serviceName)),
	}
}

func NewServer(cfg Config, ls *storagelocator.LiveSender, s storage.Storage) *Service {
	r := chi.NewRouter()
	service := Service{
		srv: &http.Server{
			Addr:              cfg.ListenAddr,
			Handler:           r,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       5 * time.Minute,
		},
		ls:      ls,
		errChan: make(chan error, 1),
		fs:      s,
	}

	r.Put("/files/{key}", service.uploadHandler)
	r.Get("/files/{key}", service.downloadHandler)
	r.Delete("/files/{key}", service.deleteHandler)

	return &service
}

func (s *Service) Run() chan error {
	if s.running {
		return nil
	}
	s.running = true

	go func() {
		defer close(s.errChan)
		log.Info().Msgf("starting locator sender")
		s.ls.Run()
		log.Info().Msgf("listening on %s", s.srv.Addr)
		if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.errChan <- err
		}
	}()
	return s.errChan
}

func (s *Service) Shutdown(ctx context.Context) error {
	var err error
	if s.running {
		if err = s.srv.Shutdown(ctx); err != nil {
			log.Error().Err(err).Msg("failed htt server shutdown")
		}
	}

	if err = s.ls.Shutdown(ctx); err != nil {
		log.Err(err).Msg("failed locator live sender shutdown")
	}

	log.Info().Msg("shutdown completed")
	return nil
}

func (s *Service) uploadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut && r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "use PUT or POST"})
		return
	}
	filename := chi.URLParam(r, "key")
	if filename == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "key required in URL"})
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
	defer r.Body.Close()
	log.Debug().Msgf("uploading file '%s'", filename)
	if err := s.fs.Save(r.Context(), filename, r.Body); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"success":  true,
		"fullSize": s.fs.Size(),
	})
}

func (s *Service) downloadHandler(w http.ResponseWriter, r *http.Request) {
	filename := chi.URLParam(r, "key")
	if filename == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "key required in URL"})
		return
	}

	data, err := s.fs.Get(filename)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	defer func() {
		if err := data.Close(); err != nil {
			log.Err(err).Msgf("failed close file '%s'", filename)
		}
	}()

	w.Header().Set("Content-Type", "application/octet-stream")

	if _, err := io.Copy(w, data.Reader); err != nil {
		log.Err(err).Msgf("failed send file '%s'", filename)
		return
	}
}

func (s *Service) deleteHandler(w http.ResponseWriter, r *http.Request) {
	if err := s.fs.Delete(chi.URLParam(r, "key")); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"ok":       true,
		"fullSize": s.fs.Size(),
	})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
