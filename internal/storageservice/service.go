// SEKAI Digital Twin Platform
// Copyright (c) 2022, SEKAI DIGITAL TWINS LTD.
// All rights reserved.
// Author: Aleksandr A. Lomov
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// * Use and Redistributions of source code allowed only by SEKAI Platform license holders
//
// * Use and Redistributions of source code must retain the above copyright notice, this
//   list of conditions and the following disclaimer.
//
// * Redistributions in binary form must reproduce the above copyright notice,
//   this list of conditions and the following disclaimer in the documentation
//   and/or other materials provided with the distribution.
//
// * Neither the name of the copyright holder nor the names of its
//   contributors may be used to endorse or promote products derived from
//   this software without specific prior written permission.
//
// https://www.sekai.io/
// https://www.sekai.io/legal/licensing/
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
// ----------------------------------------------------------------------

package storageservice

import (
	"context"
	"encoding/json"
	"github.com/go-chi/chi/v5"
	"io"
	"net/http"
	"os"
	"path"
	"time"

	"github.com/rs/zerolog/log"
	"s3testcase/internal/storage"
	"s3testcase/internal/storagelocator"
	"s3testcase/internal/util"
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
		ListenAddr: util.GetEnv("LISTEN_ADDR", ":8001"),
		StorageDir: util.GetEnv("STORAGE_DIR", path.Join(os.TempDir(), serviceName)),
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
	r.Get("/files/{key}", service.Download)
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
	ctx := context.TODO()
	if err := s.fs.Save(ctx, filename, r.Body); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"ok":       true,
		"fullSize": s.fs.Size(),
	})
}

func (s *Service) Download(w http.ResponseWriter, r *http.Request) {
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
		http.Error(w, "failed to send file", http.StatusInternalServerError)
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
