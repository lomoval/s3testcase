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

package storagelocator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"s3testcase/internal/util"
)

var ErrStorageNotFound = fmt.Errorf("storage not found")

type StorageInfo struct {
	UUID uuid.UUID
	Addr string
	Size int64
}

type LocatorConfig struct {
	ListenAddr string `yaml:"listenAddr"`
}

func LoadLocatorConfig() LocatorConfig {
	return LocatorConfig{
		ListenAddr: util.GetEnv("STORAGE_LOCATOR_LISTEN_ADDR", ":8090"),
	}
}

type Locator struct {
	server     *http.Server
	chDone     chan struct{}
	mapMutex   sync.RWMutex
	dataMap    map[uuid.UUID]*LiveRequest
	listenAddr string

	errCh chan error
}

func NewLocator(cfg LocatorConfig) (*Locator, error) {
	l := &Locator{
		dataMap:    make(map[uuid.UUID]*LiveRequest),
		chDone:     make(chan struct{}),
		listenAddr: cfg.ListenAddr,
		errCh:      make(chan error, 1),
	}

	mux := chi.NewRouter()
	mux.Post("/live", l.handleJSONRequest)
	l.server = &http.Server{
		Addr:              l.listenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	return l, nil
}

func (l *Locator) Run() chan error {
	go func() {
		defer close(l.errCh)
		log.Info().Msgf("listening on %s", l.listenAddr)
		if err := l.server.ListenAndServe(); err != http.ErrServerClosed {
			log.Err(err).Msgf("failed to start HTTP-server")
		}
		close(l.chDone)
	}()
	return l.errCh
}

func (l *Locator) Shutdown(ctx context.Context) error {
	if err := l.server.Shutdown(ctx); err != nil {
		log.Err(err).Msgf("failed to shutdown http server")
		return err
	}
	<-l.chDone
	log.Debug().Msgf("shutdown complete")
	return nil
}

func (l *Locator) Storages() []StorageInfo {
	l.mapMutex.RLock()
	defer l.mapMutex.RUnlock()
	storage := make([]StorageInfo, 0, len(l.dataMap))
	for _, request := range l.dataMap {
		if request.Timestamp.After(time.Now().Add(-1 * time.Minute)) {
			storage = append(storage, StorageInfo{
				UUID: request.ServiceID,
				Addr: request.AdvertisedAddr,
				Size: request.StorageSizeBytes,
			})
		} else {
			log.Warn().Msgf(
				"storage %s is not active, no live request from %s",
				request.ServiceID,
				request.Timestamp.Format("02.01.2006 15:04:05"),
			)
		}
	}
	return storage
}

func (l *Locator) StoragesByUUID(uuids ...uuid.UUID) (map[uuid.UUID]StorageInfo, error) {
	l.mapMutex.RLock()
	defer l.mapMutex.RUnlock()
	res := make(map[uuid.UUID]StorageInfo)
	for _, id := range uuids {
		s, ok := l.dataMap[id]
		if !ok {
			return nil, fmt.Errorf("storage %s not found: %w", id, ErrStorageNotFound)
		}
		res[id] = StorageInfo{
			UUID: s.ServiceID,
			Addr: s.AdvertisedAddr,
			Size: s.StorageSizeBytes,
		}
	}
	return res, nil
}

func (l *Locator) StorageByUUID(uuid uuid.UUID) (StorageInfo, error) {
	l.mapMutex.RLock()
	defer l.mapMutex.RUnlock()
	s, ok := l.dataMap[uuid]
	if !ok {
		return StorageInfo{}, fmt.Errorf("storage %s not found: %w", uuid, ErrStorageNotFound)
	}
	return StorageInfo{
		UUID: s.ServiceID,
		Addr: s.AdvertisedAddr,
		Size: s.StorageSizeBytes,
	}, nil
}

func (l *Locator) StoragesCount() int {
	l.mapMutex.RLock()
	defer l.mapMutex.RUnlock()
	return len(l.dataMap)
}

func (l *Locator) handleJSONRequest(w http.ResponseWriter, r *http.Request) {
	var request LiveRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		log.Err(err).Msgf("failed to decode live request")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	l.mapMutex.Lock()
	if _, ok := l.dataMap[request.ServiceID]; !ok {
		log.Info().Msgf("registered new storage '%s' - %s", request.ServiceID, request.AdvertisedAddr)
	}
	l.dataMap[request.ServiceID] = &request
	l.mapMutex.Unlock()

	w.WriteHeader(http.StatusOK)
}
