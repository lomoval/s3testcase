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
	"bytes"
	"context"
	"encoding/json"
	"math/rand"
	"net/http"
	"net/url"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"s3testcase/internal/util"
)

const sendInterval = 10 * time.Second

type StorageSizer interface {
	Size() int64
}

type LiveSenderConfig struct {
	AdvertisedAddr string
	ServiceUUID    uuid.UUID
	LocatorURL     string
	SendInterval   time.Duration
}

func LoadLiveSenderConfig() LiveSenderConfig {
	return LiveSenderConfig{
		AdvertisedAddr: util.GetEnv("ADVERTISED_ADDR", "127.0.0.0.1.:8201"),
		ServiceUUID:    uuid.MustParse(util.GetEnv("SERVICE_UUID", "")),
		LocatorURL:     util.GetEnv("LOCATOR_URL", "http://127.0.0.0.1:8300"),
		SendInterval:   sendInterval,
	}
}

type LiveRequest struct {
	ServiceID        uuid.UUID `json:"serviceId"`
	AdvertisedAddr   string    `json:"advertisedAddr"`
	Timestamp        time.Time `json:"timestamp"`
	StorageSizeBytes int64     `json:"storageSizeBytes"`
}

type LiveSender struct {
	advertisedAddr string
	serviceID      uuid.UUID
	locatorAddr    string
	interval       time.Duration
	storage        StorageSizer

	stopChan chan struct{}
	runDone  chan struct{}
	client   *http.Client
}

func NewLiveSender(cfg LiveSenderConfig, ss StorageSizer) *LiveSender {
	return &LiveSender{
		serviceID:      cfg.ServiceUUID,
		advertisedAddr: cfg.AdvertisedAddr,
		storage:        ss,
		locatorAddr:    cfg.LocatorURL,
		interval:       cfg.SendInterval,
		client:         &http.Client{Timeout: 3 * time.Second},
		stopChan:       make(chan struct{}),
	}
}

func (s *LiveSender) Run() {
	s.runDone = make(chan struct{})
	go func() {
		defer close(s.runDone)
		s.sendWithBackoff()
		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				s.sendWithBackoff()
			case <-s.stopChan:
				log.Debug().Msgf("live-sender stopping")
				return
			}
		}
	}()
}

func (s *LiveSender) Shutdown(ctx context.Context) error {
	close(s.stopChan)

	select {
	case <-s.runDone:
		log.Info().Msg("shutdown completed")
		return nil
	case <-ctx.Done():
		log.Err(ctx.Err()).Msg("shutdown failed")
		return ctx.Err()
	}
}

func (s *LiveSender) sendWithBackoff() {
	const maxRetries = 5
	baseDelay := s.interval

	for i := 0; i < maxRetries; i++ {
		err := s.sendLive()
		if err == nil {
			return
		}

		delay := baseDelay * (1 << i)
		jitter := time.Duration(rand.Int63n(int64(delay / 2))) //nolint:gosec
		sleepTime := delay + jitter

		log.Debug().Msgf("live request failed: %v — retrying in %s", err, sleepTime)

		select {
		case <-time.After(sleepTime):
		case <-s.stopChan:
			return
		}
	}
}

func (s *LiveSender) sendLive() error {
	payload := LiveRequest{
		ServiceID:        s.serviceID,
		AdvertisedAddr:   s.advertisedAddr,
		Timestamp:        time.Now().UTC(),
		StorageSizeBytes: s.storage.Size(),
	}

	body, err := json.Marshal(payload)
	if err != nil {
		log.Err(err).Msg("failed to marshal live request")
		return err
	}
	u, _ := url.JoinPath(s.locatorAddr, "live")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", u, bytes.NewReader(body))
	if err != nil {
		log.Err(err).Msg("failed to create live request")
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}
