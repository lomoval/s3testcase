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

package storagelocator_test

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"s3testcase/internal/storagelocator"
	ulog "s3testcase/internal/util/log"
)

type testStorageSizer struct {
	size int64
}

func (s *testStorageSizer) Size() int64 {
	return s.size
}

func TestMain(m *testing.M) {
	ulog.InitConsoleWriter(zerolog.ErrorLevel)
	os.Exit(m.Run())
}

func TestLiveSender(t *testing.T) {
	cfg := storagelocator.LiveSenderConfig{
		AdvertisedAddr: "127.0.0.1:8080",
		ServiceUUID:    uuid.MustParse("10000000-0000-0000-0000-000000000000"),
		SendInterval:   20 * time.Millisecond,
	}
	storage := &testStorageSizer{size: 555}

	var receivedCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req storagelocator.LiveRequest
		require.Equal(t, "POST", r.Method)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("Failed to decode request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		log.Printf("Received live: %+v\n", req)
		w.WriteHeader(http.StatusOK)
		require.Equal(t, cfg.AdvertisedAddr, req.AdvertisedAddr)
		require.Equal(t, cfg.ServiceUUID, req.ServiceID)
		require.Equal(t, storage.Size(), req.StorageSizeBytes)
		atomic.AddInt32(&receivedCount, 1)
	}))
	defer server.Close()

	cfg.LocatorURL = server.URL

	ls := storagelocator.NewLiveSender(cfg, storage)
	ls.Run()

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&receivedCount) > 3
	},
		200*time.Millisecond,
		10*time.Millisecond,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.NoError(t, ls.Shutdown(ctx))
}
