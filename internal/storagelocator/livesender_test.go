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

	var receivedCount int32 = 0
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
