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

package fileprocessor_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"s3testcase/internal/fileservice/fileprocessor"
	ulog "s3testcase/internal/utils/log"
)

func TestMain(m *testing.M) {
	ulog.InitConsoleWriter(zerolog.TraceLevel)
	os.Exit(m.Run())
}

func createFileInDir(dir, filename, content string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, filename), []byte(content), 0o644)
}

func TestWorkerPoolUpload(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "testfile.txt")
	require.NoError(t, createFileInDir(dir, "testfile.txt", "1234567890"))
	wp := fileprocessor.NewUploadWorkerPool(2, 2)
	wp.Run()
	defer wp.Shutdown(t.Context())

	var receivedRequests int32
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "PUT", r.Method)
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, "12345", string(body))
		atomic.AddInt32(&receivedRequests, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server1.Close()

	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "PUT", r.Method)
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, "67890", string(body))
		atomic.AddInt32(&receivedRequests, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server1.Close()

	resultCh := make(chan fileprocessor.FileTaskResult, 2)
	wp.AddUploadTask(fileprocessor.FileUploadTask{
		FilePath:   filePath,
		StartIndex: 0,
		EndIndex:   5,
		Number:     1,
		ResultCh:   resultCh,
		UploadURL:  server1.URL + "/files/testfile.txt",
		Ctx:        t.Context(),
	})
	wp.AddUploadTask(fileprocessor.FileUploadTask{
		FilePath:   filePath,
		StartIndex: 5,
		EndIndex:   10,
		Number:     2,
		ResultCh:   resultCh,
		UploadURL:  server2.URL + "/files/testfile.txt",
		Ctx:        t.Context(),
	})

	results := 0
	require.Eventually(t, func() bool {
		select {
		case <-resultCh:
			results++
		default:
		}
		return results == 2
	},
		200*time.Millisecond,
		10*time.Millisecond,
	)

	require.Equal(t, int32(2), atomic.LoadInt32(&receivedRequests))
}

func TestFileProcessorNotStored(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "testfile.txt")
	require.NoError(t, createFileInDir(dir, "testfile.txt", "1234567890"))
	wp := fileprocessor.NewUploadWorkerPool(2, 2)
	wp.Run()
	defer wp.Shutdown(t.Context())

	var receivedRequests int32
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "PUT", r.Method)
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, "12345", string(body))
		atomic.AddInt32(&receivedRequests, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server1.Close()

	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "PUT", r.Method)
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, "67890", string(body))
		atomic.AddInt32(&receivedRequests, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server2.Close()

	resultCh := make(chan fileprocessor.FileTaskResult, 2)
	wp.AddUploadTask(fileprocessor.FileUploadTask{
		FilePath:   filePath,
		StartIndex: 0,
		EndIndex:   5,
		Number:     1,
		ResultCh:   resultCh,
		UploadURL:  server1.URL + "/files/testfile.txt",
		Ctx:        t.Context(),
	})
	wp.AddUploadTask(fileprocessor.FileUploadTask{
		FilePath:   filePath,
		StartIndex: 5,
		EndIndex:   10,
		Number:     2,
		ResultCh:   resultCh,
		UploadURL:  server2.URL + "/files/testfile.txt",
		Ctx:        t.Context(),
	})

	results := 0
	require.Eventually(t, func() bool {
		select {
		case <-resultCh:
			results++
		default:
		}
		return results == 2
	},
		11200*time.Millisecond,
		10*time.Millisecond,
	)

	require.Equal(t, int32(2), atomic.LoadInt32(&receivedRequests))
}
