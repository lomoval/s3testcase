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

package fileprocessor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"golang.org/x/exp/mmap"
)

type FileUploadTask struct {
	// Number of file part (1..N).
	Number     int
	FilePath   string
	StartIndex int64
	EndIndex   int64
	Size       int64
	UploadURL  string
	ResultCh   chan FileTaskResult
	Ctx        context.Context
	ErrCount   int
}

type FileDownloadTask struct {
	Number       int
	FilePath     string
	DownloadURL  string
	LocationUUID uuid.UUID
	ResultCh     chan FileTaskResult
	Ctx          context.Context
	ErrCount     int
}

type FileTaskResult struct {
	// Number of file part.
	Number        int
	ProcessedTime time.Time
	Err           error
}

type UploadWorkerPool struct {
	uploadCount   int
	downloadCount int
	uploadWG      sync.WaitGroup
	downloadWG    sync.WaitGroup
	uploadTasks   chan FileUploadTask
	downloadTasks chan FileDownloadTask
	ctx           context.Context
	cancel        context.CancelFunc
	errChan       chan error
}

func NewUploadWorkerPool(uploadCount int, downloadCount int) *UploadWorkerPool {
	ctx, cancel := context.WithCancel(context.Background())
	pool := &UploadWorkerPool{
		uploadCount:   uploadCount,
		downloadCount: downloadCount,
		uploadTasks:   make(chan FileUploadTask, uploadCount),
		downloadTasks: make(chan FileDownloadTask, downloadCount),
		ctx:           ctx,
		cancel:        cancel,
		errChan:       make(chan error, uploadCount),
	}
	return pool
}

func (p *UploadWorkerPool) Run() {
	for i := 0; i < p.uploadCount; i++ {
		p.uploadWG.Add(1)
		go p.uploadWorker()
	}
	for i := 0; i < p.downloadCount; i++ {
		p.downloadWG.Add(1)
		go p.downloadWorker()
	}
}

func (p *UploadWorkerPool) Shutdown(ctx context.Context) error {
	p.cancel()
	close(p.uploadTasks)
	close(p.downloadTasks)
	p.uploadWG.Wait()
	p.downloadWG.Wait()
	close(p.errChan)

	done := make(chan struct{})
	go func() {
		p.uploadWG.Wait()
		p.downloadWG.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

func (p *UploadWorkerPool) AddUploadTask(task FileUploadTask) {
	select {
	case p.uploadTasks <- task:
	case <-p.ctx.Done():
	case <-task.Ctx.Done():
	}
}

func (p *UploadWorkerPool) AddDownloadTask(task FileDownloadTask) {
	select {
	case p.downloadTasks <- task:
	case <-p.ctx.Done():
	case <-task.Ctx.Done():
	}
}

func (p *UploadWorkerPool) uploadWorker() {
	defer p.uploadWG.Done()
	for {
		select {
		case <-p.ctx.Done():
			return
		case task, ok := <-p.uploadTasks:
			if !ok {
				return
			}
			select {
			case <-task.Ctx.Done():
			default:
			}
			log.Debug().Msgf("worker get task: %d %s", task.Number, task.UploadURL)
			var err error
			err = processPartMmap(task)
			if err != nil {
				log.Err(err).Msgf("task failed %d %s", task.Number, task.UploadURL)
			}

			select {
			case task.ResultCh <- FileTaskResult{
				Number:        task.Number,
				ProcessedTime: time.Now().UTC(),
				Err:           err,
			}:
			case <-task.Ctx.Done():
			}
		}
	}
}

func (p *UploadWorkerPool) downloadWorker() {
	defer p.downloadWG.Done()
	for {
		select {
		case <-p.ctx.Done():
			return
		case task, ok := <-p.downloadTasks:
			if !ok {
				return
			}
			err := download(task)
			if err != nil {
				log.Err(err).Msgf("failed download %s", task.DownloadURL)
			}
			select {
			case task.ResultCh <- FileTaskResult{
				Number:        task.Number,
				ProcessedTime: time.Now().UTC(),
				Err:           err,
			}:
			case <-task.Ctx.Done():
			}
		}
	}
}

func upload(ctx context.Context, url string, index int, data []byte) error {
	req, err := http.NewRequestWithContext(ctx, "PUT", url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Part-Index", fmt.Sprintf("%d", index))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP error: %s", string(body))
	}

	log.Debug().Msgf("part %d sent successfully, %d bytes\n", index, len(data))
	return nil
}

func download(task FileDownloadTask) error {
	f, err := os.Create(task.FilePath)
	if err != nil {
		return err
	}
	defer func(f *os.File) {
		if err := f.Close(); err != nil {
			log.Err(err).Msgf("failed close file %s", f.Name())
		}
	}(f)

	req, err := http.NewRequestWithContext(task.Ctx, "GET", task.DownloadURL, nil)
	if err != nil {
		return err
	}

	client := &http.Client{Timeout: 20 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Err(err).Msgf("failed close response body for %s", task.DownloadURL)
		}
	}()

	if _, err := io.Copy(f, resp.Body); err != nil {
		return err
	}

	return nil
}

func processPartMmap(t FileUploadTask) error {
	r, err := mmap.Open(t.FilePath)
	if err != nil {
		return err
	}
	defer r.Close()

	length := t.EndIndex - t.StartIndex
	buf := make([]byte, length)
	_, err = r.ReadAt(buf, t.StartIndex)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}

	err = upload(t.Ctx, t.UploadURL, t.Number, buf)
	if err != nil {
		return err
	}
	log.Debug().Msgf("part %d (%d–%d): %d bytes", t.Number, t.StartIndex, t.EndIndex, len(buf))
	return nil
}

type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (cr *ctxReader) Read(p []byte) (int, error) {
	select {
	case <-cr.ctx.Done():
		return 0, cr.ctx.Err()
	default:
		return cr.r.Read(p)
	}
}
