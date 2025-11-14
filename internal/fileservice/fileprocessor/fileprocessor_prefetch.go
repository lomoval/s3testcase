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
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/rs/zerolog/log"
	"s3testcase/internal/fileservice/filedatastore"
	ulog "s3testcase/internal/utils/log"
)

// It was done to test how pre-opening/prefetching would work compared to sequential processing (can be ignored).
// On a local machine this is not very representative, because connections are already in the pool
// and establishing a connection is fast.
// For small files, sequential processing is consistently faster; for large files, the performance is roughly the same.

type progressReader struct {
	r          io.Reader
	n          int64
	total      int64
	onProgress func()
	stopCalled bool
	threshold  float64
}

func (fp *FileProcessor) getPrefetched(ctx context.Context, dsFile *filedatastore.File, file *File) {
	resps := make([]*http.Response, len(dsFile.Locations))
	prefetchCh := make(chan int, 1)
	pr, pw := io.Pipe()
	file.Reader = pr
	go func() {
		defer pw.Close()
		defer func() {
			for _, r := range resps {
				if r != nil && r.Body != nil {
					r.Body.Close()
				}
			}
		}()

		// Make first request to start and use next requests to prefetch
		var err error
		resps[0], err = fp.loadPart(ctx, file, dsFile.Locations[0]) //nolint:bodyclose
		if err != nil {
			pw.CloseWithError(errors.Join(ErrDownloadFailed, err))
			return
		}
		log.Debug().Msgf("file %s-%s fetching part 1", dsFile.UUID.String(), dsFile.FileName)

		prefetchCh <- 0 // First request already done
		for i := 0; i < len(dsFile.Locations); i++ {
			select {
			case <-ctx.Done():
				pw.CloseWithError(ctx.Err())
				return
			case partIndex := <-prefetchCh:
				if partIndex != i {
					log.Error().Msgf("incorrect prefetched part index %d, expected %d", partIndex, i)
					continue
				}
			}

			if resps[i] == nil {
				log.Error().Msgf("download file %s-%s, part %d from failed", file.UUID.String(), file.Name, i+1)
				pw.CloseWithError(ErrDownloadFailed)
				return
			}
			resp := resps[i]

			reader := &progressReader{r: resp.Body, total: dsFile.Locations[i].Size, threshold: 0.95}
			if i+1 < len(dsFile.Locations) {
				nextIndex := i + 1
				reader.onProgress = fp.prefetchProgressHandler(ctx, file, dsFile, nextIndex, resps, prefetchCh)
			}

			log.Debug().Msgf("file %s-%s copy part %d start", dsFile.UUID.String(), dsFile.FileName, i+1)
			if _, err := io.Copy(pw, reader); err != nil {
				pw.CloseWithError(fmt.Errorf("copy part %d: %w", i, err))
				return
			}
			resp.Body.Close()
			resps[i] = nil
			log.Debug().Msgf("file %s-%s copy part %d done", dsFile.UUID.String(), dsFile.FileName, i+1)
		}
	}()
}

func (fp *FileProcessor) prefetchProgressHandler(
	ctx context.Context,
	file *File,
	dsFile *filedatastore.File,
	nextIndex int,
	resps []*http.Response,
	prefetchCh chan int,
) func() {
	return func() {
		defer func() {
			select {
			case prefetchCh <- nextIndex:
			default:
			}
		}()

		resp, err := fp.loadPart(ctx, file, dsFile.Locations[nextIndex]) //nolint:bodyclose
		if err != nil {
			log.Err(err).Msgf("%s-%s failed to prefetched next part %d", file.UUID.String(), file.Name, nextIndex)
		} else {
			resps[nextIndex] = resp
			log.Debug().Msgf(
				"file %s-%s prefetched next part %d",
				dsFile.UUID.String(),
				dsFile.FileName,
				nextIndex+1,
			)
		}
	}
}

func (fp *FileProcessor) loadPart(
	ctx context.Context,
	file *File,
	l filedatastore.FileLocation,
) (*http.Response, error) {
	s, err := fp.locator.StorageByUUID(l.LocationUUID)
	if err != nil {
		return nil, err
	}
	url := fmt.Sprintf("http://%s/files/%s", s.Addr, storageFilename(file, l.PartNumber))
	return fp.getHTTPOKResponse(ulog.ContextWithHTTPTracer(ctx, "download-file-prefetch"), url)
}

func (fp *FileProcessor) getHTTPOKResponse(ctx context.Context, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := fp.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("bad status %d for part %s", resp.StatusCode, url)
	}
	return resp, nil
}

func (p *progressReader) Read(buf []byte) (int, error) {
	n, err := p.r.Read(buf)
	if n > 0 {
		p.n += int64(n)
		if p.total > 0 && p.onProgress != nil && !p.stopCalled && float64(p.n)/float64(p.total) >= p.threshold {
			p.stopCalled = true
			go p.onProgress()
		}
	}
	return n, err
}
