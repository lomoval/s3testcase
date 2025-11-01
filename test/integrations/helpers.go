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

package integrations

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/docker/docker/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
)

type HashingGeneratorReader struct {
	remaining int64
	hasher    hash.Hash
	randSrc   *rand.Rand
}

func NewHashingGeneratorReader(size int64) *HashingGeneratorReader {
	return &HashingGeneratorReader{
		remaining: size,
		hasher:    sha256.New(),
		randSrc:   rand.New(rand.NewSource(42)),
	}
}

func (r *HashingGeneratorReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, io.EOF
	}

	toGenerate := int64(len(p))
	if toGenerate > r.remaining {
		toGenerate = r.remaining
	}

	for i := int64(0); i < toGenerate; i++ {
		p[i] = byte(r.randSrc.Intn(256))
	}

	r.hasher.Write(p[:toGenerate])
	r.remaining -= toGenerate

	if r.remaining == 0 {
		return int(toGenerate), io.EOF
	}
	return int(toGenerate), nil
}

func (r *HashingGeneratorReader) Sum() string {
	return hex.EncodeToString(r.hasher.Sum(nil))
}

func HashHTTPResponseBody(resp *http.Response) (string, error) {
	defer resp.Body.Close()

	hasher := sha256.New()
	_, err := io.Copy(hasher, resp.Body)
	if err != nil {
		return "", fmt.Errorf("error reading response: %w", err)
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func FileExists(ctx context.Context, container testcontainers.Container, path string) (bool, error) {
	cmd := []string{"sh", "-c", fmt.Sprintf("test -e %s && printf ok || printf  missing", path)}
	code, out, err := container.Exec(ctx, cmd)
	if err != nil {
		return false, fmt.Errorf("exec failed: %w", err)
	}
	if code != 0 {
		return false, nil
	}
	return strings.TrimSpace(readAll(out)) == "ok", nil
}

func FileSize(ctx context.Context, container testcontainers.Container, path string) (int64, error) {
	cmd := []string{"sh", "-c", fmt.Sprintf("stat -c %%s %s 2>/dev/null || echo -1", path)}
	code, out, err := container.Exec(ctx, cmd)
	if err != nil {
		return -1, fmt.Errorf("exec failed: %w", err)
	}
	if code != 0 {
		return -1, nil
	}

	output := strings.TrimSpace(readAll(out))
	if output == "-1" {
		return -1, nil
	}

	size, err := strconv.ParseInt(output, 10, 64)
	if err != nil {
		return -1, fmt.Errorf("parse size: %w", err)
	}

	return size, nil
}

func readAll(r io.Reader) string {
	if r == nil {
		return ""
	}
	buf := new(bytes.Buffer)
	_, _ = stdcopy.StdCopy(buf, nil, r)
	return buf.String()
}

func printContainerLogs(ctx context.Context, c testcontainers.Container) {
	out, err := c.Logs(ctx)
	if err != nil {
		return
	}
	defer out.Close()
	_, _ = io.Copy(os.Stdout, out)
}
