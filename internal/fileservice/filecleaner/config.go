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

package filecleaner

import (
	"os"
	"path"
	"time"

	usys "s3testcase/internal/utils/sys"
)

type Config struct {
	UploadDir   string
	DownloadDir string
	FilesMaxAge time.Duration
}

func LoadConfig() Config {
	return Config{
		UploadDir:   usys.GetEnv("UPLOAD_DIR", path.Join(os.TempDir(), "fp-upload")),
		DownloadDir: usys.GetEnv("DOWNLOAD_DIR", path.Join(os.TempDir(), "fp-download")),
		FilesMaxAge: 1 * time.Hour,
	}
}
