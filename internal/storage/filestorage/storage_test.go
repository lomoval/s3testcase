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

package filestorage

import (
	"context"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	ulog "s3testcase/internal/utils/log"
)

func TestMain(m *testing.M) {
	ulog.InitConsoleWriter(zerolog.ErrorLevel)
	os.Exit(m.Run())
}

func TestDirSize(t *testing.T) {
	c := Config{StorageDir: t.TempDir()}

	content := t.Name()
	expectedSize := int64(len(content) * 4)
	require.NoError(t, createFileInDir(path.Join(c.StorageDir, dataDirName), "test1.txt", content))
	require.NoError(t, createFileInDir(path.Join(c.StorageDir, dataDirName), "test2.txt", content))
	require.NoError(t, createFileInDir(path.Join(c.StorageDir, dataDirName, "dir1"), "test3.txt", content))
	require.NoError(t, createFileInDir(path.Join(c.StorageDir, dataDirName, "dir1", "dir2"), "test4.txt", content))

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	actualSize, err := getDirSize(ctx, path.Join(c.StorageDir), runtime.NumCPU())
	require.NoError(t, err)
	require.Equal(t, expectedSize, actualSize)
}

func TestStorageDirectoriesExist(t *testing.T) {
	c := Config{StorageDir: t.TempDir()}

	s := NewStorage(c)
	require.NoError(t, s.Run())
	exists, err := dirExists(c.StorageDir)
	require.NoError(t, err)
	require.True(t, exists)
	exists, err = dirExists(path.Join(c.StorageDir, tmpDirName))
	require.NoError(t, err)
	require.True(t, exists)
	exists, err = dirExists(path.Join(c.StorageDir, dataDirName))
	require.NoError(t, err)
	require.True(t, exists)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.NoError(t, s.Shutdown(ctx))
}

func TestStorageSizeInitial(t *testing.T) {
	c := Config{StorageDir: t.TempDir()}

	content := t.Name()
	expectedSize := int64(len(content) * 4)
	require.NoError(t, createFileInDir(path.Join(c.StorageDir, dataDirName), "test1.txt", content))
	require.NoError(t, createFileInDir(path.Join(c.StorageDir, dataDirName), "test2.txt", content))
	require.NoError(t, createFileInDir(path.Join(c.StorageDir, dataDirName, "dir1"), "test3.txt", content))
	require.NoError(t, createFileInDir(path.Join(c.StorageDir, dataDirName, "dir1", "dir2"), "test4.txt", content))

	s := NewStorage(c)
	require.NoError(t, s.Run())
	require.Equal(t, expectedSize, s.Size())

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.NoError(t, s.Shutdown(ctx))
}

func TestStorageSave(t *testing.T) {
	c := Config{StorageDir: t.TempDir()}

	content := t.Name()
	expectedSize := int64(len(content))

	s := NewStorage(c)
	require.NoError(t, s.Run())
	require.NoError(t, s.Save(context.Background(), "test.txt", strings.NewReader(content)))

	actualSize, err := fileSize(path.Join(c.StorageDir, dataDirName, "test.txt"))
	require.NoError(t, err)
	require.Equal(t, expectedSize, actualSize)

	exists, err := fileExists(path.Join(c.StorageDir, tmpDirName, "test.txt"))
	require.NoError(t, err)
	require.False(t, exists)

	require.Equal(t, expectedSize, s.Size())

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.NoError(t, s.Shutdown(ctx))
}

func TestStorageSaveSame(t *testing.T) {
	c := Config{StorageDir: t.TempDir()}

	expectedContent := "test content updated"
	expectedSize := int64(len(expectedContent))

	s := NewStorage(c)
	require.NoError(t, s.Run())
	require.NoError(t, s.Save(context.Background(), "test.txt", strings.NewReader("test content")))
	require.NoError(t, s.Save(context.Background(), "test.txt", strings.NewReader(expectedContent)))

	actualSize, err := fileSize(path.Join(c.StorageDir, dataDirName, "test.txt"))
	require.NoError(t, err)
	require.Equal(t, expectedSize, actualSize)

	exists, err := fileExists(path.Join(c.StorageDir, tmpDirName, "test.txt"))
	require.NoError(t, err)
	require.False(t, exists)

	require.Equal(t, expectedSize, s.Size())

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.NoError(t, s.Shutdown(ctx))
}

func TestStorageSaveGet(t *testing.T) {
	c := Config{StorageDir: t.TempDir()}

	key := "test.txt"
	content := t.Name()
	expectedSize := int64(len(content))

	s := NewStorage(c)
	require.NoError(t, s.Run())
	require.NoError(t, s.Save(context.Background(), key, strings.NewReader(content)))

	data, err := s.Get("test.txt")
	require.NoError(t, err)
	require.Equal(t, expectedSize, data.Size)

	bytes, err := io.ReadAll(data.Reader)
	require.NoError(t, data.Close())
	require.NoError(t, err)
	require.Equal(t, content, string(bytes))

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.NoError(t, s.Shutdown(ctx))
}

func TestStorageSaveDelete(t *testing.T) {
	c := Config{StorageDir: t.TempDir()}

	content := t.Name()
	contentSize := int64(len(content))
	expectedSize := int64(len(content) * 3)

	s := NewStorage(c)
	require.NoError(t, s.Run())
	require.NoError(t, s.Save(context.Background(), "test1.txt", strings.NewReader(content)))
	require.NoError(t, s.Save(context.Background(), "test2.txt", strings.NewReader(content)))
	require.NoError(t, s.Save(context.Background(), "test3.txt", strings.NewReader(content)))
	require.Equal(t, expectedSize, s.Size())

	require.NoError(t, s.Delete("test2.txt"))
	_, err := s.Get("test2.txt")
	require.ErrorIs(t, err, ErrFileNotFound)

	data, err := s.Get("test1.txt")
	require.NoError(t, err)
	require.NoError(t, data.Close())
	require.Equal(t, contentSize, data.Size)
	data, err = s.Get("test3.txt")
	require.NoError(t, err)
	require.Equal(t, contentSize, data.Size)
	require.NoError(t, data.Close())

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.NoError(t, s.Shutdown(ctx))
}

func TestStorageDeleteNotExist(t *testing.T) {
	c := Config{StorageDir: t.TempDir()}

	s := NewStorage(c)
	require.NoError(t, s.Run())
	require.NoError(t, s.Delete("not_exists"))

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.NoError(t, s.Shutdown(ctx))
}

func TestStorageCleanupOnRun(t *testing.T) {
	c := Config{StorageDir: t.TempDir(), CleanupInterval: 10000 * time.Millisecond, CleanupAge: 5 * time.Millisecond}

	content := t.Name()
	require.NoError(t, createFileInDir(path.Join(c.StorageDir, tmpDirName), "test.txt", content))
	time.Sleep(6 * time.Millisecond) // Wait to make file older than cleanup age.

	s := NewStorage(c)
	require.NoError(t, s.Run())

	require.Eventually(
		t,
		func() bool {
			exists, err := fileExists(path.Join(c.StorageDir, tmpDirName, "test.txt"))
			require.NoError(t, err)
			return !exists
		},
		250*time.Millisecond,
		50*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.NoError(t, s.Shutdown(ctx))
}

func TestStorageCleanupOnInterval(t *testing.T) {
	c := Config{StorageDir: t.TempDir(), CleanupInterval: 25 * time.Millisecond, CleanupAge: 300 * time.Millisecond}

	start := time.Now()
	content := t.Name()
	require.NoError(t, createFileInDir(path.Join(c.StorageDir, tmpDirName), "test1.txt", content))
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, createFileInDir(path.Join(c.StorageDir, tmpDirName), "test2.txt", content))

	s := NewStorage(c)
	require.NoError(t, s.Run())

	require.Eventually(
		t,
		func() bool {
			exists1, err := fileExists(path.Join(c.StorageDir, tmpDirName, "test1.txt"))
			require.NoError(t, err)
			exists2, err := fileExists(path.Join(c.StorageDir, tmpDirName, "test2.txt"))
			require.NoError(t, err)
			return !exists1 && exists2 && time.Since(start) >= 300*time.Millisecond && time.Since(start) < 400*time.Millisecond
		},
		350*time.Millisecond,
		50*time.Millisecond)

	require.Eventually(
		t,
		func() bool {
			exists, err := fileExists(path.Join(c.StorageDir, tmpDirName, "test2.txt"))
			require.NoError(t, err)
			return !exists && time.Since(start) >= 400*time.Millisecond
		},
		350*time.Millisecond,
		50*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.NoError(t, s.Shutdown(ctx))
}

// Helpers

func dirExists(path string) (bool, error) {
	info, err := os.Stat(path)
	if err == nil {
		return info.IsDir(), nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func fileExists(path string) (bool, error) {
	info, err := os.Stat(path)
	if err == nil {
		return !info.IsDir(), nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// GetFileSize возвращает размер файла в байтах.
func fileSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func createFileInDir(dir, filename, content string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, filename), []byte(content), 0o644)
}
