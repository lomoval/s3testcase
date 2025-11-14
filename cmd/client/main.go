package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
)

func main() {
	var addr string

	var file string
	var name string
	var out string

	rootCmd := &cobra.Command{
		Use:   "client",
		Short: "Client CLI for upload/download",
	}

	rootCmd.PersistentFlags().StringVarP(&addr, "addr", "a", "127.0.0.1:8100", "server address")

	// --- Upload command ---
	uploadCmd := &cobra.Command{
		Use:   "upload",
		Short: "Upload a file",
		Run: func(_ *cobra.Command, _ []string) {
			if file == "" {
				log.Fatal("upload requires --file/-f flag")
			}
			if err := uploadFile(addr, file); err != nil {
				log.Fatalf("upload failed: %v", err)
			}
		},
	}
	uploadCmd.Flags().StringVarP(&file, "file", "f", "", "file to upload")
	rootCmd.AddCommand(uploadCmd)

	// --- Download command ---
	downloadCmd := &cobra.Command{
		Use:   "download",
		Short: "Download a file",
		Run: func(_ *cobra.Command, _ []string) {
			if out == "" {
				log.Fatal("download requires --out/-o flag")
			}

			if name == "" {
				log.Fatal("download requires --name/-n flag")
			}

			if err := downloadFile(addr, name, out); err != nil {
				log.Fatalf("download failed: %v", err)
			}
		},
	}
	downloadCmd.Flags().StringVarP(&name, "name", "n", "", "name of the file to download")
	downloadCmd.Flags().StringVarP(&out, "out", "o", "", "where to save downloaded file")
	rootCmd.AddCommand(downloadCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func uploadFile(addr, filePath string) error {
	uploadURL := fmt.Sprintf("http://%s/upload", addr)
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		return fmt.Errorf("cannot get absolute path: %w", err)
	}

	f, err := os.Open(absPath)
	if err != nil {
		return fmt.Errorf("cannot open file: %w", err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return fmt.Errorf("cannot stat file: %w", err)
	}

	buffer := make([]byte, 512)
	n, _ := f.Read(buffer)
	contentType := http.DetectContentType(buffer[:n])

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("cannot seek file: %w", err)
	}

	hasher := sha256.New()
	tee := io.TeeReader(f, hasher)

	fmt.Printf("Uploading file to: %s\n", uploadURL)
	fmt.Printf("Name: %s\n", filepath.Base(filePath))
	fmt.Printf("Size: %d bytes\n", stat.Size())
	fmt.Printf("Content-Type: %s\n", contentType)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	startTime := time.Now()
	var reader io.Reader
	if stat.Size() > 0 {
		reader = tee
	}
	req, err := http.NewRequestWithContext(ctx, "POST", uploadURL, reader)
	if err != nil {
		return fmt.Errorf("cannot create request: %w", err)
	}
	req.ContentLength = stat.Size()
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("X-File-Name", filepath.Base(filePath))

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	// now the hasher contains full hash
	hashSum := hasher.Sum(nil)

	fmt.Printf("SHA256: %x\n", hashSum)

	if resp.StatusCode == http.StatusOK {
		fmt.Printf("File '%s' uploaded successfully (elapsed: %s)\n",
			filepath.Base(filePath), time.Since(startTime))
		return nil
	}

	body, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("server returned status %d: %s", resp.StatusCode, string(body))
}

func downloadFile(addr, fileName, outPath string) error {
	fileURL := fmt.Sprintf("http://%s/files/%s", addr, url.QueryEscape(fileName))
	fmt.Printf("Downloading file from %s\n", fileURL)
	startTime := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fileURL, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server returned status %d: %s", resp.StatusCode, string(body))
	}

	outFile, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("cannot create output file: %w", err)
	}
	defer outFile.Close()

	hasher := sha256.New()
	writer := io.MultiWriter(outFile, hasher)

	size, err := io.Copy(writer, resp.Body)
	if err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	contentType := resp.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	fmt.Printf("Name: %s\n", fileName)
	fmt.Printf("Size: %d bytes\n", size)
	fmt.Printf("Content-Type: %s\n", contentType)
	fmt.Printf("SHA256: %x\n", hasher.Sum(nil))
	fmt.Printf("File '%s' download successfully (elapsed: %s)\n", fileName, time.Since(startTime).String())

	return nil
}
