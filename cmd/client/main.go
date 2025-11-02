package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"net/http"
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
	if _, err := io.Copy(hasher, f); err != nil {
		return fmt.Errorf("cannot hash file: %w", err)
	}
	hashSum := hasher.Sum(nil)

	fmt.Printf("Uploading file: %s\n", filepath.Base(filePath))
	fmt.Printf("Size: %d bytes\n", stat.Size())
	fmt.Printf("Content-Type: %s\n", contentType)
	fmt.Printf("SHA256: %x\n", hashSum)

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("cannot seek file: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	startTime := time.Now()
	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("http://%s/upload", addr), f)
	if err != nil {
		return fmt.Errorf("cannot create request: %w", err)
	}
	req.ContentLength = stat.Size()
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("X-File-Name", filepath.Base(filePath))

	fmt.Printf("Sending request to %s\n", req.URL.String())
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		fmt.Printf(
			"File %s uploaded successfully (elapsed: %s)\n",
			filepath.Base(filePath),
			time.Since(startTime).String(),
		)
		return nil
	}

	body, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("server returned status %d: %s", resp.StatusCode, string(body))
}

func downloadFile(addr, fileName, outPath string) error {
	url := fmt.Sprintf("http://%s/files/%s", addr, fileName)
	fmt.Printf("Downloading file from %s\n", url)
	startTime := time.Now()
	resp, err := http.Get(url) //nolint:gosec,G107
	if err != nil {
		return fmt.Errorf("failed to GET file: %w", err)
	}
	defer resp.Body.Close()

	fmt.Printf("Get response after %s\n", time.Since(startTime))
	if resp.StatusCode != http.StatusOK {
		fmt.Printf("Server returning data")
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
	fmt.Printf("Downloaded file: %s\n", fileName)
	fmt.Printf("Size: %d bytes\n", size)
	fmt.Printf("Content-Type: %s\n", contentType)
	fmt.Printf("SHA256: %x\n", hasher.Sum(nil))
	fmt.Printf("File %s download successfully (elapsed: %s)\n", fileName, time.Since(startTime).String())

	return nil
}
