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
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"s3testcase/internal/fileservice/db"
)

const (
	postgresImage            = "postgres:18"
	fileserviceImage         = "fileservice:0.0.1"
	storageserviceImage      = "storageservice:0.0.1"
	fileserviceDockerfile    = "build/Dockerfile.fileservice"
	storageserviceDockerfile = "build/Dockerfile.storageservice"
	dockerContext            = "../../"
)

type testContainers struct {
	net                *testcontainers.DockerNetwork
	postgres           *containerData
	fileservice        *containerData
	storageservices    []*containerData
	storageservicesMap map[string]*containerData
}

type containerData struct {
	C           testcontainers.Container
	Host        string
	Port        int
	URL         string
	ServiceUUID string
}

type file struct {
	ID          uint64
	UUID        string
	FileName    string
	ContentType string
	Size        int64
	ProcessedAt *time.Time
	Hash        string
}

type fileLocation struct {
	ID           uint64
	FileUUID     string
	LocationUUID string
	ContentType  string
	Size         int64
	PartNumber   int
	ProcessedAt  *time.Time
}

func TestIntegrationUploadDownload(t *testing.T) {
	containers, err := startTestContainers(t, 6)
	require.NoError(t, err)

	sqlDB, err := db.InitDB(containers.postgres.URL)
	require.NoError(t, err)

	startTime := time.Now().UTC()
	fileName := "test.txt"
	size := int64(1073741824) // 1 ГБ
	contentType := "text/plain"

	uploadURL := "http://localhost:" + strconv.Itoa(containers.fileservice.Port) + "/upload"
	downloadURL := "http://localhost:" + strconv.Itoa(containers.fileservice.Port) + "/files/" + fileName

	defer func() {
		printContainerLogs(t.Context(), containers.fileservice.C)
	}()
	reader := NewHashingGeneratorReader(size)
	sc, err := uploadToServer(t.Context(), uploadURL, fileName, contentType, size, reader)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, sc)

	query := `SELECT id, uuid,  file_name, content_type, size, processed_at FROM files;`
	rows, err := sqlDB.Query(query)
	if err != nil {
		require.NoError(t, err)
	}
	defer rows.Close()

	files := make([]file, 0)
	for rows.Next() {
		var f file
		err := rows.Scan(&f.ID, &f.UUID, &f.FileName, &f.ContentType, &f.Size, &f.ProcessedAt)
		if err != nil {
			require.NoError(t, err)
		}
		files = append(files, f)
	}
	require.Len(t, files, 1)

	f := files[0]
	require.Equal(t, uint64(1), f.ID)
	require.NoError(t, uuid.Validate(f.UUID))
	require.Equal(t, fileName, f.FileName)
	require.Equal(t, size, f.Size)
	require.Equal(t, contentType, f.ContentType)
	require.NotNil(t, f.ProcessedAt)
	require.True(t, startTime.Before(*f.ProcessedAt))

	var downloadFile *file
	sc, downloadFile, err = downloadFromServer(t.Context(), downloadURL)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, sc)

	require.Equal(t, size, downloadFile.Size)
	require.Equal(t, contentType, downloadFile.ContentType)
	require.Equal(t, reader.Sum(), downloadFile.Hash)

	query = `
		SELECT id, file_uuid, location_uuid, size, part_number, processed_at 
		FROM file_locations
		ORDER BY part_number;
	`
	rows, err = sqlDB.Query(query)
	if err != nil {
		require.NoError(t, err)
	}
	defer rows.Close()

	var partsSize int64
	locations := make([]fileLocation, 0)
	for rows.Next() {
		var location fileLocation
		err := rows.Scan(
			&location.ID,
			&location.FileUUID,
			&location.LocationUUID,
			&location.Size,
			&location.PartNumber,
			&location.ProcessedAt,
		)
		require.NoError(t, err)
		partsSize += location.Size
		locations = append(locations, location)
	}
	require.Len(t, locations, 6)
	require.Equal(t, size, partsSize)
	require.NotNil(t, f.ProcessedAt)
	require.Greater(t, *f.ProcessedAt, startTime)
	for _, location := range locations {
		ss := containers.storageservicesMap[location.LocationUUID]
		require.NotNil(t, ss)
		exists, err := FileExists(
			t.Context(),
			ss.C,
			fmt.Sprintf("/tmp/ss/data/%s-%d", location.FileUUID, location.PartNumber),
		)
		require.NoError(t, err)
		require.True(t, exists)
		size, err := FileSize(
			t.Context(),
			ss.C,
			fmt.Sprintf("/tmp/ss/data/%s-%d", location.FileUUID, location.PartNumber),
		)
		require.NoError(t, err)
		require.Equal(t, location.Size, size)
	}
}

func startTestContainers(t *testing.T, numStorages int) (*testContainers, error) {
	t.Helper()
	ctx := t.Context()
	tc := &testContainers{storageservicesMap: make(map[string]*containerData)}
	var err error

	tc.net, err = network.New(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { tc.net.Remove(t.Context()) })

	tc.postgres, err = runPostgres(t.Context(), tc.net)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { testcontainers.TerminateContainer(tc.postgres.C) })

	for i := 0; i < numStorages; i++ {
		ss, err := runStorageService(ctx, tc.net, i+1, map[string]string{})
		if err != nil {
			return nil, err
		}
		tc.storageservicesMap[ss.ServiceUUID] = ss
		tc.storageservices = append(tc.storageservices, ss)
		t.Cleanup(func(i int) func() {
			return func() {
				testcontainers.TerminateContainer(tc.storageservices[i].C)
			}
		}(i))
	}

	tc.fileservice, err = runFileService(ctx, tc.net, map[string]string{})
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { testcontainers.TerminateContainer(tc.fileservice.C) })

	return tc, nil
}

func runPostgres(ctx context.Context, net *testcontainers.DockerNetwork) (*containerData, error) {
	req := testcontainers.ContainerRequest{
		Name:         "postgres",
		Image:        postgresImage,
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "postgres",
			"POSTGRES_PASSWORD": "password",
			"POSTGRES_DB":       "test_db",
		},
		WaitingFor: wait.ForAll(
			wait.ForLog("database system is ready to accept connections"),
			wait.ForListeningPort("5432/tcp"),
		),
		Networks:       []string{net.Name},
		NetworkAliases: map[string][]string{net.Name: {"postgres"}},
	}

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}
	mappedPort, err := c.MappedPort(ctx, nat.Port("5432"))
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, err
	}
	hostIP, err := c.Host(ctx)
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, err
	}
	dbURL := fmt.Sprintf(
		"postgresql://%s:%d/%s?sslmode=disable&user=%s&password=%s",
		hostIP,
		mappedPort.Int(),
		"test_db",
		"postgres",
		"password",
	)
	return &containerData{
		C:    c,
		Host: hostIP,
		Port: mappedPort.Int(),
		URL:  dbURL,
	}, nil
}

func runFileService(
	ctx context.Context,
	net *testcontainers.DockerNetwork,
	testEnv map[string]string,
) (*containerData, error) {
	env := map[string]string{
		// DB
		"DB_HOST":     "postgres",
		"DB_PORT":     "5432",
		"DB_USER":     "postgres",
		"DB_PASSWORD": "password",
		"DB_NAME":     "test_db",
		"DB_SSLMODE":  "disable",
		// Service
		"LISTEN_ADDR":                 "0.0.0.0:8180",
		"STORAGE_LOCATOR_LISTEN_ADDR": "0.0.0.0:8190",
		"MIGRATIONS_DIR":              "/db/migrations",
		"DOWNLOAD_DIR":                "/tmp/fs/d",
		"UPLOAD_DIR":                  "/tmp/fs/u",
	}
	for k, v := range testEnv {
		env[k] = v
	}
	strPort := strings.Split(env["LISTEN_ADDR"], ":")[1]
	req := testcontainers.ContainerRequest{
		Name: "fileservice",
		// Image: fileserviceImage,
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    dockerContext,
			Dockerfile: fileserviceDockerfile,
		},
		ExposedPorts:   []string{strPort},
		Env:            env,
		WaitingFor:     wait.ForListeningPort(nat.Port(strPort)),
		Cmd:            []string{"./fileservice"},
		Tmpfs:          map[string]string{"/tmp/fs": "rw"},
		Networks:       []string{net.Name},
		NetworkAliases: map[string][]string{net.Name: {"fileservice"}},
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}
	mappedPort, err := c.MappedPort(ctx, nat.Port(strPort))
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, err
	}
	hostIP, err := c.Host(ctx)
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, err
	}

	return &containerData{
		C:    c,
		Host: hostIP,
		Port: mappedPort.Int(),
	}, nil
}

func runStorageService(
	ctx context.Context,
	net *testcontainers.DockerNetwork,
	num int,
	testEnv map[string]string,
) (*containerData, error) {
	strNum := strconv.Itoa(num)
	env := map[string]string{
		// Service
		"LISTEN_ADDR":     "0.0.0.0:820" + strNum,
		"ADVERTISED_ADDR": "storageservice" + strNum + ":820" + strNum,
		"LOCATOR_URL":     "http://fileservice:8190",
		"SERVICE_UUID":    "00000000-0000-0000-0000-00000000000" + strNum,
		"STORAGE_DIR":     "/tmp/ss/",
	}
	for k, v := range testEnv {
		env[k] = v
	}
	strPort := strings.Split(env["LISTEN_ADDR"], ":")[1]
	req := testcontainers.ContainerRequest{
		Name: "storageservice" + strNum,
		// Image: fileserviceImage,
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    dockerContext,
			Dockerfile: storageserviceDockerfile,
		},
		ExposedPorts: []string{strPort},
		Env:          env,
		// WaitingFor:     wait.ForListeningPort(nat.Port(strPort)),
		Cmd:            []string{"./storageservice"},
		Tmpfs:          map[string]string{"/tmp/ss/": "rw"},
		Networks:       []string{net.Name},
		NetworkAliases: map[string][]string{net.Name: {"storageservice" + strNum}},
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}
	hostIP, err := c.Host(ctx)
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, err
	}
	return &containerData{
		C:           c,
		Host:        hostIP,
		ServiceUUID: env["SERVICE_UUID"],
	}, nil
}

func uploadToServer(
	ctx context.Context,
	url string,
	fileName string,
	contentType string,
	size int64,
	streamer io.Reader,
) (int, error) {
	client := &http.Client{}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, streamer)
	if err != nil {
		return -1, err
	}

	request.ContentLength = size
	request.Header.Set("Content-Type", contentType)
	request.Header.Set("X-File-Name", fileName)

	resp, err := client.Do(request)
	if err != nil {
		return resp.StatusCode, err
	}
	defer resp.Body.Close()

	return resp.StatusCode, nil
}

func downloadFromServer(ctx context.Context, url string) (int, *file, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return -1, nil, err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return -1, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, nil, nil
	}

	var file file
	file.FileName = resp.Header.Get("X-File-Name")
	file.Size = resp.ContentLength
	file.ContentType = resp.Header.Get("Content-Type")
	file.Hash, err = HashHTTPResponseBody(resp)
	return resp.StatusCode, &file, err
}
