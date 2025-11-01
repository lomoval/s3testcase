# s3testcase

A test file storage prototype implemented in Go.  
It consists of a **File Service**, multiple **Storage Services** —
that together provide file upload/download and storage discovery.

The project demonstrates:
- File management via REST API (upload, download)
- Distributed storage coordination via a locator service
- Background cleanup using the outbox pattern
- Integration testing with [Testcontainers](https://testcontainers.com)
- A console client for interacting with the services

---

## 📘 Project Structure

| Component | Path | Description |
|------------|------|-------------|
| **File Service** | `internal/fileservice` | Handles uploads/downloads, communicates with storage nodes |
| **Storage Service** | `internal/storageservice` | Stores actual file data |
| **Storage Locator** | `internal/storagelocator` | Keeps track of available storage services |
| **Storage Layer** | `internal/storage` | Filesystem-based backend used by the storage service |
| **Client App** | `cmd/client` | Command-line tool for uploading/downloading files |

---

## Main Components

### File Service (`internal/fileservice`)

The main service responsible for uploading and downloading files and
interacting with storage nodes (`Storage Service`).

Includes:

* **File Datastore** — provides functions to interact with the database that stores file metadata,
  file locations (the `Storage Services` containing file data), and handles cleanup tasks (outbox pattern).

* **File Cleaner** — removes outdated or unprocessed files.  
  It operates using the outbox pattern:
  periodically checks the datastore for records in the outbox table that indicate files to be cleaned up.

* **File Processor** — implements the logic for uploading and downloading files using worker pools.

---

### Storage Service (`internal/storageservice`)

A service that provides physical storage for file data.  
The `File Service` uses one or more of these services to store actual file contents.

---

### Storage Locator (`internal/storagelocator`)

Acts as a service registry between the `File Service` and multiple `Storage Service` instances.

Includes:

* **Locator** — used by the `File Service` to discover available `Storage Services`
  and retrieve their information (addresses, files size).
* **Live Sender** — used by each `Storage Service` to send live status updates (“live messages”)
  to the locator about its state.

---

### Storage (`internal/storage`)

Implements the API for working with files and managing them on the filesystem.  
The `Storage Service` exposes this functionality via a REST API.

---

## How to Try It

### Integration Tests (via Testcontainers)

There is an integration test (`./test/integrations/basicwork_test.go`) based on [Testcontainers](https://testcontainers.com).  
It performs a complete workflow:
1. Uploads a file
2. Verifies database entries
3. Checks files stored in storage nodes
4. Downloads the file back
5. Prints `fileservice` logs to the console

To run the test:
```bash
make integration-tests
```
or 
```bash
go test ./test/integrations/... -v -run=TestIntegrationUploadDownload
```

### Console App (cmd/client/main.go)

A simple console client for uploading and downloading files.

To use app build Docker images and start the services with:
```
make up
```
Or manually:
```
docker build -f build/Dockerfile.fileservice --build-arg SERVICE_NAME=fileservice -t fileservice:0.0.1 .
docker build -f build/Dockerfile.storageservice --build-arg SERVICE_NAME=storageservice -t storageservice:0.0.1 .
docker compose -f ./build/docker-compose.yml up -d
```

The `File Service` exposes port 8100, which can be used to upload or download files.

Upload a file
```
go run cmd/client/main.go upload -f <path-to-file>
```

Download a file
```
go run cmd/client/main.go download -n <uploaded-filename> -o <path-to-save>
```

Example:
```
go run ./cmd/client/main.go upload -f D:\Temp\gofunc.mp4
Uploading file: gofunc.mp4
Size: 801,339,766 bytes
Content-Type: video/mp4
SHA256: 64328d5b0d2e4461658c49ddd611efeca51b53257599b5239d92280fd9b1415a
Sending request to http://127.0.0.1:8100/upload
File gofunc.mp4 uploaded successfully (elapsed: 1.79s)

go run ./cmd/client/main.go download -n gofunc.mp4 -o D:\Temp\gofunc_out.mp4
Downloading file from http://127.0.0.1:8100/files/gofunc.mp4
Response received after 624ms
Downloaded file: gofunc.mp4
Size: 801,339,766 bytes
Content-Type: video/mp4
SHA256: 64328d5b0d2e4461658c49ddd611efeca51b53257599b5239d92280fd9b1415a
File gofunc.mp4 downloaded successfully (elapsed: 3.98s)
```

To cleanup stop all services and remove images, volumes, and networks:

```
make down-all
```

Or manually:
```
docker compose -f ./build/docker-compose.yml down --rmi all -v
```