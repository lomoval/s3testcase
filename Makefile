.PHONY: clean build push build-fs build-ss build-db clean-db integration-tests \
        up down down-all restart logs logs-fileservice logs-storage clean-volumes \
        build-compose rebuild ps

COMPOSE_FILE := ./build/docker-compose.yml

### Build

build-fs:
	docker build -f build/Dockerfile.fileservice --build-arg SERVICE_NAME=fileservice -t fileservice:0.0.1 .

build-ss:
	docker build -f build/Dockerfile.storageservice --build-arg SERVICE_NAME=storageservice -t storageservice:0.0.1 .

build: build-fs build-ss

clean-fs:
	-docker rmi -f fileservice:0.0.1

clean-ss:
	-docker rmi -f storageservice:0.0.1

clean: clean-fs clean-ss

### Tests

unit-tests:
	go test -v -run='^Test[^I]' ./...

integration-tests:
	go test ./test/integrations/... -v -run=TestIntegrationUploadDownload

### Docker Compose

up:
	docker build -f build/Dockerfile.fileservice --build-arg SERVICE_NAME=fileservice -t fileservice:0.0.1 .
	docker build -f build/Dockerfile.storageservice --build-arg SERVICE_NAME=storageservice -t storageservice:0.0.1 .
	docker compose -f $(COMPOSE_FILE) up -d

down:
	docker compose -f $(COMPOSE_FILE) down

down-all:
	docker compose -f $(COMPOSE_FILE) down --rmi all -v

restart:
	$(MAKE) down
	$(MAKE) up

logs:
	docker compose -f $(COMPOSE_FILE) logs -f

logs-fileservice:
	docker compose -f $(COMPOSE_FILE) logs -f fileservice

logs-storage:
ifndef SERVICE
	$(error Set SERVICE: make logs-storage SERVICE=3)
endif
	docker compose -f $(COMPOSE_FILE) logs -f storageservice$(SERVICE)

clean-volumes:
	docker compose -f $(COMPOSE_FILE) down -v

build-compose:
	docker compose -f $(COMPOSE_FILE) build

rebuild:
	$(MAKE) build-compose
	$(MAKE) up

ps:
	docker compose -f $(COMPOSE_FILE) ps
