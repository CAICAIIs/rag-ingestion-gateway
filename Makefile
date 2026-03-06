.PHONY: build run test lint tidy migrate docker-up docker-down clean

BINARY := bin/gateway

build:
	go build -o $(BINARY) ./cmd/gateway

run: build
	./$(BINARY)

test:
	go test ./...

tidy:
	go mod tidy

lint:
	go vet ./...

migrate:
	bash scripts/migrate.sh

docker-up:
	docker compose -f scripts/docker-compose.dev.yml up --build -d

docker-down:
	docker compose -f scripts/docker-compose.dev.yml down

clean:
	rm -rf bin/
