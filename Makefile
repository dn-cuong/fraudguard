.PHONY: tidy up down build test ingest loadgen smoke

tidy:
	go mod tidy

up:
	docker compose up -d redis dynamodb

down:
	docker compose down

build:
	mkdir -p bin
	go build -o bin/ingest ./cmd/ingest
	go build -o bin/worker ./cmd/worker
	go build -o bin/loadgen ./cmd/loadgen

test:
	go test ./...

ingest: build
	./bin/ingest -config config/local.yaml -mode score

loadgen: build
	./bin/loadgen -addr http://localhost:8080 -tps 1000 -duration 15s -workers 64

smoke:
	bash scripts/smoke.sh
