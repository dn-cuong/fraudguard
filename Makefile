.PHONY: tidy up down build test ingest loadgen smoke stack stack-smoke

tidy:
	go mod tidy

up:
	docker compose up -d redis dynamodb

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

# whole pipeline in containers: ingest (kinesis mode) -> LocalStack -> worker
stack:
	docker compose --profile full up -d --build

stack-smoke: stack
	until curl -sf localhost:8080/healthz >/dev/null; do sleep 1; done
	TRIES=150 bash scripts/smoke.sh

down:
	docker compose --profile full down
