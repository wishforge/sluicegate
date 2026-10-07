.PHONY: deps test vet build db-up db-down db-reset e2e chaos load-test load-matrix bottleneck-matrix all

deps:
	GOTOOLCHAIN=local go mod download

test: deps
	GOTOOLCHAIN=local go test ./...

vet: deps
	GOTOOLCHAIN=local go vet ./...

build: deps
	mkdir -p bin
	GOTOOLCHAIN=local go build -o bin/migration-controller ./cmd/migration-controller
	GOTOOLCHAIN=local go build -o bin/source-agent ./cmd/source-agent
	GOTOOLCHAIN=local go build -o bin/target-agent ./cmd/target-agent
	GOTOOLCHAIN=local go build -o bin/workload-runner ./cmd/workload-runner
	GOTOOLCHAIN=local go build -o bin/loadtest ./cmd/loadtest

DB_MAX_CONNS ?= 32
DB_MIN_CONNS ?= 4

db-up:
	./scripts/db-start.sh

db-down:
	./scripts/db-stop.sh

db-reset:
	DATABASE_URL="$${DATABASE_URL:-postgres://migration:migration@127.0.0.1:55432/migration?sslmode=disable}" ./scripts/db-reset.sh

e2e:
	DB_MAX_CONNS=$(DB_MAX_CONNS) ./scripts/e2e.sh

chaos:
	DB_MAX_CONNS=$(DB_MAX_CONNS) ./scripts/chaos.sh

bottleneck-matrix:
	DB_MAX_CONNS=$(DB_MAX_CONNS) DB_MIN_CONNS=$(DB_MIN_CONNS) ./scripts/bottleneck-matrix.sh

all: test vet build

load-test:
	DB_MAX_CONNS=$(DB_MAX_CONNS) DB_MIN_CONNS=$(DB_MIN_CONNS) ./scripts/load-test.sh

load-matrix:
	DB_MAX_CONNS=$(DB_MAX_CONNS) DB_MIN_CONNS=$(DB_MIN_CONNS) ./scripts/load-matrix.sh
