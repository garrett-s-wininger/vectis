APPS := api cli cron local log queue reconciler registry worker
BUILD_OPTS ?=
COMPONENTS := $(filter-out cli local, $(APPS))
OUT_DIR ?= bin
CGO_ENABLED ?= 1

PROTOC ?= protoc
PROTOC_GEN_GO ?= $(shell go env GOPATH)/bin/protoc-gen-go
PROTOC_GEN_GO_GRPC ?= $(shell go env GOPATH)/bin/protoc-gen-go-grpc

VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT := $(shell git rev-parse --short=12 HEAD 2>/dev/null || echo unknown)
BUILD_DATE := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS ?=
LDFLAGS += -X vectis/internal/version.Version=$(VERSION)
LDFLAGS += -X vectis/internal/version.Commit=$(COMMIT)
LDFLAGS += -X vectis/internal/version.BuildDate=$(BUILD_DATE)

API := $(shell find api -name '*.go' 2>/dev/null)
BINARIES := $(addprefix $(OUT_DIR)/vectis-, $(APPS))
INTERNAL := $(shell find internal -name '*.go' 2>/dev/null)

FORMAL_MODELS := execution reconciliation
JAVA ?= java
TLA_TOOLS_JAR ?= /opt/tla+/tla2tools.jar

.PHONY: all
all: build

$(OUT_DIR):
	mkdir -p ${@}

$(BINARIES): $(OUT_DIR)/vectis-%: cmd/%/main.go $(API) $(INTERNAL) | $(OUT_DIR)
	CGO_ENABLED=${CGO_ENABLED} go build ${BUILD_OPTS} -ldflags '${LDFLAGS}' -o ${@} ./cmd/${*}

.PHONY: build
build: $(BINARIES)

.PHONY: build-container
build-container: CGO_ENABLED = 0
build-container: BUILD_OPTS = -tags=nosqlite
build-container: LDFLAGS += -s -w
build-container: $(BINARIES)

.PHONY: proto
proto:
	rm -rf ./api/gen/
	mkdir -p ./api/gen/go/
	${PROTOC} -I ./api/proto \
		--plugin=protoc-gen-go=${PROTOC_GEN_GO} \
		--plugin=protoc-gen-go-grpc=${PROTOC_GEN_GO_GRPC} \
		--go_out=./api/gen/go/ --go_opt=paths=source_relative \
		--go-grpc_out=./api/gen/go/ --go-grpc_opt=paths=source_relative \
		./api/proto/*.proto

.PHONY: ci-quick
ci-quick:
	@sh .vectis/ci-quick.sh

formal-verification-%: formal/tla/%.tla
	${JAVA} -jar $(TLA_TOOLS_JAR) -workers auto formal/tla/${*}.tla -config formal/tla/${*}.cfg

.PHONY: formal-verification
formal-verification: $(addprefix formal-verification-, $(FORMAL_MODELS))

.PHONY: format
format:
	go fix ./...
	go fmt ./...
	go mod tidy

.PHONY: test
test:
	go test ./...

.PHONY: test-integration
test-integration:
	go test -tags=integration ./...

.PHONY: test-postgres-integration
test-postgres-integration:
	go test -tags=integration ./tests/integration/postgres

.PHONY: test-race
test-race:
	go test -race ./...

.PHONY: test-quick
test-quick:
	go test -count=1 -timeout=60s ./internal/... ./cmd/... ./api/...

.PHONY: website-a11y
website-a11y:
	cd website && \
	PLAYWRIGHT_BROWSERS_PATH="$$PWD/node_modules/.cache/ms-playwright" npm ci && \
	PLAYWRIGHT_BROWSERS_PATH="$$PWD/node_modules/.cache/ms-playwright" npx playwright install chromium && \
	PLAYWRIGHT_BROWSERS_PATH="$$PWD/node_modules/.cache/ms-playwright" npm run test:a11y

GOLANGCI_LINT_VERSION ?= v2.6.1
GOVULNCHECK_VERSION ?= v1.1.4

# NOTE(garrett):Match the `go` directive in go.mod to analyz the same stdlib the module declares.
GO_MOD_GO_VERSION := $(shell awk '/^go /{print $$2; exit}' go.mod)

.PHONY: lint
lint:
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) run ./...

.PHONY: vulncheck
vulncheck:
	GOTOOLCHAIN=go$(GO_MOD_GO_VERSION) go run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

.PHONY: capacity-benchmark
capacity-benchmark:
	sh scripts/capacity_benchmark.sh

FUZZTIME ?= 30s

.PHONY: fuzz-api-auth
fuzz-api-auth:
	go test -fuzz=FuzzBearerToken -fuzztime=$(FUZZTIME) ./internal/api
	go test -fuzz=FuzzHashAPIToken -fuzztime=$(FUZZTIME) ./internal/api
	go test -fuzz=FuzzActionForRequest -fuzztime=$(FUZZTIME) ./internal/api/authz

.PHONY: clean
clean:
	rm -rf ${OUT_DIR}
	rm -rf formal/tla/*_TTrace_*
	rm -rf states/

.PHONY: image-full
image-full:
	podman build -t vectis:latest -f build/Containerfile --target all-in-one .

# NOTE(garrett): Slight hack to ensure .PHONY applies to individual image builds while
# getting around the copy-pasta Podman build commands.
image-internal-%:
	podman build -t vectis-${*}:latest -f build/Containerfile --target ${*} .

.PHONY: $(addprefix image-, $(COMPONENTS))
image-api: image-internal-api
image-cli: image-internal-cli
image-cron: image-internal-cron
image-queue: image-internal-queue
image-log: image-internal-log
image-reconciler: image-internal-reconciler
image-registry: image-internal-registry
image-worker: image-internal-worker

.PHONY: images-all
images-all: image-full images-components

.PHONY: images-components
images-components: image-cli $(addprefix image-, $(COMPONENTS))
