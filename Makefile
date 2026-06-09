SKIP_WEB_BUILD ?= 0
SKIP_DOCS_ASSETS ?= 0

APPS := api artifact catalog cell-ingress cli cron local log log-forwarder orchestrator queue reconciler registry secrets spiffe worker worker-core

ifeq ($(SKIP_WEB_BUILD),0)
APPS += docs
endif

BUILD_OPTS ?=
COMPONENTS := $(filter-out cli local, $(APPS))
OUT_DIR ?= bin
CGO_ENABLED ?= 1
DOCS_EMBED_DIR := cmd/docs/embedded
DOCS_ASSETS_STAMP := $(DOCS_EMBED_DIR)/.stamp
DOCS_ASSETS_TARGET :=
WEBSITE_SOURCES := $(shell find website \
	\( -path 'website/node_modules' -o -path 'website/.docusaurus' -o -path 'website/build' -o -path 'website/playwright-report' -o -path 'website/test-results' \) -prune \
	-o -type f -print 2>/dev/null)

ifeq ($(SKIP_DOCS_ASSETS),0)
DOCS_ASSETS_TARGET := $(DOCS_ASSETS_STAMP)
endif

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
CMD := $(shell find cmd -name '*.go' 2>/dev/null)
BINARIES := $(addprefix $(OUT_DIR)/vectis-, $(APPS))
NON_DOC_BINARIES := $(filter-out $(OUT_DIR)/vectis-docs,$(BINARIES))
INTERNAL := $(shell find internal -name '*.go' 2>/dev/null)

FORMAL_MODELS := execution reconciliation
JAVA ?= java
TLA_TOOLS_JAR ?= /opt/tla+/tla2tools.jar

GO ?= go
SUITE ?= queue
PERF_ARGS ?=
PERF_BIN ?= $(OUT_DIR)/vectis-perf
PERF_SOURCES := $(shell find scripts/perf -name '*.go' 2>/dev/null)

.PHONY: all
all: build

$(OUT_DIR):
	mkdir -p ${@}

$(OUT_DIR)/vectis-docs: $(CMD) $(API) $(INTERNAL) $(DOCS_ASSETS_TARGET) | $(OUT_DIR)
	CGO_ENABLED=${CGO_ENABLED} go build ${BUILD_OPTS} -ldflags '${LDFLAGS}' -o ${@} ./cmd/docs

$(NON_DOC_BINARIES): $(OUT_DIR)/vectis-%: $(CMD) $(API) $(INTERNAL) | $(OUT_DIR)
	CGO_ENABLED=${CGO_ENABLED} go build ${BUILD_OPTS} -ldflags '${LDFLAGS}' -o ${@} ./cmd/${*}

.PHONY: build
build: $(BINARIES)

$(DOCS_ASSETS_STAMP): $(WEBSITE_SOURCES)
	cd website && npm ci
	cd website && npm run build
	mkdir -p $(DOCS_EMBED_DIR)
	find $(DOCS_EMBED_DIR) -mindepth 1 ! -name .gitkeep -exec rm -rf {} +
	cp -R website/build/. $(DOCS_EMBED_DIR)/
	touch $(DOCS_ASSETS_STAMP)

.PHONY: docs-assets
docs-assets: $(DOCS_ASSETS_STAMP)

.PHONY: podman-grafana-configmaps
podman-grafana-configmaps:
	go run ./deploy/podman/cmd/generate-grafana-configmaps -o deploy/podman/grafana-configmaps.gen.yaml

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

.PHONY: test-lima
test-lima:
	go test ./internal/platform ./internal/job -run TestVirtualMachineIntegration -count=1 -v

.PHONY: test-e2e
test-e2e:
	go test -tags=e2e ./tests/e2e/...

.PHONY: test-postgres-integration
test-postgres-integration:
	go test -tags=integration ./tests/integration/postgres

.PHONY: test-race
test-race:
	go test -race ./...

.PHONY: test-quick
test-quick:
	go test -count=1 -timeout=60s ./internal/... ./cmd/... ./api/... ./sdk/... ./examples/... ./tools/...

.PHONY: test-fault
test-fault:
	go test -count=1 ./internal/faultinject
	go test -count=1 ./internal/artifact ./internal/catalog ./internal/cron ./internal/job ./internal/logforwarder ./internal/queue ./internal/reconciler -run 'Fault|RestoreSkew|QueueDown|QueueRecovery|MinGap|DuplicateDelivery|DBUnavailable'

.PHONY: test-property
test-property:
	go test -count=1 ./internal/queue -run 'Property'
	go test -count=1 ./internal/logforwarder -run 'Property'
	go test -count=1 ./internal/catalog -run 'Property'

.PHONY: deploy-artifacts-test
deploy-artifacts-test:
	go test ./deploy/linux/...

DEPLOY_LINUX_OUT ?= artifacts/deploy/linux

.PHONY: deploy-artifacts-render
deploy-artifacts-render:
	go run ./cmd/cli deploy linux render --output $(DEPLOY_LINUX_OUT)

LIMA_INSTANCE ?= vectis-deploy-smoke
LIMA_TEMPLATE ?= ubuntu-lts

.PHONY: deploy-linux-lima-verify
deploy-linux-lima-verify:
	go run ./cmd/cli deploy linux lima verify --instance $(LIMA_INSTANCE) --template $(LIMA_TEMPLATE)

.PHONY: deploy-linux-lima-clean
deploy-linux-lima-clean:
	go run ./cmd/cli deploy linux lima clean --instance $(LIMA_INSTANCE)

.PHONY: deploy-linux-lima-down
deploy-linux-lima-down:
	go run ./cmd/cli deploy linux lima down --instance $(LIMA_INSTANCE)

.PHONY: deploy-linux-lima-delete
deploy-linux-lima-delete:
	go run ./cmd/cli deploy linux lima delete --instance $(LIMA_INSTANCE)

.PHONY: website-a11y
website-a11y:
	cd website && \
	PLAYWRIGHT_BROWSERS_PATH="$$PWD/node_modules/.cache/ms-playwright" npm ci && \
	PLAYWRIGHT_BROWSERS_PATH="$$PWD/node_modules/.cache/ms-playwright" npx playwright install chromium && \
	PLAYWRIGHT_BROWSERS_PATH="$$PWD/node_modules/.cache/ms-playwright" npm run test:a11y

GOLANGCI_LINT_VERSION ?= v2.6.1
GOVULNCHECK_VERSION ?= v1.1.4

# NOTE(garrett): Match the `go` directive in go.mod to use the same stdlib the module declares.
GO_MOD_GO_VERSION := $(shell awk '/^go /{print $$2; exit}' go.mod)

.PHONY: lint-api-routes
lint-api-routes:
	go run ./tools/vectis-lint ./internal/api

.PHONY: lint
lint: lint-api-routes
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) run ./...

.PHONY: vulncheck
vulncheck:
	GOTOOLCHAIN=go$(GO_MOD_GO_VERSION) go run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

.PHONY: perf
perf: $(PERF_BIN)
	$(PERF_BIN) $(SUITE) $(PERF_ARGS)

.PHONY: perf-compare
perf-compare: $(PERF_BIN)
	$(PERF_BIN) compare --baseline "$(BASELINE)" --current "$(CURRENT)"

$(PERF_BIN): $(PERF_SOURCES) | $(OUT_DIR)
	CGO_ENABLED=${CGO_ENABLED} $(GO) build ${BUILD_OPTS} -o ${@} ./scripts/perf

FUZZTIME ?= 30s

.PHONY: fuzz-api-auth
fuzz-api-auth:
	go test -fuzz=FuzzBearerToken -fuzztime=$(FUZZTIME) ./internal/api
	go test -fuzz=FuzzHashAPIToken -fuzztime=$(FUZZTIME) ./internal/api
	go test -fuzz=FuzzActionForRequest -fuzztime=$(FUZZTIME) ./internal/api/authz

.PHONY: clean
clean:
	rm -rf artifacts/perf/
	rm -rf artifacts/deploy/
	rm -rf ${OUT_DIR}
	rm -rf formal/tla/*_TTrace_*
	rm -rf states/
	rm -rf website/.docusaurus website/build
	find $(DOCS_EMBED_DIR) -mindepth 1 ! -name .gitkeep -exec rm -rf {} +

.PHONY: image-full
image-full:
	podman build -t vectis:latest -f build/Containerfile --target all-in-one .

# NOTE(garrett): Slight hack to ensure .PHONY applies to individual image builds while
# getting around the copy-pasta Podman build commands.
image-internal-%:
	podman build -t vectis-${*}:latest -f build/Containerfile --target ${*} .

.PHONY: $(addprefix image-, $(COMPONENTS))
image-api: image-internal-api
image-artifact: image-internal-artifact
image-catalog: image-internal-catalog
image-cell-ingress: image-internal-cell-ingress
image-cli: image-internal-cli
image-cron: image-internal-cron
image-docs: image-internal-docs
image-spiffe: image-internal-spiffe
image-queue: image-internal-queue
image-log: image-internal-log
image-log-forwarder: image-internal-log-forwarder
image-reconciler: image-internal-reconciler
image-registry: image-internal-registry
image-secrets: image-internal-secrets
image-worker: image-internal-worker
image-worker-core: image-internal-worker-core

.PHONY: images-all
images-all: image-full images-components

.PHONY: images-components
images-components: image-cli $(addprefix image-, $(COMPONENTS))
