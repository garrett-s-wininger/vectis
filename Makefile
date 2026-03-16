APPS := api cli cron local log queue registry worker
BUF ?= npx @bufbuild/buf
BUILD_OPTS ?=
COMPONENTS := $(filter-out cli local, $(APPS))
OUT_DIR ?= bin

API := $(shell find api -name '*.go' 2>/dev/null)
BINARIES := $(addprefix $(OUT_DIR)/vectis-, $(APPS))
INTERNAL := $(shell find internal -name '*.go' 2>/dev/null)

.PHONY: all
all: build

$(OUT_DIR):
	mkdir -p ${@}

$(BINARIES): $(OUT_DIR)/vectis-%: cmd/%/main.go $(API) $(INTERNAL) | $(OUT_DIR)
	go build ${BUILD_OPTS} -o ${@} ./cmd/${*}

.PHONY: build
build: $(BINARIES)

.PHONY: build-static
build-static: BUILD_OPTS = -a -ldflags '-s -w -linkmode external -extldflags "-static"'
build-static: $(BINARIES)

.PHONY: proto
proto:
	rm -rf ./api/gen/
	${BUF} generate

.PHONY: format
format:
	go fix ./...
	go fmt ./...
	go mod tidy

.PHONY: test
test:
	go test -skip '^TestIntegration' ./...

.PHONY: test-integration
test-integration:
	go test -run '^TestIntegration' ./...

.PHONY: test-all
test-all:
	go test ./...

.PHONY: test-race
test-race:
	go test -race ./...

.PHONY: clean
clean:
	rm -rf ${OUT_DIR}

.PHONY: image-full
image-full:
	podman build -t vectis:latest -f build/Containerfile --target all-in-one .

# NOTE(garrett): Slight hack to ensure .PHONY applies to individual image builds while
# getting around the copy-pasta Podman build commands.
image-internal-%:
	podman build -t vectis-${*}:latest -f build/Containerfile --target ${*} .

.PHONY: $(addprefix image-, $(COMPONENTS))
image-api: image-internal-api
image-cron: image-internal-cron
image-queue: image-internal-queue
image-log: image-internal-log
image-registry: image-internal-registry
image-worker: image-internal-worker

.PHONY: images-all
images-all: image-full images-components

.PHONY: images-components
images-components: $(addprefix image-, $(COMPONENTS))
