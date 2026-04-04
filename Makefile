APPS := api cli cron local log queue reconciler registry worker
BUF ?= npx @bufbuild/buf
BUILD_OPTS ?=
COMPONENTS := $(filter-out cli local, $(APPS))
OUT_DIR ?= bin
CGO_ENABLED ?= 1

API := $(shell find api -name '*.go' 2>/dev/null)
BINARIES := $(addprefix $(OUT_DIR)/vectis-, $(APPS))
INTERNAL := $(shell find internal -name '*.go' 2>/dev/null)

PODMAN_KUBE_SPEC ?= deploy/podman/kube-spec.yaml
PODMAN_NETWORK ?= pasta

VECTIS_DATABASE_DRIVER ?= pgx
VECTIS_POSTGRES_HOST_PORT ?= 15432
VECTIS_DATABASE_DSN ?= postgres://vectis:vectis@127.0.0.1:${VECTIS_POSTGRES_HOST_PORT}/vectis?sslmode=disable

.PHONY: all
all: build

$(OUT_DIR):
	mkdir -p ${@}

$(BINARIES): $(OUT_DIR)/vectis-%: cmd/%/main.go $(API) $(INTERNAL) | $(OUT_DIR)
	CGO_ENABLED=${CGO_ENABLED} go build ${BUILD_OPTS} -o ${@} ./cmd/${*}

.PHONY: build
build: $(BINARIES)

.PHONY: build-container
build-container: CGO_ENABLED = 0
build-container: BUILD_OPTS = -tags=nosqlite -ldflags '-s -w'
build-container: $(BINARIES)

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
	go test ./...

.PHONY: test-integration
test-integration:
	go test -tags=integration ./...

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

# NOTE(garrett): In certain cases like WSL, the standard network support for nftables
# may not work due to missing kernel modules. We use the newer standard of PASTA to
# work around this. Older versions of Podman may need to manually fallback to
# slirp4netns (override PODMAN_NETWORK=slirp4netns).
define PODMAN_DEPLOY_BODY
	podman play kube --replace $(PODMAN_KUBE_SPEC) --network $(PODMAN_NETWORK)
	VECTIS_DATABASE_DRIVER=$(VECTIS_DATABASE_DRIVER) \
	VECTIS_DATABASE_DSN="$(VECTIS_DATABASE_DSN)" \
		$(OUT_DIR)/vectis-cli migrate
endef

.PHONY: deploy-podman deploy-podman-spec
deploy-podman: images-components $(OUT_DIR)/vectis-cli
	$(PODMAN_DEPLOY_BODY)

deploy-podman-spec: $(OUT_DIR)/vectis-cli
	$(PODMAN_DEPLOY_BODY)
