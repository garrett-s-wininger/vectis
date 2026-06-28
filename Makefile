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

FORMAL_MODELS := execution reconciliation terminalization
JAVA ?= java
TLA_TOOLS_JAR ?= /opt/tla+/tla2tools.jar

GO ?= go
SUITE ?= queue
PERF_ARGS ?=
PERF_BIN ?= $(OUT_DIR)/vectis-perf
PERF_SOURCES := $(shell find scripts/perf -name '*.go' 2>/dev/null)
RELEASE_READINESS_PROFILE ?= local
RELEASE_READINESS_CHECKS ?=
RELEASE_READINESS_ARGS ?=

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

.PHONY: test-e2e-deploy-linux
test-e2e-deploy-linux:
	PACKER_VM_PREP_VERSION=$(PACKER_VM_PREP_VERSION) go test -tags=e2e ./tests/e2e/deploy/linux -count=1 -v

.PHONY: test-postgres-integration
test-postgres-integration:
	go test -tags=integration ./tests/integration/postgres

.PHONY: test-race
test-race:
	go test -race ./...

.PHONY: test-quick
test-quick:
	go test -count=1 -timeout=60s ./internal/... ./cmd/... ./api/... ./sdk/... ./examples/... ./tools/...

.PHONY: release-local-validate
release-local-validate:
	$(MAKE) test-quick
	$(MAKE) deploy-artifacts-test
	$(MAKE) test-package
	$(MAKE) build

.PHONY: release-readiness-report
release-readiness-report:
	$(GO) run ./tools/release-readiness --profile "$(RELEASE_READINESS_PROFILE)" $(if $(RELEASE_READINESS_CHECKS),--checks "$(RELEASE_READINESS_CHECKS)",) $(RELEASE_READINESS_ARGS)

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

PACKAGE_OUT ?= artifacts/packages
PACKAGE_BUILD_DIR ?= $(PACKAGE_OUT)/build
PACKAGE_ARCH ?= $(shell go env GOARCH)
PACKAGE_ARCHES ?= amd64 arm64
PACKAGE_SERVICE_APPS ?= api artifact catalog cell-ingress cron docs log log-forwarder orchestrator queue reconciler registry secrets spiffe worker worker-core
PACKAGE_LOCAL_ARCHES ?= $(PACKAGE_ARCH)
PACKAGE_LOCAL_APPS ?= local api artifact catalog cell-ingress cron docs log orchestrator queue reconciler registry secrets worker worker-core
PACKAGE_LOCAL_ALLOW_CROSS_CGO ?= 0
PACKAGE_LOCAL_MAKE ?= make
PACKER ?= packer
PACKER_VM_PREP_VERSION ?= 1
VM_PROVIDER ?= auto
VM_DOCTOR_TIMEOUT ?= 10m
PACKER_DEPLOY_SMOKE_DIR ?= build/packer/deploy-smoke
PACKER_DEPLOY_SMOKE_INSTANCE ?= vectis-deploy-smoke
PACKER_DEPLOY_SMOKE_TEMPLATE ?= ubuntu-lts
PACKER_DEPLOY_SMOKE_CPUS ?= 2
PACKER_DEPLOY_SMOKE_MEMORY ?= 2
PACKER_DEPLOY_SMOKE_DISK ?= 30
PACKER_DEPLOY_SMOKE_STOP ?= true
PACKER_DEPLOY_SMOKE_LIMA_BIN ?= limactl
PACKER_PACKAGE_BUILDER_DIR ?= build/packer/package-builder
PACKER_PACKAGE_BUILDER_INSTANCE ?= vectis-package-builder
PACKER_PACKAGE_BUILDER_TEMPLATE ?= ubuntu-lts
PACKER_PACKAGE_BUILDER_GO_VERSION ?= $(GO_MOD_GO_VERSION)
PACKER_PACKAGE_BUILDER_GO_SHA256 ?=
PACKER_PACKAGE_BUILDER_CPUS ?= 4
PACKER_PACKAGE_BUILDER_MEMORY ?= 4
PACKER_PACKAGE_BUILDER_DISK ?= 60
PACKER_PACKAGE_BUILDER_STOP ?= true
PACKER_PACKAGE_BUILDER_LIMA_BIN ?= $(if $(PACKAGE_LOCAL_VM_PROVIDER_PATH),$(PACKAGE_LOCAL_VM_PROVIDER_PATH),limactl)
PACKER_PACKAGE_BUILDER_WORKSPACE_ROOT ?= /var/tmp/vectis-package-local-workspaces
PACKER_PACKAGE_BUILDER_CACHE_ROOT ?= /var/tmp/vectis-package-local-cache
PACKER_PACKAGE_SMOKE_DIR ?= build/packer/package-smoke
PACKER_PACKAGE_SMOKE_CPUS ?= 2
PACKER_PACKAGE_SMOKE_MEMORY ?= 2
PACKER_PACKAGE_SMOKE_DISK ?= 30
PACKER_PACKAGE_SMOKE_STOP ?= true
PACKER_PACKAGE_SMOKE_LIMA_BIN ?= limactl
PACKER_PACKAGE_DEB_SMOKE_INSTANCE ?= vectis-package-smoke
PACKER_PACKAGE_DEB_SMOKE_TEMPLATE ?= ubuntu-lts
PACKER_PACKAGE_RPM_SMOKE_INSTANCE ?= vectis-package-rpm-smoke
PACKER_PACKAGE_RPM_SMOKE_TEMPLATE ?= fedora
PACKAGE_LOCAL_VM_PROVIDER ?= auto
PACKAGE_LOCAL_VM_PROVIDER_PATH ?=
PACKAGE_LOCAL_VM_INSTANCE ?= $(PACKER_PACKAGE_BUILDER_INSTANCE)
PACKAGE_LOCAL_VM_TIMEOUT ?= 30m
PACKAGE_LOCAL_VM_WORKSPACE_ROOT ?= $(PACKER_PACKAGE_BUILDER_WORKSPACE_ROOT)
PACKAGE_LOCAL_VM_CACHE_ROOT ?= $(PACKER_PACKAGE_BUILDER_CACHE_ROOT)
PACKAGE_LOCAL_VM_KEEP ?= 0
PACKAGE_LOCAL_VM_PRESERVE_ENV ?= 0
PACKAGE_LOCAL_VM_GO ?= go
packer_deploy_smoke_vars = -var 'instance=$(PACKER_DEPLOY_SMOKE_INSTANCE)' -var 'base_template=$(PACKER_DEPLOY_SMOKE_TEMPLATE)' -var 'prep_version=$(PACKER_VM_PREP_VERSION)' -var 'cpus=$(PACKER_DEPLOY_SMOKE_CPUS)' -var 'memory=$(PACKER_DEPLOY_SMOKE_MEMORY)' -var 'disk=$(PACKER_DEPLOY_SMOKE_DISK)' -var 'stop_after_prepare=$(PACKER_DEPLOY_SMOKE_STOP)' -var 'lima_bin=$(PACKER_DEPLOY_SMOKE_LIMA_BIN)'
packer_package_builder_optional_sha = $(if $(strip $(PACKER_PACKAGE_BUILDER_GO_SHA256)),-var 'go_sha256=$(PACKER_PACKAGE_BUILDER_GO_SHA256)',)
packer_package_builder_vars = -var 'instance=$(PACKER_PACKAGE_BUILDER_INSTANCE)' -var 'base_template=$(PACKER_PACKAGE_BUILDER_TEMPLATE)' -var 'go_version=$(PACKER_PACKAGE_BUILDER_GO_VERSION)' $(packer_package_builder_optional_sha) -var 'prep_version=$(PACKER_VM_PREP_VERSION)' -var 'cpus=$(PACKER_PACKAGE_BUILDER_CPUS)' -var 'memory=$(PACKER_PACKAGE_BUILDER_MEMORY)' -var 'disk=$(PACKER_PACKAGE_BUILDER_DISK)' -var 'stop_after_prepare=$(PACKER_PACKAGE_BUILDER_STOP)' -var 'lima_bin=$(PACKER_PACKAGE_BUILDER_LIMA_BIN)' -var 'workspace_root=$(PACKER_PACKAGE_BUILDER_WORKSPACE_ROOT)' -var 'cache_root=$(PACKER_PACKAGE_BUILDER_CACHE_ROOT)'
packer_package_smoke_vars = -var 'profile=$(1)' -var 'instance=$(2)' -var 'base_template=$(3)' -var 'prep_version=$(PACKER_VM_PREP_VERSION)' -var 'cpus=$(PACKER_PACKAGE_SMOKE_CPUS)' -var 'memory=$(PACKER_PACKAGE_SMOKE_MEMORY)' -var 'disk=$(PACKER_PACKAGE_SMOKE_DISK)' -var 'stop_after_prepare=$(PACKER_PACKAGE_SMOKE_STOP)' -var 'lima_bin=$(PACKER_PACKAGE_SMOKE_LIMA_BIN)'
package_deb_arch = $(if $(filter x86_64,$(1)),amd64,$(if $(filter aarch64,$(1)),arm64,$(if $(filter 386,$(1)),i386,$(1))))
package_rpm_arch = $(if $(filter amd64,$(1)),x86_64,$(if $(filter arm64,$(1)),aarch64,$(1)))
package_service_bins = $(addprefix $(PACKAGE_BUILD_DIR)/linux-$(1)/vectis-,$(PACKAGE_SERVICE_APPS))
package_local_bin_dir = $(PACKAGE_BUILD_DIR)/linux-$(1)-local/bin
package_local_bins = $(addprefix $(call package_local_bin_dir,$(1))/vectis-,$(PACKAGE_LOCAL_APPS))
package_common_inputs = --input linux-artifacts=$(PACKAGE_LINUX_ARTIFACTS)
package_service_inputs = --input linux-artifacts=$(PACKAGE_LINUX_ARTIFACTS) --input vectis-$(2)=$(PACKAGE_BUILD_DIR)/linux-$(1)/vectis-$(2)
package_local_inputs = --input vectis-local-wrapper=$(PACKAGE_LOCAL_WRAPPER) --input vectis-local-binaries=$(call package_local_bin_dir,$(1))
package_local_dispatch_env = --env 'PACKAGE_OUT=$(PACKAGE_OUT)' --env 'PACKAGE_BUILD_DIR=$(PACKAGE_BUILD_DIR)' --env 'PACKAGE_VERSION=$(PACKAGE_VERSION)' --env 'PACKAGE_RELEASE=$(PACKAGE_RELEASE)' --env 'PACKAGE_ARCH=$(2)' --env 'PACKAGE_LOCAL_ARCHES=$(2)' --env 'PACKAGE_LOCAL_APPS=$(PACKAGE_LOCAL_APPS)'
package_local_dispatch = PACKER_VM_PREP_VERSION='$(PACKER_VM_PREP_VERSION)' $(GO) run ./deploy/package/cmd/build-local --format $(1) --arch $(2) --workdir '$(CURDIR)' --make '$(PACKAGE_LOCAL_MAKE)' --provider '$(PACKAGE_LOCAL_VM_PROVIDER)' --provider-path '$(PACKAGE_LOCAL_VM_PROVIDER_PATH)' --instance '$(PACKAGE_LOCAL_VM_INSTANCE)' --timeout '$(PACKAGE_LOCAL_VM_TIMEOUT)' --allow-cross-cgo=$(PACKAGE_LOCAL_ALLOW_CROSS_CGO) --keep-vm=$(PACKAGE_LOCAL_VM_KEEP) --vm-workspace-root '$(PACKAGE_LOCAL_VM_WORKSPACE_ROOT)' --vm-cache-root '$(PACKAGE_LOCAL_VM_CACHE_ROOT)' --vm-preserve-env=$(PACKAGE_LOCAL_VM_PRESERVE_ENV) --vm-go '$(PACKAGE_LOCAL_VM_GO)' $(call package_local_dispatch_env,$(1),$(2))
vm_doctor_args = --provider '$(VM_PROVIDER)' --timeout '$(VM_DOCTOR_TIMEOUT)' --packer '$(PACKER)' --prep-version '$(PACKER_VM_PREP_VERSION)' --deploy-lima-bin '$(PACKER_DEPLOY_SMOKE_LIMA_BIN)' --deploy-instance '$(PACKER_DEPLOY_SMOKE_INSTANCE)' --builder-lima-bin '$(PACKER_PACKAGE_BUILDER_LIMA_BIN)' --builder-instance '$(PACKER_PACKAGE_BUILDER_INSTANCE)' --builder-go-version '$(PACKER_PACKAGE_BUILDER_GO_VERSION)' --builder-cache-root '$(PACKER_PACKAGE_BUILDER_CACHE_ROOT)' --builder-workspace-root '$(PACKER_PACKAGE_BUILDER_WORKSPACE_ROOT)' --smoke-lima-bin '$(PACKER_PACKAGE_SMOKE_LIMA_BIN)' --deb-smoke-instance '$(PACKER_PACKAGE_DEB_SMOKE_INSTANCE)' --rpm-smoke-instance '$(PACKER_PACKAGE_RPM_SMOKE_INSTANCE)'
vm_doctor = $(GO) run ./tools/vm-doctor $(vm_doctor_args)
package_common_deb_path = $(PACKAGE_OUT)/vectis-common_$(PACKAGE_VERSION)-$(PACKAGE_RELEASE)_$(call package_deb_arch,$(1)).deb
package_common_rpm_path = $(PACKAGE_OUT)/vectis-common-$(subst -,_,$(PACKAGE_VERSION))-$(subst -,_,$(PACKAGE_RELEASE)).$(call package_rpm_arch,$(1)).rpm
package_service_deb_path = $(PACKAGE_OUT)/vectis-$(2)_$(PACKAGE_VERSION)-$(PACKAGE_RELEASE)_$(call package_deb_arch,$(1)).deb
package_service_rpm_path = $(PACKAGE_OUT)/vectis-$(2)-$(subst -,_,$(PACKAGE_VERSION))-$(subst -,_,$(PACKAGE_RELEASE)).$(call package_rpm_arch,$(1)).rpm
package_local_deb_path = $(PACKAGE_OUT)/vectis-local_$(PACKAGE_VERSION)-$(PACKAGE_RELEASE)_$(call package_deb_arch,$(1)).deb
package_local_rpm_path = $(PACKAGE_OUT)/vectis-local-$(subst -,_,$(PACKAGE_VERSION))-$(subst -,_,$(PACKAGE_RELEASE)).$(call package_rpm_arch,$(1)).rpm
package_services_deb_path = $(PACKAGE_OUT)/vectis-services_$(PACKAGE_VERSION)-$(PACKAGE_RELEASE)_$(call package_deb_arch,$(1)).deb
package_services_rpm_path = $(PACKAGE_OUT)/vectis-services-$(subst -,_,$(PACKAGE_VERSION))-$(subst -,_,$(PACKAGE_RELEASE)).$(call package_rpm_arch,$(1)).rpm
PACKAGE_VERSION ?= 0.0.0+$(COMMIT)
PACKAGE_RELEASE ?= 1
PACKAGE_LINUX_ARTIFACTS := $(PACKAGE_BUILD_DIR)/linux-artifacts
PACKAGE_LINUX_ARTIFACTS_STAMP := $(PACKAGE_LINUX_ARTIFACTS)/.stamp
PACKAGE_LOCAL_WRAPPER := $(PACKAGE_BUILD_DIR)/vectis-local-wrapper
PACKAGE_CLI_BIN = $(PACKAGE_BUILD_DIR)/linux-$(PACKAGE_ARCH)/vectis-cli
PACKAGE_CLI_DEB = $(PACKAGE_OUT)/vectis-cli_$(PACKAGE_VERSION)-$(PACKAGE_RELEASE)_$(call package_deb_arch,$(PACKAGE_ARCH)).deb
PACKAGE_CLI_RPM = $(PACKAGE_OUT)/vectis-cli-$(subst -,_,$(PACKAGE_VERSION))-$(subst -,_,$(PACKAGE_RELEASE)).$(call package_rpm_arch,$(PACKAGE_ARCH)).rpm
PACKAGE_COMMON_DEB = $(call package_common_deb_path,$(PACKAGE_ARCH))
PACKAGE_COMMON_RPM = $(call package_common_rpm_path,$(PACKAGE_ARCH))
PACKAGE_LOCAL_DEB = $(call package_local_deb_path,$(PACKAGE_ARCH))
PACKAGE_LOCAL_RPM = $(call package_local_rpm_path,$(PACKAGE_ARCH))
PACKAGE_SERVICE_DEB_PATHS = $(foreach app,$(PACKAGE_SERVICE_APPS),$(call package_service_deb_path,$(PACKAGE_ARCH),$(app)))
PACKAGE_SERVICE_RPM_PATHS = $(foreach app,$(PACKAGE_SERVICE_APPS),$(call package_service_rpm_path,$(PACKAGE_ARCH),$(app)))
PACKAGE_SERVICES_DEB = $(call package_services_deb_path,$(PACKAGE_ARCH))
PACKAGE_SERVICES_RPM = $(call package_services_rpm_path,$(PACKAGE_ARCH))
PACKAGE_CLI_DEB_TARGETS := $(addprefix package-cli-deb-,$(PACKAGE_ARCHES))
PACKAGE_CLI_RPM_TARGETS := $(addprefix package-cli-rpm-,$(PACKAGE_ARCHES))
PACKAGE_CLI_DEB_ARCH_TARGET := package-cli-deb-$(PACKAGE_ARCH)
PACKAGE_CLI_RPM_ARCH_TARGET := package-cli-rpm-$(PACKAGE_ARCH)
PACKAGE_CLI_ALL_DEB_TARGETS := $(sort $(PACKAGE_CLI_DEB_TARGETS) $(PACKAGE_CLI_DEB_ARCH_TARGET))
PACKAGE_CLI_ALL_RPM_TARGETS := $(sort $(PACKAGE_CLI_RPM_TARGETS) $(PACKAGE_CLI_RPM_ARCH_TARGET))
PACKAGE_SERVICE_ALL_ARCHES := $(sort $(PACKAGE_ARCHES) $(PACKAGE_ARCH))
PACKAGE_SERVICE_BINARIES := $(foreach arch,$(PACKAGE_SERVICE_ALL_ARCHES),$(call package_service_bins,$(arch)))
PACKAGE_LOCAL_ALL_ARCHES := $(sort $(PACKAGE_LOCAL_ARCHES) $(PACKAGE_ARCH))
PACKAGE_LOCAL_BINARIES := $(foreach arch,$(PACKAGE_LOCAL_ALL_ARCHES),$(call package_local_bins,$(arch)))
PACKAGE_COMMON_DEB_TARGETS := $(addprefix package-common-deb-,$(PACKAGE_ARCHES))
PACKAGE_COMMON_RPM_TARGETS := $(addprefix package-common-rpm-,$(PACKAGE_ARCHES))
PACKAGE_LOCAL_DEB_TARGETS := $(addprefix package-local-deb-,$(PACKAGE_LOCAL_ARCHES))
PACKAGE_LOCAL_RPM_TARGETS := $(addprefix package-local-rpm-,$(PACKAGE_LOCAL_ARCHES))
PACKAGE_LOCAL_NATIVE_DEB_TARGETS := $(addprefix package-local-native-deb-,$(PACKAGE_LOCAL_ARCHES))
PACKAGE_LOCAL_NATIVE_RPM_TARGETS := $(addprefix package-local-native-rpm-,$(PACKAGE_LOCAL_ARCHES))
PACKAGE_SERVICE_DEB_TARGETS := $(foreach arch,$(PACKAGE_ARCHES),$(foreach app,$(PACKAGE_SERVICE_APPS),package-service-deb-$(arch)-$(app)))
PACKAGE_SERVICE_RPM_TARGETS := $(foreach arch,$(PACKAGE_ARCHES),$(foreach app,$(PACKAGE_SERVICE_APPS),package-service-rpm-$(arch)-$(app)))
PACKAGE_SERVICES_DEB_TARGETS := $(addprefix package-services-deb-,$(PACKAGE_ARCHES))
PACKAGE_SERVICES_RPM_TARGETS := $(addprefix package-services-rpm-,$(PACKAGE_ARCHES))

.PRECIOUS: $(PACKAGE_BUILD_DIR)/linux-%/vectis-cli $(PACKAGE_SERVICE_BINARIES) $(PACKAGE_LOCAL_BINARIES) $(PACKAGE_LOCAL_WRAPPER)

.PHONY: vm-validate
vm-validate:
	$(MAKE) vm-scripts-test
	$(MAKE) vm-deploy-smoke-validate
	$(MAKE) vm-package-builder-validate
	$(MAKE) vm-package-smoke-validate

.PHONY: vm-scripts-test
vm-scripts-test:
	$(GO) test ./build/packer

.PHONY: vm-prepare
vm-prepare:
	$(MAKE) vm-deploy-smoke-prepare
	$(MAKE) vm-package-builder-prepare
	$(MAKE) vm-package-smoke-prepare

.PHONY: vm-check
vm-check:
	$(MAKE) vm-doctor

.PHONY: vm-status
vm-status:
	$(vm_doctor) --mode status

.PHONY: vm-doctor
vm-doctor:
	$(vm_doctor) --mode doctor

.PHONY: vm-deploy-smoke-validate
vm-deploy-smoke-validate:
	$(PACKER) validate $(packer_deploy_smoke_vars) $(PACKER_DEPLOY_SMOKE_DIR)

.PHONY: vm-deploy-smoke-prepare
vm-deploy-smoke-prepare:
	$(PACKER) build $(packer_deploy_smoke_vars) $(PACKER_DEPLOY_SMOKE_DIR)

.PHONY: vm-deploy-smoke-check
vm-deploy-smoke-check:
	$(vm_doctor) --mode doctor --lane deploy-smoke

.PHONY: vm-package-builder-validate
vm-package-builder-validate:
	$(PACKER) validate $(packer_package_builder_vars) $(PACKER_PACKAGE_BUILDER_DIR)

.PHONY: vm-package-builder-prepare
vm-package-builder-prepare:
	$(PACKER) build $(packer_package_builder_vars) $(PACKER_PACKAGE_BUILDER_DIR)

.PHONY: vm-package-builder-check
vm-package-builder-check:
	$(vm_doctor) --mode doctor --lane package-builder

.PHONY: vm-package-smoke-validate
vm-package-smoke-validate:
	$(PACKER) validate $(call packer_package_smoke_vars,deb,$(PACKER_PACKAGE_DEB_SMOKE_INSTANCE),$(PACKER_PACKAGE_DEB_SMOKE_TEMPLATE)) $(PACKER_PACKAGE_SMOKE_DIR)
	$(PACKER) validate $(call packer_package_smoke_vars,rpm,$(PACKER_PACKAGE_RPM_SMOKE_INSTANCE),$(PACKER_PACKAGE_RPM_SMOKE_TEMPLATE)) $(PACKER_PACKAGE_SMOKE_DIR)

.PHONY: vm-package-smoke-deb-prepare
vm-package-smoke-deb-prepare:
	$(PACKER) build $(call packer_package_smoke_vars,deb,$(PACKER_PACKAGE_DEB_SMOKE_INSTANCE),$(PACKER_PACKAGE_DEB_SMOKE_TEMPLATE)) $(PACKER_PACKAGE_SMOKE_DIR)

.PHONY: vm-package-smoke-rpm-prepare
vm-package-smoke-rpm-prepare:
	$(PACKER) build $(call packer_package_smoke_vars,rpm,$(PACKER_PACKAGE_RPM_SMOKE_INSTANCE),$(PACKER_PACKAGE_RPM_SMOKE_TEMPLATE)) $(PACKER_PACKAGE_SMOKE_DIR)

.PHONY: vm-package-smoke-prepare
vm-package-smoke-prepare: vm-package-smoke-deb-prepare vm-package-smoke-rpm-prepare

.PHONY: vm-package-smoke-deb-check
vm-package-smoke-deb-check:
	$(vm_doctor) --mode doctor --lane package-smoke-deb

.PHONY: vm-package-smoke-rpm-check
vm-package-smoke-rpm-check:
	$(vm_doctor) --mode doctor --lane package-smoke-rpm

.PHONY: vm-package-smoke-check
vm-package-smoke-check: vm-package-smoke-deb-check vm-package-smoke-rpm-check

$(PACKAGE_BUILD_DIR)/linux-%/vectis-cli: cmd/cli/main.go $(API) $(INTERNAL)
	mkdir -p $(dir ${@})
	GOOS=linux GOARCH=${*} CGO_ENABLED=0 $(GO) build -tags=nosqlite -ldflags '${LDFLAGS}' -o ${@} ./cmd/cli

$(PACKAGE_LINUX_ARTIFACTS_STAMP): deploy/linux/services.toml deploy/linux/artifacts.go deploy/linux/cmd/render/main.go
	rm -rf $(PACKAGE_LINUX_ARTIFACTS)
	mkdir -p $(PACKAGE_LINUX_ARTIFACTS)
	go run ./deploy/linux/cmd/render -out $(PACKAGE_LINUX_ARTIFACTS)
	touch ${@}

define package_service_binary_rules
$(PACKAGE_BUILD_DIR)/linux-$(1)/vectis-%: cmd/%/main.go $(API) $(INTERNAL)
	mkdir -p $$(dir $$@)
	GOOS=linux GOARCH=$(1) CGO_ENABLED=0 $$(GO) build -tags=nosqlite -ldflags '$$(LDFLAGS)' -o $$@ ./cmd/$$*
endef

$(foreach arch,$(PACKAGE_SERVICE_ALL_ARCHES),$(eval $(call package_service_binary_rules,$(arch))))

.PHONY: $(addprefix package-local-cgo-check-,$(PACKAGE_LOCAL_ALL_ARCHES))
$(addprefix package-local-cgo-check-,$(PACKAGE_LOCAL_ALL_ARCHES)): package-local-cgo-check-%:
	@if [ "$$($(GO) env GOOS)" != "linux" ] && [ "$(PACKAGE_LOCAL_ALLOW_CROSS_CGO)" != "1" ]; then \
		echo "vectis-local Linux packages require a CGO Linux build. Run this target on Linux or set PACKAGE_LOCAL_ALLOW_CROSS_CGO=1 with a working Linux C toolchain."; \
		exit 1; \
	fi

$(PACKAGE_LOCAL_WRAPPER):
	mkdir -p $(dir ${@})
	printf '%s\n' '#!/bin/sh' 'exec /usr/lib/vectis-local/bin/vectis-local "$$@"' > ${@}
	chmod 0755 ${@}

define package_local_binary_rules
$(call package_local_bin_dir,$(1))/vectis-%: package-local-cgo-check-$(1) cmd/%/main.go $(API) $(INTERNAL)
	mkdir -p $$(dir $$@)
	GOOS=linux GOARCH=$(1) CGO_ENABLED=1 $$(GO) build -ldflags '$$(LDFLAGS)' -o $$@ ./cmd/$$*
endef

$(foreach arch,$(PACKAGE_LOCAL_ALL_ARCHES),$(eval $(call package_local_binary_rules,$(arch))))

.PHONY: $(PACKAGE_CLI_ALL_DEB_TARGETS)
$(PACKAGE_CLI_ALL_DEB_TARGETS): package-cli-deb-%: $(PACKAGE_BUILD_DIR)/linux-%/vectis-cli
	go run ./deploy/package/cmd/build --package vectis-cli --format deb --out $(PACKAGE_OUT) --version $(PACKAGE_VERSION) --release $(PACKAGE_RELEASE) --arch ${*} --input vectis-cli=${<}

.PHONY: $(PACKAGE_CLI_ALL_RPM_TARGETS)
$(PACKAGE_CLI_ALL_RPM_TARGETS): package-cli-rpm-%: $(PACKAGE_BUILD_DIR)/linux-%/vectis-cli
	go run ./deploy/package/cmd/build --package vectis-cli --format rpm --out $(PACKAGE_OUT) --version $(PACKAGE_VERSION) --release $(PACKAGE_RELEASE) --arch ${*} --input vectis-cli=${<}

.PHONY: package-cli-deb
package-cli-deb: $(PACKAGE_CLI_DEB_TARGETS)

.PHONY: package-cli-rpm
package-cli-rpm: $(PACKAGE_CLI_RPM_TARGETS)

.PHONY: package-cli
package-cli: package-cli-deb package-cli-rpm

.PHONY: $(addprefix package-local-deb-,$(PACKAGE_LOCAL_ALL_ARCHES))
$(addprefix package-local-deb-,$(PACKAGE_LOCAL_ALL_ARCHES)): package-local-deb-%:
	$(call package_local_dispatch,deb,${*})

.PHONY: $(addprefix package-local-rpm-,$(PACKAGE_LOCAL_ALL_ARCHES))
$(addprefix package-local-rpm-,$(PACKAGE_LOCAL_ALL_ARCHES)): package-local-rpm-%:
	$(call package_local_dispatch,rpm,${*})

.PHONY: $(addprefix package-local-native-deb-,$(PACKAGE_LOCAL_ALL_ARCHES))
$(addprefix package-local-native-deb-,$(PACKAGE_LOCAL_ALL_ARCHES)): package-local-native-deb-%: $(PACKAGE_LOCAL_WRAPPER) $(call package_local_bins,%)
	go run ./deploy/package/cmd/build --package vectis-local --format deb --out $(PACKAGE_OUT) --version $(PACKAGE_VERSION) --release $(PACKAGE_RELEASE) --arch ${*} $(call package_local_inputs,${*})

.PHONY: $(addprefix package-local-native-rpm-,$(PACKAGE_LOCAL_ALL_ARCHES))
$(addprefix package-local-native-rpm-,$(PACKAGE_LOCAL_ALL_ARCHES)): package-local-native-rpm-%: $(PACKAGE_LOCAL_WRAPPER) $(call package_local_bins,%)
	go run ./deploy/package/cmd/build --package vectis-local --format rpm --out $(PACKAGE_OUT) --version $(PACKAGE_VERSION) --release $(PACKAGE_RELEASE) --arch ${*} $(call package_local_inputs,${*})

.PHONY: $(addprefix package-common-deb-,$(PACKAGE_SERVICE_ALL_ARCHES))
$(addprefix package-common-deb-,$(PACKAGE_SERVICE_ALL_ARCHES)): package-common-deb-%: $(PACKAGE_LINUX_ARTIFACTS_STAMP)
	go run ./deploy/package/cmd/build --package vectis-common --format deb --out $(PACKAGE_OUT) --version $(PACKAGE_VERSION) --release $(PACKAGE_RELEASE) --arch ${*} $(package_common_inputs)

.PHONY: $(addprefix package-common-rpm-,$(PACKAGE_SERVICE_ALL_ARCHES))
$(addprefix package-common-rpm-,$(PACKAGE_SERVICE_ALL_ARCHES)): package-common-rpm-%: $(PACKAGE_LINUX_ARTIFACTS_STAMP)
	go run ./deploy/package/cmd/build --package vectis-common --format rpm --out $(PACKAGE_OUT) --version $(PACKAGE_VERSION) --release $(PACKAGE_RELEASE) --arch ${*} $(package_common_inputs)

define package_service_package_rules
.PHONY: package-service-deb-$(1)-$(2)
package-service-deb-$(1)-$(2): $(PACKAGE_LINUX_ARTIFACTS_STAMP) $(PACKAGE_BUILD_DIR)/linux-$(1)/vectis-$(2)
	go run ./deploy/package/cmd/build --package vectis-$(2) --format deb --out $(PACKAGE_OUT) --version $(PACKAGE_VERSION) --release $(PACKAGE_RELEASE) --arch $(1) $$(call package_service_inputs,$(1),$(2))

.PHONY: package-service-rpm-$(1)-$(2)
package-service-rpm-$(1)-$(2): $(PACKAGE_LINUX_ARTIFACTS_STAMP) $(PACKAGE_BUILD_DIR)/linux-$(1)/vectis-$(2)
	go run ./deploy/package/cmd/build --package vectis-$(2) --format rpm --out $(PACKAGE_OUT) --version $(PACKAGE_VERSION) --release $(PACKAGE_RELEASE) --arch $(1) $$(call package_service_inputs,$(1),$(2))
endef

$(foreach arch,$(PACKAGE_SERVICE_ALL_ARCHES),$(foreach app,$(PACKAGE_SERVICE_APPS),$(eval $(call package_service_package_rules,$(arch),$(app)))))

.PHONY: $(addprefix package-services-deb-,$(PACKAGE_SERVICE_ALL_ARCHES))
$(addprefix package-services-deb-,$(PACKAGE_SERVICE_ALL_ARCHES)): package-services-deb-%: package-common-deb-% $(foreach app,$(PACKAGE_SERVICE_APPS),package-service-deb-%-$(app))
	go run ./deploy/package/cmd/build --package vectis-services --format deb --out $(PACKAGE_OUT) --version $(PACKAGE_VERSION) --release $(PACKAGE_RELEASE) --arch ${*}

.PHONY: $(addprefix package-services-rpm-,$(PACKAGE_SERVICE_ALL_ARCHES))
$(addprefix package-services-rpm-,$(PACKAGE_SERVICE_ALL_ARCHES)): package-services-rpm-%: package-common-rpm-% $(foreach app,$(PACKAGE_SERVICE_APPS),package-service-rpm-%-$(app))
	go run ./deploy/package/cmd/build --package vectis-services --format rpm --out $(PACKAGE_OUT) --version $(PACKAGE_VERSION) --release $(PACKAGE_RELEASE) --arch ${*}

.PHONY: package-common-deb
package-common-deb: $(PACKAGE_COMMON_DEB_TARGETS)

.PHONY: package-common-rpm
package-common-rpm: $(PACKAGE_COMMON_RPM_TARGETS)

.PHONY: package-common
package-common: package-common-deb package-common-rpm

.PHONY: package-local-deb
package-local-deb: $(PACKAGE_LOCAL_DEB_TARGETS)

.PHONY: package-local-rpm
package-local-rpm: $(PACKAGE_LOCAL_RPM_TARGETS)

.PHONY: package-local
package-local: package-local-deb package-local-rpm

.PHONY: package-local-native-deb
package-local-native-deb: $(PACKAGE_LOCAL_NATIVE_DEB_TARGETS)

.PHONY: package-local-native-rpm
package-local-native-rpm: $(PACKAGE_LOCAL_NATIVE_RPM_TARGETS)

.PHONY: package-local-native
package-local-native: package-local-native-deb package-local-native-rpm

.PHONY: package-service-deb
package-service-deb: $(PACKAGE_SERVICE_DEB_TARGETS)

.PHONY: package-service-rpm
package-service-rpm: $(PACKAGE_SERVICE_RPM_TARGETS)

.PHONY: package-service
package-service: package-service-deb package-service-rpm

.PHONY: package-services-deb
package-services-deb: $(PACKAGE_SERVICES_DEB_TARGETS)

.PHONY: package-services-rpm
package-services-rpm: $(PACKAGE_SERVICES_RPM_TARGETS)

.PHONY: package-services
package-services: package-services-deb package-services-rpm

.PHONY: package-linux
package-linux: package-cli package-services

.PHONY: test-package
test-package:
	go test ./deploy/package/...

.PHONY: test-e2e-package-cli-deb
test-e2e-package-cli-deb: $(PACKAGE_CLI_DEB_ARCH_TARGET)
	PACKER_VM_PREP_VERSION=$(PACKER_VM_PREP_VERSION) VECTIS_E2E_PACKAGE_CLI_DEB=$(abspath $(PACKAGE_CLI_DEB)) go test -tags=e2e ./tests/e2e/package/linux -run TestE2EPackageCLIDeb -count=1 -v

.PHONY: test-e2e-package-cli-rpm
test-e2e-package-cli-rpm: $(PACKAGE_CLI_RPM_ARCH_TARGET)
	PACKER_VM_PREP_VERSION=$(PACKER_VM_PREP_VERSION) VECTIS_E2E_PACKAGE_CLI_RPM=$(abspath $(PACKAGE_CLI_RPM)) go test -tags=e2e ./tests/e2e/package/linux -run TestE2EPackageCLIRPM -count=1 -v

.PHONY: test-e2e-package-services-deb
test-e2e-package-services-deb: $(PACKAGE_CLI_DEB_ARCH_TARGET) package-services-deb-$(PACKAGE_ARCH)
	PACKER_VM_PREP_VERSION=$(PACKER_VM_PREP_VERSION) VECTIS_E2E_PACKAGE_CLI_DEB=$(abspath $(PACKAGE_CLI_DEB)) VECTIS_E2E_PACKAGE_SERVICES_DEB="$(abspath $(PACKAGE_COMMON_DEB) $(PACKAGE_SERVICE_DEB_PATHS) $(PACKAGE_SERVICES_DEB))" go test -tags=e2e ./tests/e2e/package/linux -run TestE2EPackageServicesDeb -count=1 -v

.PHONY: test-e2e-package-services-rpm
test-e2e-package-services-rpm: $(PACKAGE_CLI_RPM_ARCH_TARGET) package-services-rpm-$(PACKAGE_ARCH)
	PACKER_VM_PREP_VERSION=$(PACKER_VM_PREP_VERSION) VECTIS_E2E_PACKAGE_CLI_RPM=$(abspath $(PACKAGE_CLI_RPM)) VECTIS_E2E_PACKAGE_SERVICES_RPM="$(abspath $(PACKAGE_COMMON_RPM) $(PACKAGE_SERVICE_RPM_PATHS) $(PACKAGE_SERVICES_RPM))" go test -tags=e2e ./tests/e2e/package/linux -run TestE2EPackageServicesRPM -count=1 -v

.PHONY: test-e2e-package-local-deb
test-e2e-package-local-deb: $(PACKAGE_CLI_DEB_ARCH_TARGET) package-local-deb-$(PACKAGE_ARCH)
	PACKER_VM_PREP_VERSION=$(PACKER_VM_PREP_VERSION) VECTIS_E2E_PACKAGE_CLI_DEB=$(abspath $(PACKAGE_CLI_DEB)) VECTIS_E2E_PACKAGE_LOCAL_DEB=$(abspath $(PACKAGE_LOCAL_DEB)) go test -tags=e2e ./tests/e2e/package/linux -run TestE2EPackageLocalDeb -count=1 -v

.PHONY: test-e2e-package-local-rpm
test-e2e-package-local-rpm: $(PACKAGE_CLI_RPM_ARCH_TARGET) package-local-rpm-$(PACKAGE_ARCH)
	PACKER_VM_PREP_VERSION=$(PACKER_VM_PREP_VERSION) VECTIS_E2E_PACKAGE_CLI_RPM=$(abspath $(PACKAGE_CLI_RPM)) VECTIS_E2E_PACKAGE_LOCAL_RPM=$(abspath $(PACKAGE_LOCAL_RPM)) go test -tags=e2e ./tests/e2e/package/linux -run TestE2EPackageLocalRPM -count=1 -v

.PHONY: test-e2e-package-local
test-e2e-package-local: test-e2e-package-local-deb test-e2e-package-local-rpm

.PHONY: test-e2e-vm
test-e2e-vm:
	$(MAKE) vm-check
	$(MAKE) test-e2e-deploy-linux
	$(MAKE) test-e2e-package-cli-deb
	$(MAKE) test-e2e-package-cli-rpm
	$(MAKE) test-e2e-package-services-deb
	$(MAKE) test-e2e-package-services-rpm
	$(MAKE) test-e2e-package-local

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
	rm -rf artifacts/release-readiness/
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
