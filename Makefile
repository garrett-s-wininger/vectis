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

FORMAL_MODELS := execution reconciliation terminalization cancellation continuation queue_handoff action_timeout worker_choreography cron_schedule cell_ingress_handoff secrets_authorization idempotency_recovery
JAVA ?= java
TLA_TOOLS_JAR ?= /opt/tla+/tla2tools.jar

GO ?= go
MAGE ?= mage
PODMAN ?= podman
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
build:
	SKIP_WEB_BUILD="$(SKIP_WEB_BUILD)" SKIP_DOCS_ASSETS="$(SKIP_DOCS_ASSETS)" OUT_DIR="$(OUT_DIR)" CGO_ENABLED="$(CGO_ENABLED)" BUILD_OPTS="$(BUILD_OPTS)" GO="$(GO)" $(MAGE) build

$(DOCS_ASSETS_STAMP): $(WEBSITE_SOURCES)
	GO="$(GO)" $(MAGE) docsAssets

.PHONY: docs-assets
docs-assets: $(DOCS_ASSETS_STAMP)

.PHONY: podman-grafana-configmaps
podman-grafana-configmaps:
	GO="$(GO)" $(MAGE) podmanGrafanaConfigmaps

.PHONY: build-container
build-container:
	SKIP_WEB_BUILD="$(SKIP_WEB_BUILD)" SKIP_DOCS_ASSETS="$(SKIP_DOCS_ASSETS)" OUT_DIR="$(OUT_DIR)" GO="$(GO)" $(MAGE) buildContainer

.PHONY: proto
proto:
	PROTOC="$(PROTOC)" PROTOC_GEN_GO="$(PROTOC_GEN_GO)" PROTOC_GEN_GO_GRPC="$(PROTOC_GEN_GO_GRPC)" GO="$(GO)" $(MAGE) proto

.PHONY: doctor
doctor:
	GO="$(GO)" $(MAGE) doctor

.PHONY: ci-quick
ci-quick:
	GO="$(GO)" $(MAGE) ciQuick

formal-verification-%: formal/tla/%.tla
	FORMAL_MODEL="${*}" JAVA="$(JAVA)" TLA_TOOLS_JAR="$(TLA_TOOLS_JAR)" GO="$(GO)" $(MAGE) formalVerificationModel

.PHONY: formal-verification
formal-verification:
	FORMAL_MODELS="$(FORMAL_MODELS)" JAVA="$(JAVA)" TLA_TOOLS_JAR="$(TLA_TOOLS_JAR)" GO="$(GO)" $(MAGE) formalVerification

.PHONY: format
format:
	GO="$(GO)" $(MAGE) format

.PHONY: test
test:
	GO="$(GO)" $(MAGE) test

.PHONY: test-integration
test-integration:
	GO="$(GO)" $(MAGE) testIntegration

.PHONY: test-lima
test-lima:
	GO="$(GO)" $(MAGE) testLima

.PHONY: test-e2e
test-e2e:
	GO="$(GO)" $(MAGE) testE2E

.PHONY: test-e2e-deploy-linux
test-e2e-deploy-linux:
	PACKER_VM_PREP_VERSION="$(PACKER_VM_PREP_VERSION)" GO="$(GO)" $(MAGE) testE2EDeployLinux

.PHONY: test-postgres-integration
test-postgres-integration:
	GO="$(GO)" $(MAGE) testPostgresIntegration

.PHONY: test-race
test-race:
	GO="$(GO)" $(MAGE) testRace

.PHONY: test-quick
test-quick:
	GO="$(GO)" $(MAGE) testQuick

.PHONY: release-local-validate
release-local-validate:
	SKIP_WEB_BUILD="$(SKIP_WEB_BUILD)" SKIP_DOCS_ASSETS="$(SKIP_DOCS_ASSETS)" OUT_DIR="$(OUT_DIR)" CGO_ENABLED="$(CGO_ENABLED)" BUILD_OPTS="$(BUILD_OPTS)" GO="$(GO)" $(MAGE) releaseLocalValidate

.PHONY: release-readiness-report
release-readiness-report:
	RELEASE_READINESS_PROFILE="$(RELEASE_READINESS_PROFILE)" RELEASE_READINESS_CHECKS="$(RELEASE_READINESS_CHECKS)" RELEASE_READINESS_ARGS="$(RELEASE_READINESS_ARGS)" GO="$(GO)" $(MAGE) releaseReadinessReport

.PHONY: test-fault
test-fault:
	GO="$(GO)" $(MAGE) testFault

.PHONY: test-property
test-property:
	GO="$(GO)" $(MAGE) testProperty

.PHONY: deploy-artifacts-test
deploy-artifacts-test:
	GO="$(GO)" $(MAGE) deployArtifactsTest

DEPLOY_LINUX_OUT ?= artifacts/deploy/linux

.PHONY: deploy-artifacts-render
deploy-artifacts-render:
	DEPLOY_LINUX_OUT="$(DEPLOY_LINUX_OUT)" GO="$(GO)" $(MAGE) deployArtifactsRender

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
package_deb_arch = $(if $(filter x86_64,$(1)),amd64,$(if $(filter aarch64,$(1)),arm64,$(if $(filter 386,$(1)),i386,$(1))))
package_rpm_arch = $(if $(filter amd64,$(1)),x86_64,$(if $(filter arm64,$(1)),aarch64,$(1)))
package_service_bins = $(addprefix $(PACKAGE_BUILD_DIR)/linux-$(1)/vectis-,$(PACKAGE_SERVICE_APPS))
package_local_bin_dir = $(PACKAGE_BUILD_DIR)/linux-$(1)-local/bin
package_local_bins = $(addprefix $(call package_local_bin_dir,$(1))/vectis-,$(PACKAGE_LOCAL_APPS))
VM_MAGE_ENV = GO="$(GO)" PACKER="$(PACKER)" PACKER_VM_PREP_VERSION="$(PACKER_VM_PREP_VERSION)" \
	VM_PROVIDER="$(VM_PROVIDER)" VM_DOCTOR_TIMEOUT="$(VM_DOCTOR_TIMEOUT)" \
	PACKER_DEPLOY_SMOKE_DIR="$(PACKER_DEPLOY_SMOKE_DIR)" \
	PACKER_DEPLOY_SMOKE_INSTANCE="$(PACKER_DEPLOY_SMOKE_INSTANCE)" \
	PACKER_DEPLOY_SMOKE_TEMPLATE="$(PACKER_DEPLOY_SMOKE_TEMPLATE)" \
	PACKER_DEPLOY_SMOKE_CPUS="$(PACKER_DEPLOY_SMOKE_CPUS)" \
	PACKER_DEPLOY_SMOKE_MEMORY="$(PACKER_DEPLOY_SMOKE_MEMORY)" \
	PACKER_DEPLOY_SMOKE_DISK="$(PACKER_DEPLOY_SMOKE_DISK)" \
	PACKER_DEPLOY_SMOKE_STOP="$(PACKER_DEPLOY_SMOKE_STOP)" \
	PACKER_DEPLOY_SMOKE_LIMA_BIN="$(PACKER_DEPLOY_SMOKE_LIMA_BIN)" \
	PACKER_PACKAGE_BUILDER_DIR="$(PACKER_PACKAGE_BUILDER_DIR)" \
	PACKER_PACKAGE_BUILDER_INSTANCE="$(PACKER_PACKAGE_BUILDER_INSTANCE)" \
	PACKER_PACKAGE_BUILDER_TEMPLATE="$(PACKER_PACKAGE_BUILDER_TEMPLATE)" \
	PACKER_PACKAGE_BUILDER_GO_VERSION="$(PACKER_PACKAGE_BUILDER_GO_VERSION)" \
	PACKER_PACKAGE_BUILDER_GO_SHA256="$(PACKER_PACKAGE_BUILDER_GO_SHA256)" \
	PACKER_PACKAGE_BUILDER_CPUS="$(PACKER_PACKAGE_BUILDER_CPUS)" \
	PACKER_PACKAGE_BUILDER_MEMORY="$(PACKER_PACKAGE_BUILDER_MEMORY)" \
	PACKER_PACKAGE_BUILDER_DISK="$(PACKER_PACKAGE_BUILDER_DISK)" \
	PACKER_PACKAGE_BUILDER_STOP="$(PACKER_PACKAGE_BUILDER_STOP)" \
	PACKER_PACKAGE_BUILDER_LIMA_BIN="$(PACKER_PACKAGE_BUILDER_LIMA_BIN)" \
	PACKER_PACKAGE_BUILDER_WORKSPACE_ROOT="$(PACKER_PACKAGE_BUILDER_WORKSPACE_ROOT)" \
	PACKER_PACKAGE_BUILDER_CACHE_ROOT="$(PACKER_PACKAGE_BUILDER_CACHE_ROOT)" \
	PACKER_PACKAGE_SMOKE_DIR="$(PACKER_PACKAGE_SMOKE_DIR)" \
	PACKER_PACKAGE_SMOKE_CPUS="$(PACKER_PACKAGE_SMOKE_CPUS)" \
	PACKER_PACKAGE_SMOKE_MEMORY="$(PACKER_PACKAGE_SMOKE_MEMORY)" \
	PACKER_PACKAGE_SMOKE_DISK="$(PACKER_PACKAGE_SMOKE_DISK)" \
	PACKER_PACKAGE_SMOKE_STOP="$(PACKER_PACKAGE_SMOKE_STOP)" \
	PACKER_PACKAGE_SMOKE_LIMA_BIN="$(PACKER_PACKAGE_SMOKE_LIMA_BIN)" \
	PACKER_PACKAGE_DEB_SMOKE_INSTANCE="$(PACKER_PACKAGE_DEB_SMOKE_INSTANCE)" \
	PACKER_PACKAGE_DEB_SMOKE_TEMPLATE="$(PACKER_PACKAGE_DEB_SMOKE_TEMPLATE)" \
	PACKER_PACKAGE_RPM_SMOKE_INSTANCE="$(PACKER_PACKAGE_RPM_SMOKE_INSTANCE)" \
	PACKER_PACKAGE_RPM_SMOKE_TEMPLATE="$(PACKER_PACKAGE_RPM_SMOKE_TEMPLATE)" \
	PACKAGE_LOCAL_VM_PROVIDER_PATH="$(PACKAGE_LOCAL_VM_PROVIDER_PATH)"
PACKAGE_MAGE_ENV = GO="$(GO)" PACKER_VM_PREP_VERSION="$(PACKER_VM_PREP_VERSION)" \
	PACKAGE_OUT="$(PACKAGE_OUT)" PACKAGE_BUILD_DIR="$(PACKAGE_BUILD_DIR)" \
	PACKAGE_ARCH="$(PACKAGE_ARCH)" PACKAGE_ARCHES="$(PACKAGE_ARCHES)" \
	PACKAGE_SERVICE_APPS="$(PACKAGE_SERVICE_APPS)" \
	PACKAGE_LOCAL_ARCHES="$(PACKAGE_LOCAL_ARCHES)" \
	PACKAGE_LOCAL_APPS="$(PACKAGE_LOCAL_APPS)" \
	PACKAGE_LOCAL_ALLOW_CROSS_CGO="$(PACKAGE_LOCAL_ALLOW_CROSS_CGO)" \
	PACKAGE_LOCAL_MAKE="$(PACKAGE_LOCAL_MAKE)" \
	PACKAGE_VERSION="$(PACKAGE_VERSION)" PACKAGE_RELEASE="$(PACKAGE_RELEASE)" \
	PACKAGE_LOCAL_VM_PROVIDER="$(PACKAGE_LOCAL_VM_PROVIDER)" \
	PACKAGE_LOCAL_VM_PROVIDER_PATH="$(PACKAGE_LOCAL_VM_PROVIDER_PATH)" \
	PACKAGE_LOCAL_VM_INSTANCE="$(PACKAGE_LOCAL_VM_INSTANCE)" \
	PACKAGE_LOCAL_VM_TIMEOUT="$(PACKAGE_LOCAL_VM_TIMEOUT)" \
	PACKAGE_LOCAL_VM_WORKSPACE_ROOT="$(PACKAGE_LOCAL_VM_WORKSPACE_ROOT)" \
	PACKAGE_LOCAL_VM_CACHE_ROOT="$(PACKAGE_LOCAL_VM_CACHE_ROOT)" \
	PACKAGE_LOCAL_VM_KEEP="$(PACKAGE_LOCAL_VM_KEEP)" \
	PACKAGE_LOCAL_VM_PRESERVE_ENV="$(PACKAGE_LOCAL_VM_PRESERVE_ENV)" \
	PACKAGE_LOCAL_VM_GO="$(PACKAGE_LOCAL_VM_GO)" \
	PACKER_PACKAGE_BUILDER_INSTANCE="$(PACKER_PACKAGE_BUILDER_INSTANCE)" \
	PACKER_PACKAGE_BUILDER_WORKSPACE_ROOT="$(PACKER_PACKAGE_BUILDER_WORKSPACE_ROOT)" \
	PACKER_PACKAGE_BUILDER_CACHE_ROOT="$(PACKER_PACKAGE_BUILDER_CACHE_ROOT)"
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
	$(VM_MAGE_ENV) $(MAGE) vmValidate

.PHONY: vm-scripts-test
vm-scripts-test:
	GO="$(GO)" $(MAGE) vmScriptsTest

.PHONY: vm-prepare
vm-prepare:
	$(VM_MAGE_ENV) $(MAGE) vmPrepare

.PHONY: vm-check
vm-check:
	$(VM_MAGE_ENV) $(MAGE) vmCheck

.PHONY: vm-status
vm-status:
	$(VM_MAGE_ENV) $(MAGE) vmStatus

.PHONY: vm-doctor
vm-doctor:
	$(VM_MAGE_ENV) $(MAGE) vmDoctor

.PHONY: vm-deploy-smoke-validate
vm-deploy-smoke-validate:
	$(VM_MAGE_ENV) $(MAGE) vmDeploySmokeValidate

.PHONY: vm-deploy-smoke-prepare
vm-deploy-smoke-prepare:
	$(VM_MAGE_ENV) $(MAGE) vmDeploySmokePrepare

.PHONY: vm-deploy-smoke-check
vm-deploy-smoke-check:
	$(VM_MAGE_ENV) $(MAGE) vmDeploySmokeCheck

.PHONY: vm-package-builder-validate
vm-package-builder-validate:
	$(VM_MAGE_ENV) $(MAGE) vmPackageBuilderValidate

.PHONY: vm-package-builder-prepare
vm-package-builder-prepare:
	$(VM_MAGE_ENV) $(MAGE) vmPackageBuilderPrepare

.PHONY: vm-package-builder-check
vm-package-builder-check:
	$(VM_MAGE_ENV) $(MAGE) vmPackageBuilderCheck

.PHONY: vm-package-smoke-validate
vm-package-smoke-validate:
	$(VM_MAGE_ENV) $(MAGE) vmPackageSmokeValidate

.PHONY: vm-package-smoke-deb-prepare
vm-package-smoke-deb-prepare:
	$(VM_MAGE_ENV) $(MAGE) vmPackageSmokeDebPrepare

.PHONY: vm-package-smoke-rpm-prepare
vm-package-smoke-rpm-prepare:
	$(VM_MAGE_ENV) $(MAGE) vmPackageSmokeRPMPrepare

.PHONY: vm-package-smoke-prepare
vm-package-smoke-prepare:
	$(VM_MAGE_ENV) $(MAGE) vmPackageSmokePrepare

.PHONY: vm-package-smoke-deb-check
vm-package-smoke-deb-check:
	$(VM_MAGE_ENV) $(MAGE) vmPackageSmokeDebCheck

.PHONY: vm-package-smoke-rpm-check
vm-package-smoke-rpm-check:
	$(VM_MAGE_ENV) $(MAGE) vmPackageSmokeRPMCheck

.PHONY: vm-package-smoke-check
vm-package-smoke-check:
	$(VM_MAGE_ENV) $(MAGE) vmPackageSmokeCheck

$(PACKAGE_BUILD_DIR)/linux-%/vectis-cli: cmd/cli/main.go $(API) $(INTERNAL)
	$(PACKAGE_MAGE_ENV) $(MAGE) packageCLIBinary ${*}

$(PACKAGE_LINUX_ARTIFACTS_STAMP): deploy/linux/services.toml deploy/linux/artifacts.go deploy/linux/cmd/render/main.go
	$(PACKAGE_MAGE_ENV) $(MAGE) packageLinuxArtifacts

define package_service_binary_rules
$(PACKAGE_BUILD_DIR)/linux-$(1)/vectis-%: cmd/%/main.go $(API) $(INTERNAL)
	$$(PACKAGE_MAGE_ENV) $$(MAGE) packageServiceBinary $(1) $$*
endef

$(foreach arch,$(PACKAGE_SERVICE_ALL_ARCHES),$(eval $(call package_service_binary_rules,$(arch))))

.PHONY: $(addprefix package-local-cgo-check-,$(PACKAGE_LOCAL_ALL_ARCHES))
$(addprefix package-local-cgo-check-,$(PACKAGE_LOCAL_ALL_ARCHES)): package-local-cgo-check-%:
	$(PACKAGE_MAGE_ENV) $(MAGE) packageLocalCGOCheck ${*}

$(PACKAGE_LOCAL_WRAPPER):
	$(PACKAGE_MAGE_ENV) $(MAGE) packageLocalWrapper

define package_local_binary_rules
$(call package_local_bin_dir,$(1))/vectis-%: package-local-cgo-check-$(1) cmd/%/main.go $(API) $(INTERNAL)
	$$(PACKAGE_MAGE_ENV) $$(MAGE) packageLocalBinary $(1) $$*
endef

$(foreach arch,$(PACKAGE_LOCAL_ALL_ARCHES),$(eval $(call package_local_binary_rules,$(arch))))

.PHONY: $(PACKAGE_CLI_ALL_DEB_TARGETS)
$(PACKAGE_CLI_ALL_DEB_TARGETS): package-cli-deb-%:
	$(PACKAGE_MAGE_ENV) $(MAGE) packageCLIDebArch ${*}

.PHONY: $(PACKAGE_CLI_ALL_RPM_TARGETS)
$(PACKAGE_CLI_ALL_RPM_TARGETS): package-cli-rpm-%:
	$(PACKAGE_MAGE_ENV) $(MAGE) packageCLIRPMArch ${*}

.PHONY: package-cli-deb
package-cli-deb:
	$(PACKAGE_MAGE_ENV) $(MAGE) packageCLIDeb

.PHONY: package-cli-rpm
package-cli-rpm:
	$(PACKAGE_MAGE_ENV) $(MAGE) packageCLIRPM

.PHONY: package-cli
package-cli:
	$(PACKAGE_MAGE_ENV) $(MAGE) packageCLI

.PHONY: $(addprefix package-local-deb-,$(PACKAGE_LOCAL_ALL_ARCHES))
$(addprefix package-local-deb-,$(PACKAGE_LOCAL_ALL_ARCHES)): package-local-deb-%:
	$(PACKAGE_MAGE_ENV) $(MAGE) packageLocalDebArch ${*}

.PHONY: $(addprefix package-local-rpm-,$(PACKAGE_LOCAL_ALL_ARCHES))
$(addprefix package-local-rpm-,$(PACKAGE_LOCAL_ALL_ARCHES)): package-local-rpm-%:
	$(PACKAGE_MAGE_ENV) $(MAGE) packageLocalRPMArch ${*}

.PHONY: $(addprefix package-local-native-deb-,$(PACKAGE_LOCAL_ALL_ARCHES))
$(addprefix package-local-native-deb-,$(PACKAGE_LOCAL_ALL_ARCHES)): package-local-native-deb-%:
	$(PACKAGE_MAGE_ENV) $(MAGE) packageLocalNativeDebArch ${*}

.PHONY: $(addprefix package-local-native-rpm-,$(PACKAGE_LOCAL_ALL_ARCHES))
$(addprefix package-local-native-rpm-,$(PACKAGE_LOCAL_ALL_ARCHES)): package-local-native-rpm-%:
	$(PACKAGE_MAGE_ENV) $(MAGE) packageLocalNativeRPMArch ${*}

.PHONY: $(addprefix package-common-deb-,$(PACKAGE_SERVICE_ALL_ARCHES))
$(addprefix package-common-deb-,$(PACKAGE_SERVICE_ALL_ARCHES)): package-common-deb-%:
	$(PACKAGE_MAGE_ENV) $(MAGE) packageCommonDebArch ${*}

.PHONY: $(addprefix package-common-rpm-,$(PACKAGE_SERVICE_ALL_ARCHES))
$(addprefix package-common-rpm-,$(PACKAGE_SERVICE_ALL_ARCHES)): package-common-rpm-%:
	$(PACKAGE_MAGE_ENV) $(MAGE) packageCommonRPMArch ${*}

define package_service_package_rules
.PHONY: package-service-deb-$(1)-$(2)
package-service-deb-$(1)-$(2):
	$$(PACKAGE_MAGE_ENV) $$(MAGE) packageServiceDebArch $(1) $(2)

.PHONY: package-service-rpm-$(1)-$(2)
package-service-rpm-$(1)-$(2):
	$$(PACKAGE_MAGE_ENV) $$(MAGE) packageServiceRPMArch $(1) $(2)
endef

$(foreach arch,$(PACKAGE_SERVICE_ALL_ARCHES),$(foreach app,$(PACKAGE_SERVICE_APPS),$(eval $(call package_service_package_rules,$(arch),$(app)))))

.PHONY: $(addprefix package-services-deb-,$(PACKAGE_SERVICE_ALL_ARCHES))
$(addprefix package-services-deb-,$(PACKAGE_SERVICE_ALL_ARCHES)): package-services-deb-%:
	$(PACKAGE_MAGE_ENV) $(MAGE) packageServicesDebArch ${*}

.PHONY: $(addprefix package-services-rpm-,$(PACKAGE_SERVICE_ALL_ARCHES))
$(addprefix package-services-rpm-,$(PACKAGE_SERVICE_ALL_ARCHES)): package-services-rpm-%:
	$(PACKAGE_MAGE_ENV) $(MAGE) packageServicesRPMArch ${*}

.PHONY: package-common-deb
package-common-deb:
	$(PACKAGE_MAGE_ENV) $(MAGE) packageCommonDeb

.PHONY: package-common-rpm
package-common-rpm:
	$(PACKAGE_MAGE_ENV) $(MAGE) packageCommonRPM

.PHONY: package-common
package-common:
	$(PACKAGE_MAGE_ENV) $(MAGE) packageCommon

.PHONY: package-local-deb
package-local-deb:
	$(PACKAGE_MAGE_ENV) $(MAGE) packageLocalDeb

.PHONY: package-local-rpm
package-local-rpm:
	$(PACKAGE_MAGE_ENV) $(MAGE) packageLocalRPM

.PHONY: package-local
package-local:
	$(PACKAGE_MAGE_ENV) $(MAGE) packageLocal

.PHONY: package-local-native-deb
package-local-native-deb:
	$(PACKAGE_MAGE_ENV) $(MAGE) packageLocalNativeDeb

.PHONY: package-local-native-rpm
package-local-native-rpm:
	$(PACKAGE_MAGE_ENV) $(MAGE) packageLocalNativeRPM

.PHONY: package-local-native
package-local-native:
	$(PACKAGE_MAGE_ENV) $(MAGE) packageLocalNative

.PHONY: package-service-deb
package-service-deb:
	$(PACKAGE_MAGE_ENV) $(MAGE) packageServiceDeb

.PHONY: package-service-rpm
package-service-rpm:
	$(PACKAGE_MAGE_ENV) $(MAGE) packageServiceRPM

.PHONY: package-service
package-service:
	$(PACKAGE_MAGE_ENV) $(MAGE) packageService

.PHONY: package-services-deb
package-services-deb:
	$(PACKAGE_MAGE_ENV) $(MAGE) packageServicesDeb

.PHONY: package-services-rpm
package-services-rpm:
	$(PACKAGE_MAGE_ENV) $(MAGE) packageServicesRPM

.PHONY: package-services
package-services:
	$(PACKAGE_MAGE_ENV) $(MAGE) packageServices

.PHONY: package-linux
package-linux:
	$(PACKAGE_MAGE_ENV) $(MAGE) packageLinux

.PHONY: test-package
test-package:
	GO="$(GO)" $(MAGE) testPackage

.PHONY: test-e2e-package-cli-deb
test-e2e-package-cli-deb:
	$(PACKAGE_MAGE_ENV) $(MAGE) testE2EPackageCLIDeb

.PHONY: test-e2e-package-cli-rpm
test-e2e-package-cli-rpm:
	$(PACKAGE_MAGE_ENV) $(MAGE) testE2EPackageCLIRPM

.PHONY: test-e2e-package-services-deb
test-e2e-package-services-deb:
	$(PACKAGE_MAGE_ENV) $(MAGE) testE2EPackageServicesDeb

.PHONY: test-e2e-package-services-rpm
test-e2e-package-services-rpm:
	$(PACKAGE_MAGE_ENV) $(MAGE) testE2EPackageServicesRPM

.PHONY: test-e2e-package-local-deb
test-e2e-package-local-deb:
	$(PACKAGE_MAGE_ENV) $(MAGE) testE2EPackageLocalDeb

.PHONY: test-e2e-package-local-rpm
test-e2e-package-local-rpm:
	$(PACKAGE_MAGE_ENV) $(MAGE) testE2EPackageLocalRPM

.PHONY: test-e2e-package-local
test-e2e-package-local:
	$(PACKAGE_MAGE_ENV) $(MAGE) testE2EPackageLocal

.PHONY: test-e2e-vm
test-e2e-vm:
	$(VM_MAGE_ENV) $(PACKAGE_MAGE_ENV) $(MAGE) testE2EVM

.PHONY: website-a11y
website-a11y:
	GO="$(GO)" $(MAGE) websiteA11y

GOLANGCI_LINT_VERSION ?= v2.6.1
GOVULNCHECK_VERSION ?= v1.1.4

# NOTE(garrett): Match the `go` directive in go.mod to use the same stdlib the module declares.
GO_MOD_GO_VERSION := $(shell awk '/^go /{print $$2; exit}' go.mod)

.PHONY: lint-api-routes
lint-api-routes:
	GO="$(GO)" $(MAGE) lintAPIRoutes

.PHONY: lint
lint:
	GOLANGCI_LINT_VERSION="$(GOLANGCI_LINT_VERSION)" GO="$(GO)" $(MAGE) lint

.PHONY: vulncheck
vulncheck:
	GOVULNCHECK_VERSION="$(GOVULNCHECK_VERSION)" GO="$(GO)" $(MAGE) vulncheck

.PHONY: perf
perf:
	SUITE="$(SUITE)" PERF_ARGS="$(PERF_ARGS)" PERF_BIN="$(PERF_BIN)" CGO_ENABLED="$(CGO_ENABLED)" BUILD_OPTS="$(BUILD_OPTS)" GO="$(GO)" $(MAGE) perf

.PHONY: perf-compare
perf-compare:
	BASELINE="$(BASELINE)" CURRENT="$(CURRENT)" PERF_BIN="$(PERF_BIN)" CGO_ENABLED="$(CGO_ENABLED)" BUILD_OPTS="$(BUILD_OPTS)" GO="$(GO)" $(MAGE) perfCompare

$(PERF_BIN): $(PERF_SOURCES) | $(OUT_DIR)
	CGO_ENABLED=${CGO_ENABLED} $(GO) build ${BUILD_OPTS} -o ${@} ./scripts/perf

FUZZTIME ?= 30s

.PHONY: fuzz-api-auth
fuzz-api-auth:
	FUZZTIME="$(FUZZTIME)" GO="$(GO)" $(MAGE) fuzzAPIAuth

.PHONY: clean
clean:
	OUT_DIR="$(OUT_DIR)" GO="$(GO)" $(MAGE) clean

.PHONY: image-full
image-full:
	PODMAN="$(PODMAN)" GO="$(GO)" $(MAGE) imageFull

# NOTE(garrett): Slight hack to ensure .PHONY applies to individual image builds while
# getting around the copy-pasta Podman build commands.
image-internal-%:
	PODMAN="$(PODMAN)" GO="$(GO)" $(MAGE) image ${*}

.PHONY: $(addprefix image-, $(COMPONENTS))
image-api: image-internal-api
image-artifact: image-internal-artifact
image-catalog: image-internal-catalog
image-cell-ingress: image-internal-cell-ingress
image-cli: image-internal-cli
image-cron: image-internal-cron
image-docs: image-internal-docs
image-spiffe: image-internal-spiffe
image-orchestrator: image-internal-orchestrator
image-queue: image-internal-queue
image-log: image-internal-log
image-log-forwarder: image-internal-log-forwarder
image-reconciler: image-internal-reconciler
image-registry: image-internal-registry
image-secrets: image-internal-secrets
image-worker: image-internal-worker
image-worker-core: image-internal-worker-core

.PHONY: images-all
images-all:
	SKIP_WEB_BUILD="$(SKIP_WEB_BUILD)" PODMAN="$(PODMAN)" GO="$(GO)" $(MAGE) imagesAll

.PHONY: images-components
images-components:
	SKIP_WEB_BUILD="$(SKIP_WEB_BUILD)" PODMAN="$(PODMAN)" GO="$(GO)" $(MAGE) imagesComponents
