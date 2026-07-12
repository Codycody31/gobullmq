##@ General

.PHONY: all
all: help

.PHONY: version
version: ## Print the current version
	@echo ${VERSION}

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk commands is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

.PHONY: vendor
vendor: ## Update the vendor directory
	go mod tidy
	go mod vendor

.PHONY: generate
generate: generate-lua ## Run all code generations
	go generate ./...

.PHONY: generate-lua
generate-lua: ## Generate Lua bindings
	go run ./scripts/generate_lua.go

.PHONY: verify-compat
verify-compat: ## Verify release metadata, BullMQ Lua sources, and generated wrappers
	go run ./scripts/check_compat.go

.PHONY: lint
lint: ## Lint the code with golangci-lint
	golangci-lint run

.PHONY: test
test: ## Run tests
	go test -count=1 -v ./...

.PHONY: test-race
test-race: ## Run the Go race detector
	go test -race -count=1 ./...

.PHONY: test-interop
test-interop: ## Run Node/Go interoperability tests on standalone and cluster Redis
	scripts/ci/test-interop.sh

.PHONY: redis-cluster-up
redis-cluster-up: ## Start the six-node Redis Cluster (Linux host networking required)
	scripts/ci/redis-cluster.sh up

.PHONY: redis-cluster-down
redis-cluster-down: ## Stop the local Redis Cluster; safe to run repeatedly
	scripts/ci/redis-cluster.sh down
