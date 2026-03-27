REGISTRY   ?= reg.local:32000
TAG        ?= latest

.PHONY: build-router build-sidecar build-prefixhash build-all \
        deploy deploy-samples delete tidy fmt vet test

# ---- Build individual images ----
build-router:
	bash scripts/build-router.sh

build-sidecar:
	bash scripts/build-sidecar.sh

build-prefixhash:
	bash scripts/build-prefixhash.sh

build-all: build-router build-sidecar build-prefixhash

# ---- Deploy ----
deploy:
	bash scripts/deploy.sh

deploy-build:
	bash scripts/deploy.sh --build

deploy-samples:
	bash scripts/deploy.sh --samples

delete:
	bash scripts/deploy.sh --delete

# ---- Go development ----
tidy:
	go mod tidy

fmt:
	gofmt -s -w .

vet:
	go vet ./...

test:
	go test ./... -v

# ---- Local run (for development) ----
run-router:
	go run ./cmd/router

run-sidecar:
	go run ./cmd/sidecar

run-prefixhash:
	go run ./cmd/prefixhash
