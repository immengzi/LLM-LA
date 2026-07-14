# Go router + sidecar: unit tests.
#
# NOTE: go.mod requires Go 1.26. If your local Go is older, run these in the
# CI container instead (see .github/workflows/ci.yml, job "go-tests") or:
#   docker run --rm -v "$$PWD":/src -w /src golang:1.26 make test
#
# Quick start:
#   make test     # go test ./...
#   make vet      # go vet ./...
#   make race     # go test -race ./...

.PHONY: test vet race build clean

test:
	go test ./...

vet:
	go vet ./...

race:
	go test -race ./...

build:
	go build ./...

clean:
	go clean -testcache
