SHELL := /usr/bin/env bash
VERSION := $(shell tr -d ' \n\r\t' < VERSION)
BIN := bin/vfs

.PHONY: help build test fmt vet check clean install image

.DEFAULT_GOAL := help

help: ## Show targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

build: ## Build vfs CLI
	@mkdir -p bin
	go build -ldflags "-X main.version=$(VERSION)" -o $(BIN) ./cmd/vfs

build-fuse: ## Build with FUSE mount support (Linux/macOS)
	@mkdir -p bin
	go build -tags=fuse -ldflags "-X main.version=$(VERSION)" -o $(BIN) ./cmd/vfs

test: ## Run unit tests
	go test -race ./...

fmt: ## gofmt
	gofmt -s -w .

vet: ## go vet
	go vet ./...

check: fmt vet test ## fmt + vet + test

install: build ## Install to $GOBIN
	go install -ldflags "-X main.version=$(VERSION)" ./cmd/vfs

clean: ## Clean build artifacts
	rm -rf bin/ dist/

image: ## Build container image
	docker build --build-arg VERSION=$(VERSION) -t ghcr.io/hanzoai/vfs:$(VERSION) .
