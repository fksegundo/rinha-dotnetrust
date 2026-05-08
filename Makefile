SHELL := /bin/bash

APP_IMAGE ?= filonsegundo/rinha-dotnetrust-api:submission
LB_IMAGE ?= filonsegundo/rinha-dotnetrust-lb:submission
RUST_TARGET_CPU ?= haswell
RINHA_NATIVE_LEAF_SIZE ?= 192
DOTNET_CLI_HOME ?= /tmp/dotnet-home
OFFICIAL_REPO ?= ../rinha-de-backend-2026-main

.PHONY: build native build-api publish-api compose-up compose-down compose-pull smoke smoke-build bench bench-build bench-diagnostic bench-matrix verify-compose sync-resources

build:
	DOTNET_CLI_HOME=$(DOTNET_CLI_HOME) dotnet build src/Rinha.Api/Rinha.Api.csproj -c Release -v minimal
	DOTNET_CLI_HOME=$(DOTNET_CLI_HOME) dotnet build tools/Rinha.Preprocess/Rinha.Preprocess.csproj -c Release -v minimal
	DOTNET_CLI_HOME=$(DOTNET_CLI_HOME) dotnet build tools/Rinha.VerifyNative/Rinha.VerifyNative.csproj -c Release -v minimal
	cargo build --release --manifest-path native/rinha-native/Cargo.toml

native:
	cargo build --release --manifest-path native/rinha-native/Cargo.toml

sync-resources:
	mkdir -p resources
	cp $(OFFICIAL_REPO)/resources/references.json.gz resources/references.json.gz

build-api:
	test -f resources/references.json.gz
	docker build \
		--build-arg RUST_TARGET_CPU=$(RUST_TARGET_CPU) \
		--build-arg RINHA_NATIVE_LEAF_SIZE=$(RINHA_NATIVE_LEAF_SIZE) \
		-f submission/Dockerfile \
		-t $(APP_IMAGE) \
		.

publish-api:
	test -f resources/references.json.gz
	APP_IMAGE=$(APP_IMAGE) \
	RUST_TARGET_CPU=$(RUST_TARGET_CPU) \
	RINHA_NATIVE_LEAF_SIZE=$(RINHA_NATIVE_LEAF_SIZE) \
	./scripts/publish-api-image.sh

verify-compose:
	docker compose -f submission/docker-compose.yml config >/dev/null

compose-pull:
	APP_IMAGE=$(APP_IMAGE) LB_IMAGE=$(LB_IMAGE) docker compose -f submission/docker-compose.yml pull

compose-up:
	APP_IMAGE=$(APP_IMAGE) LB_IMAGE=$(LB_IMAGE) docker compose -f submission/docker-compose.yml up -d

compose-down:
	APP_IMAGE=$(APP_IMAGE) LB_IMAGE=$(LB_IMAGE) docker compose -f submission/docker-compose.yml down -v

smoke:
	./bench/run-smoke.sh

smoke-build:
	./bench/run-build-smoke.sh

bench:
	./bench/run-official-local.sh

bench-build:
	./bench/run-build-benchmark.sh

bench-diagnostic:
	COMPOSE_FILES=submission/docker-compose.yml:submission/docker-compose.diagnostic.yml ./bench/run-official-local.sh

bench-matrix:
	./bench/run-diagnostic-matrix.sh
