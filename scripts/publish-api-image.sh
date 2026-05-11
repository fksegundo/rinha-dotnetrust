#!/usr/bin/env bash
set -euo pipefail

APP_IMAGE="${APP_IMAGE:-filonsegundo/rinha-dotnetrust-api:submissionv2}"
RUST_TARGET_CPU="${RUST_TARGET_CPU:-haswell}"
RINHA_NATIVE_LEAF_SIZE="${RINHA_NATIVE_LEAF_SIZE:-192}"

docker buildx build \
  --platform linux/amd64 \
  --build-arg RUST_TARGET_CPU="${RUST_TARGET_CPU}" \
  --build-arg RINHA_NATIVE_LEAF_SIZE="${RINHA_NATIVE_LEAF_SIZE}" \
  -f submission/Dockerfile \
  -t "${APP_IMAGE}" \
  --push \
  .
