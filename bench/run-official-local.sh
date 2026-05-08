#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/lib.sh"

RUN_ID="${RUN_ID:-$(date +%Y%m%d-%H%M%S)}"
ARTIFACT_DIR="${ARTIFACT_DIR:-${REPO_ROOT}/artifacts/bench/${RUN_ID}}"
WORKDIR="${REPO_ROOT}/test-workdir/${RUN_ID}"

mkdir -p "${ARTIFACT_DIR}"

compose_cmd down -v --remove-orphans || true
compose_cmd pull
compose_cmd up -d
wait_ready_stable

prepare_k6_workdir "${WORKDIR}"
run_k6_in_docker "test.js" "${WORKDIR}"
cp "${WORKDIR}/test/results.json" "${ARTIFACT_DIR}/results.json"
collect_runtime_evidence "${ARTIFACT_DIR}"

cat "${ARTIFACT_DIR}/results.json"
