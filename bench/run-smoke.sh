#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/lib.sh"

WORKDIR="${REPO_ROOT}/test-workdir/smoke"

compose_cmd down -v --remove-orphans || true
compose_cmd pull
compose_cmd up -d
wait_ready_stable

prepare_k6_workdir "${WORKDIR}"
run_k6_in_docker "smoke.js" "${WORKDIR}"
