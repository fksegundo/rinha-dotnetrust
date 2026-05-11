#!/usr/bin/env bash
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${BENCH_DIR}/.." && pwd)"
OFFICIAL_REPO="${OFFICIAL_REPO:-${REPO_ROOT}/../rinha-de-backend-2026-main}"
K6_IMAGE="${K6_IMAGE:-grafana/k6:latest}"
READY_URL="${READY_URL:-http://localhost:9999/ready}"
READY_MAX_ATTEMPTS="${READY_MAX_ATTEMPTS:-90}"
READY_CONSECUTIVE_SUCCESSES="${READY_CONSECUTIVE_SUCCESSES:-5}"
READY_SLEEP_SECONDS="${READY_SLEEP_SECONDS:-1}"
COMPOSE_FILES="${COMPOSE_FILES:-submission/docker-compose.yml}"
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-rinha-dotnetrust}"

compose_file_args() {
    local args=()
    local files=()
    IFS=':' read -r -a files <<<"${COMPOSE_FILES}"
    for file in "${files[@]}"; do
        args+=("-f" "${REPO_ROOT}/${file}")
    done
    printf '%s\n' "${args[@]}"
}

compose_cmd() {
    local args=()
    mapfile -t args < <(compose_file_args)
    COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME}" \
    APP_IMAGE="${APP_IMAGE:-filonsegundo/rinha-dotnetrust-api:submissionv2}" \
    LB_IMAGE="${LB_IMAGE:-filonsegundo/rinha-dotnetrust-lb:submission}" \
    docker compose "${args[@]}" "$@"
}

wait_ready_stable() {
    local consecutive=0
    local attempt=0
    while (( attempt < READY_MAX_ATTEMPTS )); do
        if curl -fsS "${READY_URL}" >/dev/null; then
            consecutive=$((consecutive + 1))
            if (( consecutive >= READY_CONSECUTIVE_SUCCESSES )); then
                return 0
            fi
        else
            consecutive=0
        fi
        attempt=$((attempt + 1))
        sleep "${READY_SLEEP_SECONDS}"
    done
    return 1
}

prepare_k6_workdir() {
    local target_dir="$1"
    rm -rf "${target_dir}"
    mkdir -p "${target_dir}"
    mkdir -p "${target_dir}/test"
    chmod 0777 "${target_dir}" "${target_dir}/test"
    cp "${OFFICIAL_REPO}/test/test.js" "${target_dir}/test.js"
    cp "${OFFICIAL_REPO}/test/smoke.js" "${target_dir}/smoke.js"
    cp "${OFFICIAL_REPO}/test/test-data.json" "${target_dir}/test-data.json"
}

run_k6_in_docker() {
    local script_name="$1"
    local workdir="$2"
    docker run --rm --network host \
        --user "$(id -u):$(id -g)" \
        -v "${workdir}:/repo/test" \
        -w /repo/test \
        "${K6_IMAGE}" run "${script_name}"
}

collect_runtime_evidence() {
    local output_dir="$1"
    mkdir -p "${output_dir}"

    compose_cmd config > "${output_dir}/compose.resolved.yml"
    compose_cmd ps --format json > "${output_dir}/compose.ps.json"

    mapfile -t container_ids < <(compose_cmd ps -q)
    if ((${#container_ids[@]} > 0)); then
        docker inspect "${container_ids[@]}" > "${output_dir}/containers.inspect.json"
        docker stats --no-stream --format json "${container_ids[@]}" > "${output_dir}/docker.stats.jsonl"
    fi

    if docker image inspect "${APP_IMAGE:-filonsegundo/rinha-dotnetrust-api:submissionv2}" >/dev/null 2>&1; then
        docker image inspect \
            "${APP_IMAGE:-filonsegundo/rinha-dotnetrust-api:submissionv2}" \
            "${LB_IMAGE:-filonsegundo/rinha-dotnetrust-lb:submission}" \
            > "${output_dir}/images.inspect.json"
    fi

    while IFS= read -r service; do
        compose_cmd logs --no-color "${service}" > "${output_dir}/${service}.log" || true
    done < <(compose_cmd config --services)
}
