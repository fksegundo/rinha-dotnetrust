#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/lib.sh"

MATRIX_FILE="${MATRIX_FILE:-${REPO_ROOT}/bench/diagnostic-matrix.tsv}"
MATRIX_RUN_ID="${MATRIX_RUN_ID:-matrix-$(date +%Y%m%d-%H%M%S)}"
MATRIX_ROOT="${MATRIX_ROOT:-${REPO_ROOT}/artifacts/matrix/${MATRIX_RUN_ID}}"
SUMMARY_FILE="${MATRIX_ROOT}/summary.tsv"

mkdir -p "${MATRIX_ROOT}"

printf 'scenario\tp99_ms\tfinal_score\tfp\tfn\thttp_errors\tlb_cpus\tapi1_cpus\tapi2_cpus\tlb_cpuset\tapi1_cpuset\tapi2_cpuset\tlb_workers\tlb_buf_size\tmin_threads\trinha_probes\tmax_candidates\tnative_fast_probes\tnative_full_probes\tartifact_dir\n' > "${SUMMARY_FILE}"

tail -n +2 "${MATRIX_FILE}" | while IFS=$'\t' read -r scenario lb_cpus api1_cpus api2_cpus lb_cpuset api1_cpuset api2_cpuset lb_workers lb_buf_size min_threads rinha_probes max_candidates native_fast_probes native_full_probes; do
    if [[ -z "${scenario}" ]]; then
        continue
    fi

    artifact_dir="${MATRIX_ROOT}/${scenario}"
    run_log="${artifact_dir}.stdout.log"
    mkdir -p "${artifact_dir}"

    echo "==> ${scenario}"

    env \
        COMPOSE_FILES="submission/docker-compose.yml:submission/docker-compose.diagnostic.yml" \
        RUN_ID="${scenario}" \
        ARTIFACT_DIR="${artifact_dir}" \
        LB_CPUS="${lb_cpus}" \
        API1_CPUS="${api1_cpus}" \
        API2_CPUS="${api2_cpus}" \
        LB_CPUSET="${lb_cpuset}" \
        API1_CPUSET="${api1_cpuset}" \
        API2_CPUSET="${api2_cpuset}" \
        LB_WORKERS="${lb_workers}" \
        LB_BUF_SIZE="${lb_buf_size}" \
        RINHA_MIN_THREADS="${min_threads}" \
        RINHA_PROBES="${rinha_probes}" \
        RINHA_MAX_CANDIDATES_PER_CENTER="${max_candidates}" \
        RINHA_NATIVE_FAST_PROBES="${native_fast_probes}" \
        RINHA_NATIVE_FULL_PROBES="${native_full_probes}" \
        bash "${REPO_ROOT}/bench/run-official-local.sh" \
        > "${run_log}"

    p99_ms="$(jq -r '.p99 | sub("ms$"; "")' "${artifact_dir}/results.json")"
    final_score="$(jq -r '.scoring.final_score' "${artifact_dir}/results.json")"
    fp="$(jq -r '.scoring.breakdown.false_positive_detections' "${artifact_dir}/results.json")"
    fn="$(jq -r '.scoring.breakdown.false_negative_detections' "${artifact_dir}/results.json")"
    http_errors="$(jq -r '.scoring.breakdown.http_errors' "${artifact_dir}/results.json")"

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "${scenario}" \
        "${p99_ms}" \
        "${final_score}" \
        "${fp}" \
        "${fn}" \
        "${http_errors}" \
        "${lb_cpus}" \
        "${api1_cpus}" \
        "${api2_cpus}" \
        "${lb_cpuset}" \
        "${api1_cpuset}" \
        "${api2_cpuset}" \
        "${lb_workers}" \
        "${lb_buf_size}" \
        "${min_threads}" \
        "${rinha_probes}" \
        "${max_candidates}" \
        "${native_fast_probes}" \
        "${native_full_probes}" \
        "${artifact_dir}" \
        >> "${SUMMARY_FILE}"
done

column -t -s $'\t' "${SUMMARY_FILE}"
