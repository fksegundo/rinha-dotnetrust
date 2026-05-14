#!/usr/bin/env bash
set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT" || exit 1

MATRIX_NAME="${MATRIX_NAME:-resource-fd}"
APP_IMAGE="${APP_IMAGE:-rinha-dotnetrust-api:resource-fd}"
LB_IMAGE="${LB_IMAGE:-rinha-lb:resource-fd}"
RESULTS_ROOT="${RESULTS_ROOT:-test/matrix-${MATRIX_NAME}-$(date +%Y%m%d-%H%M%S)}"
COMPOSE_FILES="${COMPOSE_FILES:-docker compose -f submission/docker-compose.yml -f compose.local.yml -f compose.fd-passing.yml}"
RINHA_NATIVE_LEAF_SIZE="${RINHA_NATIVE_LEAF_SIZE:-192}"
RINHA_MAX_LEAF_VISITS="${RINHA_MAX_LEAF_VISITS:-0}"

CASES="${CASES:-\
baseline,0.40,0.40,0.20,155MB,155MB,40MB
lb-light,0.425,0.425,0.15,155MB,155MB,40MB
lb-min,0.45,0.45,0.10,160MB,160MB,30MB
lb-heavy,0.375,0.375,0.25,155MB,155MB,40MB
api-mem-heavy,0.40,0.40,0.20,165MB,165MB,20MB
api-cpu-mem-heavy,0.425,0.425,0.15,165MB,165MB,20MB
api-max,0.475,0.475,0.05,165MB,165MB,20MB
api-mem-max,0.425,0.425,0.15,170MB,170MB,10MB}"

mkdir -p "$RESULTS_ROOT"

{
  printf 'matrix=%s\n' "$MATRIX_NAME"
  printf 'app_image=%s\n' "$APP_IMAGE"
  printf 'lb_image=%s\n' "$LB_IMAGE"
  printf 'compose=%s\n' "$COMPOSE_FILES"
  printf 'rinha_native_leaf_size=%s\n' "$RINHA_NATIVE_LEAF_SIZE"
  printf 'rinha_max_leaf_visits=%s\n' "$RINHA_MAX_LEAF_VISITS"
  printf 'cases=name,api1_cpus,api2_cpus,lb_cpus,api1_memory,api2_memory,lb_memory\n%s\n' "$CASES"
} > "$RESULTS_ROOT/metadata.txt"
printf '%s\n' "$CASES" > "$RESULTS_ROOT/cases.csv"

make build-lb LB_IMAGE="$LB_IMAGE" || exit 1
make build APP_IMAGE="$APP_IMAGE" RINHA_NATIVE_LEAF_SIZE="$RINHA_NATIVE_LEAF_SIZE" || exit 1

while IFS=, read -r name api1_cpus api2_cpus lb_cpus api1_memory api2_memory lb_memory; do
  [ -n "$name" ] || continue
  run_dir="$RESULTS_ROOT/$name"
  mkdir -p "$run_dir"

  echo "=== $name ==="
  {
    printf 'api1_cpus=%s\n' "$api1_cpus"
    printf 'api2_cpus=%s\n' "$api2_cpus"
    printf 'lb_cpus=%s\n' "$lb_cpus"
    printf 'api1_memory=%s\n' "$api1_memory"
    printf 'api2_memory=%s\n' "$api2_memory"
    printf 'lb_memory=%s\n' "$lb_memory"
    printf 'app_image=%s\n' "$APP_IMAGE"
    printf 'lb_image=%s\n' "$LB_IMAGE"
  } > "$run_dir/config.txt"

  if API1_CPUS="$api1_cpus" \
    API2_CPUS="$api2_cpus" \
    LB_CPUS="$lb_cpus" \
    API1_MEMORY="$api1_memory" \
    API2_MEMORY="$api2_memory" \
    LB_MEMORY="$lb_memory" \
    make bench-diag \
      APP_IMAGE="$APP_IMAGE" \
      LB_IMAGE="$LB_IMAGE" \
      RINHA_NATIVE_LEAF_SIZE="$RINHA_NATIVE_LEAF_SIZE" \
      RINHA_MAX_LEAF_VISITS="$RINHA_MAX_LEAF_VISITS" \
      COMPOSE="$COMPOSE_FILES" \
      RESULTS_DIR="test/current-run" </dev/null; then
    echo "ok" > "$run_dir/status.txt"
  else
    echo "failed" > "$run_dir/status.txt"
  fi

  cp test/current-run/results.json "$run_dir/results.json" 2>/dev/null || true
  cp test/current-run/k6_summary.json "$run_dir/k6_summary.json" 2>/dev/null || true
  cp test/current-run/diag-api-logs.txt "$run_dir/diag-api-logs.txt" 2>/dev/null || true
  cp test/current-run/docker-stats.txt "$run_dir/docker-stats.txt" 2>/dev/null || true
done < "$RESULTS_ROOT/cases.csv"

python3 scripts/summarize_resource_matrix.py "$RESULTS_ROOT"
