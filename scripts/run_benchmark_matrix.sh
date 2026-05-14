#!/usr/bin/env bash
set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT" || exit 1

LEAF_SIZES="${LEAF_SIZES:-128 192 256 384 512 768 1024}"
MAX_LEAF_VISITS_LIST="${MAX_LEAF_VISITS_LIST:-0 2 4 8 16}"
MATRIX_NAME="${MATRIX_NAME:-matrix}"
APP_IMAGE_PREFIX="${APP_IMAGE_PREFIX:-rinha-dotnetrust-api}"
LB_IMAGE="${LB_IMAGE:-rinha-lb:baseline}"
RESULTS_ROOT="${RESULTS_ROOT:-test/matrix-${MATRIX_NAME}-$(date +%Y%m%d-%H%M%S)}"

mkdir -p "$RESULTS_ROOT"

printf 'matrix=%s\nleaf_sizes=%s\nmax_leaf_visits=%s\n' \
  "$MATRIX_NAME" "$LEAF_SIZES" "$MAX_LEAF_VISITS_LIST" > "$RESULTS_ROOT/metadata.txt"

make build-lb LB_IMAGE="$LB_IMAGE"

for leaf in $LEAF_SIZES; do
  app_image="${APP_IMAGE_PREFIX}:${MATRIX_NAME}-leaf${leaf}"
  make build APP_IMAGE="$app_image" RINHA_NATIVE_LEAF_SIZE="$leaf"

  for max_leaf_visits in $MAX_LEAF_VISITS_LIST; do
    run_name="leaf${leaf}-max${max_leaf_visits}"
    run_dir="$RESULTS_ROOT/$run_name"
    mkdir -p "$run_dir"

    echo "=== $run_name ==="
    printf 'leaf_size=%s\nmax_leaf_visits=%s\napp_image=%s\nlb_image=%s\n' \
      "$leaf" "$max_leaf_visits" "$app_image" "$LB_IMAGE" > "$run_dir/config.txt"

    if make bench-diag \
      APP_IMAGE="$app_image" \
      LB_IMAGE="$LB_IMAGE" \
      RINHA_NATIVE_LEAF_SIZE="$leaf" \
      RINHA_MAX_LEAF_VISITS="$max_leaf_visits" \
      RESULTS_DIR="test/current-run"; then
      cp test/current-run/results.json "$run_dir/results.json" 2>/dev/null || true
      cp test/current-run/k6_summary.json "$run_dir/k6_summary.json" 2>/dev/null || true
      cp test/current-run/diag-api-logs.txt "$run_dir/diag-api-logs.txt" 2>/dev/null || true
      cp test/current-run/docker-stats.txt "$run_dir/docker-stats.txt" 2>/dev/null || true
      echo "ok" > "$run_dir/status.txt"
    else
      cp test/current-run/results.json "$run_dir/results.json" 2>/dev/null || true
      cp test/current-run/k6_summary.json "$run_dir/k6_summary.json" 2>/dev/null || true
      cp test/current-run/diag-api-logs.txt "$run_dir/diag-api-logs.txt" 2>/dev/null || true
      cp test/current-run/docker-stats.txt "$run_dir/docker-stats.txt" 2>/dev/null || true
      echo "failed" > "$run_dir/status.txt"
    fi
  done
done

python3 scripts/summarize_matrix.py "$RESULTS_ROOT"
