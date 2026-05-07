#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../submission"
docker compose up -d --build
for _ in $(seq 1 30); do
  if curl -fsS http://localhost:9999/ready >/dev/null; then
    break
  fi
  sleep 1
done
cd ../../rinha-de-backend-2026-main/test
k6 run smoke.js
