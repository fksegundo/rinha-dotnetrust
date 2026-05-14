.PHONY: help build up down logs test-k6 generate-extended test-k6-extended bench-local bench-extended clean-results

COMPOSE ?= docker compose -f submission/docker-compose.yml -f compose.local.yml
APP_IMAGE ?= rinha-dotnetrust-api:local
LB_IMAGE ?= haproxy:3.0-alpine
K6_IMAGE ?= grafana/k6:latest
OFFICIAL_TEST_DIR ?= ../rinha-de-backend-2026-main/test
RESULTS_DIR ?= test
TEST_DATA_FILE ?= $(OFFICIAL_TEST_DIR)/test-data.json
EXTENDED_FACTOR ?= 2
EXTENDED_MODE ?= neutral
EXTENDED_TEST_DATA_FILE ?= $(RESULTS_DIR)/extended-test-data.json
RINHA_K6_TARGET_RATE ?= 900
RINHA_K6_DURATION ?= 120s
RINHA_K6_MAX_VUS ?= 250

help:
	@echo "Targets:"
	@echo "  build       Build local API image ($(APP_IMAGE))"
	@echo "  up          Start local stack"
	@echo "  down        Stop local stack"
	@echo "  test-k6     Run official k6 workload and write $(RESULTS_DIR)/results.json"
	@echo "  generate-extended Generate larger/reordered test-data JSON"
	@echo "  test-k6-extended  Run k6 with generated extended dataset"
	@echo "  bench-local Build, run stack, execute k6, and stop stack"
	@echo "  bench-extended Build, run stack, execute extended k6, and stop stack"
	@echo "  logs        Follow compose logs"

build:
	@docker build -f submission/Dockerfile -t $(APP_IMAGE) .

up:
	@APP_IMAGE=$(APP_IMAGE) LB_IMAGE=$(LB_IMAGE) $(COMPOSE) up -d --force-recreate
	@echo "Waiting for /ready..."
	@for i in $$(seq 1 60); do \
		if curl -sf http://localhost:9999/ready >/dev/null; then echo "ready"; exit 0; fi; \
		sleep 1; \
	done; \
	echo "service did not become ready"; exit 1

down:
	@APP_IMAGE=$(APP_IMAGE) LB_IMAGE=$(LB_IMAGE) $(COMPOSE) down -v --remove-orphans

logs:
	@$(COMPOSE) logs -f

clean-results:
	@rm -f $(RESULTS_DIR)/results.json $(RESULTS_DIR)/k6_summary.json $(RESULTS_DIR)/test-data.json

test-k6: clean-results
	@test -f "$(TEST_DATA_FILE)" || (echo "Missing $(TEST_DATA_FILE)" && exit 1)
	@mkdir -p $(RESULTS_DIR)
	@cp "$(TEST_DATA_FILE)" "$(RESULTS_DIR)/test-data.json"
	@chmod 777 $(RESULTS_DIR)
	@docker run --rm -i \
		--network host \
		-e RINHA_K6_TARGET_RATE="$(RINHA_K6_TARGET_RATE)" \
		-e RINHA_K6_DURATION="$(RINHA_K6_DURATION)" \
		-e RINHA_K6_MAX_VUS="$(RINHA_K6_MAX_VUS)" \
		-v "$(PWD)/scripts/k6_rinha_test.js:/scripts/test.js:ro" \
		-v "$(PWD)/$(RESULTS_DIR):/test" \
		-w / \
		$(K6_IMAGE) \
		run --summary-trend-stats="p(99)" /scripts/test.js
	@cat $(RESULTS_DIR)/results.json

generate-extended:
	@python3 scripts/generate_extended_test_data.py \
		--input "$(OFFICIAL_TEST_DIR)/test-data.json" \
		--output "$(EXTENDED_TEST_DATA_FILE)" \
		--factor "$(EXTENDED_FACTOR)" \
		--mode "$(EXTENDED_MODE)"

test-k6-extended: generate-extended
	@$(MAKE) test-k6 \
		TEST_DATA_FILE="$(EXTENDED_TEST_DATA_FILE)" \
		RINHA_K6_DURATION=241s \
		RINHA_K6_TARGET_RATE="$(RINHA_K6_TARGET_RATE)" \
		RINHA_K6_MAX_VUS="$(RINHA_K6_MAX_VUS)"

bench-local: build up test-k6 down

bench-extended: build up test-k6-extended down

# Diagnostic bench: keeps containers up after k6, dumps API logs, then tears down.
bench-diag: build up test-k6 capture-logs down

capture-logs:
	@mkdir -p $(RESULTS_DIR)
	@$(COMPOSE) logs --no-color api1 api2 > $(RESULTS_DIR)/diag-api-logs.txt 2>&1 || true
	@echo "API logs captured to $(RESULTS_DIR)/diag-api-logs.txt"
	@echo "Last lines:"
	@tail -30 $(RESULTS_DIR)/diag-api-logs.txt
