SHELL := /bin/bash
.DEFAULT_GOAL := help

ENV_FILE        := .env
TRACES_FRAGMENT := .env.traces
TRACES_ENV      := .env.traces.generated

OTEL_TUI_IMAGE := ymtdzzz/otel-tui:v0.7.3

.PHONY: help otel-tui start-traces clean

help:
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-13s\033[0m %s\n", $$1, $$2}'

otel-tui: ## Terminal OTLP viewer, foreground: the spans and their gen_ai.* attributes
	@docker rm -f otel-tui >/dev/null 2>&1 || true
	docker run --rm -it --name otel-tui -p 4317:4317 -p 4318:4318 $(OTEL_TUI_IMAGE)

# .env holds the secrets and is git-ignored; .env.traces holds only flags and is
# committed. Merging them into a throwaway copy keeps the secrets in one place and
# leaves .env untouched.
$(TRACES_ENV): $(ENV_FILE) $(TRACES_FRAGMENT)
	@{ cat $(ENV_FILE); printf '\n'; cat $(TRACES_FRAGMENT); } > $@
	@chmod 600 $@

start-traces: $(TRACES_ENV) ## Start Airflow with OTel tracing on (start the viewer first)
	@if ! nc -z localhost 4317 >/dev/null 2>&1; then \
		echo "WARNING: nothing is listening on localhost:4317."; \
		echo "         Run 'make otel-tui' in another terminal first,"; \
		echo "         or every task will stall for up to 5s flushing spans into the void."; \
		echo; \
	fi
	@# restart, not start: works whether or not Airflow is already up.
	astro dev restart --env $(TRACES_ENV)

clean: ## Stop the viewer and delete the generated env file (it holds a copy of .env)
	@docker rm -f otel-tui >/dev/null 2>&1 || true
	@rm -f $(TRACES_ENV)
