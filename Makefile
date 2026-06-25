DOCKERFILE ?= Dockerfile
IMAGE = $(shell grep -m1 '^FROM' $(DOCKERFILE) | awk '{print $$2}')

.PHONY: venv
venv:
	@docker image inspect $(IMAGE) >/dev/null 2>&1 || docker pull $(IMAGE)
	@AIRFLOW_VERSION=$$(docker inspect $(IMAGE) --format '{{ index .Config.Labels "io.astronomer.docker.airflow.version" }}'); \
	AIRFLOW_BASE=$${AIRFLOW_VERSION%%+*}; \
	PYTHON_VERSION=$$(docker run --rm $(IMAGE) python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")' | tail -1); \
	CONSTRAINTS="https://raw.githubusercontent.com/apache/airflow/constraints-$$AIRFLOW_BASE/constraints-$$PYTHON_VERSION.txt"; \
	echo "Airflow $$AIRFLOW_VERSION (base $$AIRFLOW_BASE) on Python $$PYTHON_VERSION"; \
	uv venv --python "$$PYTHON_VERSION"; \
	uv pip install "apache-airflow==$$AIRFLOW_BASE" --constraint "$$CONSTRAINTS"; \
	[ -f requirements.txt ] && uv pip install -r requirements.txt --constraint "$$CONSTRAINTS" || echo "no requirements.txt, skipping"
	@$(MAKE) interpreter

.PHONY: interpreter
interpreter:
	@echo ""
	@echo "VS Code interpreter path:"
	@echo "  $(CURDIR)/.venv/bin/python"

.PHONY: clean
clean:
	@rm -rf .venv
	@echo "Removed .venv"
