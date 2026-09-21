.PHONY: install
install:
	uv sync --frozen --extra dev $(opts)

.PHONY: lock
lock:
	uv lock

.PHONY: lint
lint:
	uv run ruff check .
	uv run ruff format --check .

.PHONY: format
format:
	uv run ruff check --fix .
	uv run ruff format .

.PHONY: typecheck
typecheck:
	uv run mypy

.PHONY: test
test:
	uv run pytest -m "not integration" $(opts)

.PHONY: run
run:
	uv run warehouse-delivery
