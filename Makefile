.PHONY: help sync lint test all

help:
	@echo "Targets:"
	@echo "  sync         uv sync --all-extras --dev"
	@echo "  lint         uv run ruff check && uv run ruff format"
	@echo "  test         uv run pytest -v --cov=./pyrio --cov-fail-under=90 --cov-report=xml"
	@echo "  all          sync lint test"

sync:
	uv sync --all-extras --dev

lint:
	uv run ruff check && uv run ruff format

test:
	uv run pytest -v --cov=./pyrio --cov-fail-under=90 --cov-report=xml

build:
	uv build

all: sync lint test
