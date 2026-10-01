.PHONY: help sync lint test build publish all

help:
	@echo "Targets:"
	@echo "  sync         uv sync --all-extras --dev"
	@echo "  lint         uv run ruff check && uv run ruff format"
	@echo "  test         uv run pytest -v --cov=./pyrio --cov-fail-under=90 --cov-report=xml"
	@echo "  build        uv build"
	@echo "  publish      uvx uv-publish"
	@echo "  clean        remove ruff/pytest caches"
	@echo "  all          sync lint test"

sync:
	uv sync --all-extras --dev

lint:
	uv run ruff check && uv run ruff format

test:
	uv run pytest -v --cov=./pyrio --cov-fail-under=90 --cov-report=xml

build:
	uv build

publish: build
	uvx uv-publish

clean:
	rm -rf .ruff_cache/ .pytest_cache/

all: sync lint test
