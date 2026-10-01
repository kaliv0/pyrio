.PHONY: help sync lint test docs-build docs-open build publish all

help:
	@echo "Targets:"
	@echo "  sync         uv sync --all-extras --dev"
	@echo "  lint         uv run ruff check && uv run ruff format"
	@echo "  test         uv run pytest -v --cov=./pyrio --cov-fail-under=90 --cov-report=xml"
	@echo "  build        uv build"
	@echo "  docs         (build and open sphinx docs)"
	@echo "  build        uv build"
	@echo "  publish      uvx uv-publish"
	@echo "  clean        (remove ruff/pytest caches, doc builds and dist/)"
	@echo "  all          sync lint test"

sync:
	uv sync --all-extras --dev

lint:
	uv run ruff check && uv run ruff format

test:
	uv run pytest -v --cov=./pyrio --cov-fail-under=90 --cov-report=xml

docs-build:
	uv run sphinx-build -b html docs docs/_build/html

docs: docs-build
	@CMD=$$(command -v xdg-open || command -v open || command -v start); \
	$$CMD docs/_build/html/index.html

build:
	uv build

publish: build
	uvx uv-publish

clean:
	rm -rf .ruff_cache/ .pytest_cache/ docs/_build .pdm-build/ dist/

all: sync lint test
