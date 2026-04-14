SHELL := bash
.ONESHELL:


.PHONY: lint
lint:
	@echo "Running Ruff linter..."
	uv run --only-group lint ruff check --fix


.PHONY: format
format:
	@echo "Running Ruff formatter..."
	uv run --only-group lint ruff format


.PHONY: test
test:
	@echo "Running Local Tests..."
	uv run --dev pytest -v --disable-warnings tests/


.PHONY: install
install: ## Install dependencies
	uv sync


.PHONY: dev
dev:
	@echo "Starting development server..."
	uv run run.py


.PHONY: bump
bump:
	@echo "🔼 Bumping project version..."
	uv run --only-group version-check python .github/scripts/bump_version.py
	@echo "🔄 Generating new lock file..."
	uv lock
