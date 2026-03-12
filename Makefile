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
	@echo "Running Unit Tests..."
	uv run --dev pytest -v --disable-warnings tests/

.PHONY: test-parallel
test-parallel:
	@echo "Running Unit Tests..."
	uv run --dev pytest -n auto -v --disable-warnings tests/

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
