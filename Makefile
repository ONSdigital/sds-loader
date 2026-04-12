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

.PHONY: dev-firestore
dev-firestore:
	@echo "🔥 Starting Firestore emulator..."
	docker run \
		--rm \
		-p=9000:9000 \
		-p=8080:8080 \
		-p=4000:4000 \
		-p=9099:9099 \
		-p=8085:8085 \
		-p=5001:5001 \
		-p=9199:9199 \
		--env "GCP_PROJECT=${PROJECT_ID}" \
		--env "ENABLE_UI=true" \
		spine3/firebase-emulator &

	@echo "🚀 Starting dev server with Firestore profile..."

	FIRESTORE_EMULATOR_HOST=localhost:8080 \
	PROFILE=local_storage_firestore \
	uv run run.py

.PHONY: bump
bump:
	@echo "🔼 Bumping project version..."
	uv run --only-group version-check python .github/scripts/bump_version.py
	@echo "🔄 Generating new lock file..."
	uv lock
