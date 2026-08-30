SHELL := /bin/bash

UV ?= uv
BUILD_FLAGS ?=
DIST_DIR ?= dist

REMOTE ?= eoi-wsl
REMOTE_DIR ?= ~/GIT/ssync
RSYNC ?= rsync
RSYNC_FLAGS ?= -az --info=stats2,progress2
RSYNC_FILTERS ?= \
	--exclude ".git/" \
	--exclude ".venv/" \
	--exclude "__pycache__/" \
	--exclude "*.pyc" \
	--exclude "artifacts/" \
	--exclude "ssync-loopback-*.pcap"

.PHONY: help wheel build deploy deploy-dry-run deploy-delete

help:
	@echo "Targets:"
	@echo "  make wheel          # Build wheel + source archive in $(DIST_DIR)/"
	@echo "  make build          # Alias for make wheel"
	@echo "  make deploy         # Sync repo to $(REMOTE):$(REMOTE_DIR)"
	@echo "  make deploy-dry-run # Show what would be synced"
	@echo "  make deploy-delete  # Sync and delete remote files not present locally"
	@echo ""
	@echo "Overrides:"
	@echo "  make wheel DIST_DIR=artifacts/dist BUILD_FLAGS=--offline"
	@echo "  make wheel UV=/path/to/uv"
	@echo "  make deploy REMOTE=my-host REMOTE_DIR=~/GIT/ssync"

wheel:
	# Build via an sdist so stale build/ files cannot leak into the wheel.
	$(UV) build $(BUILD_FLAGS) --out-dir "$(DIST_DIR)"

build: wheel

deploy:
	$(RSYNC) $(RSYNC_FLAGS) $(RSYNC_FILTERS) ./ "$(REMOTE):$(REMOTE_DIR)/"

deploy-dry-run:
	$(RSYNC) $(RSYNC_FLAGS) --dry-run $(RSYNC_FILTERS) ./ "$(REMOTE):$(REMOTE_DIR)/"

deploy-delete:
	$(RSYNC) $(RSYNC_FLAGS) --delete $(RSYNC_FILTERS) ./ "$(REMOTE):$(REMOTE_DIR)/"
