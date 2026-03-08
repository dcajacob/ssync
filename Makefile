SHELL := /bin/bash

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

.PHONY: help deploy deploy-dry-run deploy-delete

help:
	@echo "Targets:"
	@echo "  make deploy         # Sync repo to $(REMOTE):$(REMOTE_DIR)"
	@echo "  make deploy-dry-run # Show what would be synced"
	@echo "  make deploy-delete  # Sync and delete remote files not present locally"
	@echo ""
	@echo "Overrides:"
	@echo "  make deploy REMOTE=my-host REMOTE_DIR=~/GIT/ssync"

deploy:
	$(RSYNC) $(RSYNC_FLAGS) $(RSYNC_FILTERS) ./ "$(REMOTE):$(REMOTE_DIR)/"

deploy-dry-run:
	$(RSYNC) $(RSYNC_FLAGS) --dry-run $(RSYNC_FILTERS) ./ "$(REMOTE):$(REMOTE_DIR)/"

deploy-delete:
	$(RSYNC) $(RSYNC_FLAGS) --delete $(RSYNC_FILTERS) ./ "$(REMOTE):$(REMOTE_DIR)/"
