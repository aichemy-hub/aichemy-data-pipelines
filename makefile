IMAGE ?= pwiz-watch
SERVICE ?= pwiz-watch
DATA_DIR ?= $(PWD)/data

.PHONY: build up down logs bash once

$(DATA_DIR):
	mkdir -p "$(DATA_DIR)"; mkdir -p "$(DATA_DIR)/mzML"

build:
	docker compose build

up: | $(DATA_DIR)
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

# Quick interactive shell with the bind-mount
bash: | $(DATA_DIR)
	docker run --rm -it -v "$(DATA_DIR):/data" --entrypoint /bin/bash $(IMAGE)

# One-shot without compose
once: | $(DATA_DIR)
	docker run --rm -it -v "$(DATA_DIR):/data" $(IMAGE)