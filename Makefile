.PHONY: help build build-aarch64 build-multiarch test validate lint-docker security-scan push clean \
        start stop restart status logs shell \
        setup env-setup env-validate \
        secrets-generate secrets-generate-ci secrets-rotate secrets-clean secrets-info

IMAGE_NAME ?= $(or $(DOCKERHUB_SINGLE_REPOSITORY),$(USER)/abs-tracked)
VERSION := $(shell cat VERSION)
BUILD_DATE := $(shell date -u +'%Y-%m-%dT%H:%M:%SZ')
COMPOSE_FILE ?= docker-compose.dev.yml
SERVICE ?= abs-tracked

GREEN := \033[0;32m
YELLOW := \033[0;33m
BLUE := \033[0;34m
RED := \033[0;31m
NC := \033[0m

## help: Display this help message
help:
	@echo "$(BLUE)abs-tracked Single-Container Build System$(NC)"
	@echo ""
	@echo "$(GREEN)Available targets:$(NC)"
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' | sed -e 's/^/ /'

## build: Build single-container image for amd64
build:
	docker buildx build \
		--platform linux/amd64 \
		--file Dockerfile \
		--tag $(IMAGE_NAME):latest \
		--tag $(IMAGE_NAME):$(VERSION) \
		--build-arg BUILD_DATE=$(BUILD_DATE) \
		--build-arg VERSION=$(VERSION) \
		--load \
		.

## build-aarch64: Build single-container image for arm64
build-aarch64:
	docker buildx build \
		--platform linux/arm64 \
		--file Dockerfile.aarch64 \
		--tag $(IMAGE_NAME):aarch64-$(VERSION) \
		--build-arg BUILD_DATE=$(BUILD_DATE) \
		--build-arg VERSION=$(VERSION) \
		--load \
		.

## build-multiarch: Build and push multi-arch single image
build-multiarch:
	docker buildx build \
		--platform linux/amd64,linux/arm64 \
		--file Dockerfile \
		--tag $(IMAGE_NAME):latest \
		--tag $(IMAGE_NAME):$(VERSION) \
		--build-arg BUILD_DATE=$(BUILD_DATE) \
		--build-arg VERSION=$(VERSION) \
		--push \
		.

## test: Run quick container smoke test
test:
	@echo "$(GREEN)Running smoke test...$(NC)"
	@docker rm -f abs-tracked-test >/dev/null 2>&1 || true
	@docker run -d --name abs-tracked-test \
		-e MYSQL_ROOT_PASSWORD=testroot \
		-e ABS_DB_NAME=abs_tracked \
		-e ABS_DB_USER=abs_tracked \
		-e ABS_DB_PASSWORD=testdb \
		$(IMAGE_NAME):latest
	@sleep 20
	@docker logs --tail 100 abs-tracked-test
	@docker rm -f abs-tracked-test >/dev/null 2>&1 || true

## validate: Validate Dockerfiles and shell syntax
validate: lint-docker
	@echo "$(GREEN)Check shell scripts$(NC)"
	@find root -type f \( -name "*.sh" -o -name "run" -o -name "finish" -o -name "abs-tracked-*" \) -print0 | xargs -0 -I{} bash -n "{}"
	@echo "$(GREEN)Check UI python syntax$(NC)"
	@python3 -m py_compile ui/abs-tracked-ui/app.py

## lint-docker: Run hadolint across single-container Dockerfiles
lint-docker:
	@echo "$(GREEN)Running hadolint$(NC)"
	@docker run --rm -v "$(PWD):/workspace" -w /workspace hadolint/hadolint \
		hadolint --config .hadolint.yaml Dockerfile Dockerfile.aarch64

## security-scan: Run Trivy image scan
security-scan: build
	@docker run --rm -v /var/run/docker.sock:/var/run/docker.sock aquasec/trivy image --severity HIGH,CRITICAL $(IMAGE_NAME):latest

## push: Push versioned and latest tags
push:
	docker push $(IMAGE_NAME):latest
	docker push $(IMAGE_NAME):$(VERSION)

## clean: Remove local images and build cache
clean:
	docker rmi $(IMAGE_NAME):latest $(IMAGE_NAME):$(VERSION) || true
	docker builder prune -f

## start: Start single-container service from compose example
start:
	docker compose -f $(COMPOSE_FILE) up -d $(SERVICE)

## stop: Stop services
stop:
	docker compose -f $(COMPOSE_FILE) down

## restart: Restart services
restart: stop start

## status: Show compose service status
status:
	docker compose -f $(COMPOSE_FILE) ps

## logs: Tail compose logs
logs:
	docker compose -f $(COMPOSE_FILE) logs -f

## shell: Open shell in container
shell:
	docker compose -f $(COMPOSE_FILE) exec $(SERVICE) /bin/bash

## setup: Prepare env and secrets
setup: env-setup secrets-generate
	@echo "$(GREEN)Setup complete$(NC)"

## env-setup: Create .env from .env.example
env-setup:
	@test ! -f .env && cp .env.example .env || echo "$(YELLOW).env exists, skipping$(NC)"

## env-validate: Validate key env values exist
env-validate:
	@test -f .env || (echo "$(RED).env missing$(NC)" && exit 1)
	@grep -q '^PUID=' .env || echo "$(YELLOW)PUID missing$(NC)"
	@grep -q '^PGID=' .env || echo "$(YELLOW)PGID missing$(NC)"
	@grep -q '^TZ=' .env || echo "$(YELLOW)TZ missing$(NC)"

## secrets-generate: Generate local development secrets
secrets-generate:
	@mkdir -p secrets
	@openssl rand -base64 48 | tr -d '=+/\n' | head -c 32 > secrets/abs_db_password.txt
	@openssl rand -base64 48 | tr -d '=+/\n' | head -c 32 > secrets/mysql_root_password.txt
	@openssl rand -base64 48 | tr -d '=+/\n' | head -c 32 > secrets/ui_secret_key.txt
	@openssl rand -base64 48 | tr -d '=+/\n' | head -c 64 > secrets/ui_token_encryption_key.txt
	@touch secrets/audible_api_bearer_token.txt
	@chmod 600 secrets/*.txt || true
	@echo "$(GREEN)Secrets generated$(NC)"

## secrets-generate-ci: Generate CI secrets (same as local)
secrets-generate-ci: secrets-generate

## secrets-rotate: Rotate secrets and keep one backup snapshot
secrets-rotate:
	@mkdir -p secrets/backup-$(shell date +%Y%m%d-%H%M%S)
	@cp secrets/*.txt secrets/backup-$(shell date +%Y%m%d-%H%M%S)/ 2>/dev/null || true
	@$(MAKE) secrets-generate

## secrets-clean: Keep only latest 5 backups
secrets-clean:
	@cd secrets && ls -dt backup-* 2>/dev/null | tail -n +6 | xargs -r rm -rf

## secrets-info: Show secret file info
secrets-info:
	@ls -la secrets/*.txt 2>/dev/null || echo "No secret files found"
