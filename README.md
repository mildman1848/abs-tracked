# abs-tracked - LinuxServer.io Style Edition

> 🇩🇪 **[German Version](README.DE.md)** | 📖 **English Version**

[![GitHub Tag](https://img.shields.io/github/v/tag/mildman1848/abs-tracked?style=for-the-badge&logo=github&color=005AA4)](https://github.com/mildman1848/abs-tracked/tags)
[![Docker Hub Pulls](https://img.shields.io/docker/pulls/mildman1848/abs-tracked?style=for-the-badge&logo=docker&logoColor=fff&color=005AA4)](https://hub.docker.com/r/mildman1848/abs-tracked)
[![Docker Image Size](https://img.shields.io/docker/image-size/mildman1848/abs-tracked/latest?style=for-the-badge&logo=docker&logoColor=fff&color=005AA4)](https://hub.docker.com/r/mildman1848/abs-tracked)
[![License](https://img.shields.io/github/license/mildman1848/abs-tracked?style=for-the-badge&color=005AA4)](https://github.com/mildman1848/abs-tracked/blob/main/LICENSE)

[![CI Status](https://img.shields.io/github/actions/workflow/status/mildman1848/abs-tracked/ci.yml?branch=main&style=flat-square&logo=github&label=CI)](https://github.com/mildman1848/abs-tracked/actions/workflows/ci.yml)
[![Hadolint](https://img.shields.io/github/actions/workflow/status/mildman1848/abs-tracked/hadolint.yml?branch=main&style=flat-square&logo=docker&label=Hadolint)](https://github.com/mildman1848/abs-tracked/actions/workflows/hadolint.yml)
[![Security Scan](https://img.shields.io/github/actions/workflow/status/mildman1848/abs-tracked/security.yml?branch=main&style=flat-square&logo=github&label=Security)](https://github.com/mildman1848/abs-tracked/actions/workflows/security.yml)
[![Docker Release](https://img.shields.io/github/actions/workflow/status/mildman1848/abs-tracked/docker-release.yml?branch=main&style=flat-square&logo=docker&label=Release)](https://github.com/mildman1848/abs-tracked/actions/workflows/docker-release.yml)
[![Version](https://img.shields.io/badge/version-v0.1.1-blue?style=flat-square&logo=github)](VERSION)

---

**Single-container Audiobookshelf history tracker with LinuxServer-style Alpine baseimage, s6-overlay services, embedded MariaDB, and integrated UI with two-way ABS sync.**

## 📦 Available Registries

```bash
# Docker Hub
docker pull mildman1848/abs-tracked:latest

# GHCR
docker pull ghcr.io/mildman1848/abs-tracked:latest
```

## Quick Start

```bash
cp .env.example .env

printf '%s' 'your-db-password' > secrets/abs_db_password.txt
printf '%s' 'your-mariadb-root-password' > secrets/mysql_root_password.txt
printf '%s' 'your-ui-secret-key' > secrets/ui_secret_key.txt
printf '%s' 'your-ui-token-encryption-key' > secrets/ui_token_encryption_key.txt

# Run from Docker Hub image (DB + sync + UI)
docker compose -f docker-compose.example.yml up -d abs-tracked

# Local development build
docker compose -f docker-compose.dev.yml up -d --build abs-tracked
```

UI: `http://<host>:8080`

## 🔐 Secret Management

abs-tracked follows LinuxServer-style `FILE__` secret handling.

Examples:
- `FILE__ABS_DB_PASSWORD=/run/secrets/abs_db_password`
- `FILE__MYSQL_ROOT_PASSWORD=/run/secrets/mysql_root_password`
- `FILE__UI_SECRET_KEY=/run/secrets/ui_secret_key`
- `FILE__UI_TOKEN_ENCRYPTION_KEY=/run/secrets/ui_token_encryption_key`
- `FILE__AUDIBLE_API_BEARER_TOKEN=/run/secrets/audible_api_bearer_token`

Template secret files are provided in `secrets/*.txt.example`.

## Build & Test

```bash
make help
make build
make start
make logs
make test
make security-scan
```

## Make Targets

```bash
make setup
make env-setup
make env-validate
make secrets-generate
make secrets-info
make secrets-rotate
make build
make build-aarch64
make build-multiarch
make start
make stop
make restart
make status
make logs
make shell
```

## Documentation

- German README: [`README.DE.md`](README.DE.md)
- English changelog: [`CHANGELOG.md`](CHANGELOG.md)
- German changelog: [`CHANGELOG.DE.md`](CHANGELOG.DE.md)
- Translation guide: [`i18n/TRANSLATIONS.md`](i18n/TRANSLATIONS.md)

## Original Project Context

abs-tracked syncs against [Audiobookshelf](https://github.com/advplyr/audiobookshelf) instances via API and extends them with persistent, cross-server history tracking.
