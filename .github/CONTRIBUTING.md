# Contributing to abs-tracked

## Before opening a PR

- Make all related changes before opening the PR.
- Follow the PR template.
- Link the issue in your PR when applicable (`closes #<issue>`).
- Keep Dockerfile and Dockerfile.aarch64 aligned for functional changes.
- If startup scripts or container behavior change, update `CHANGELOG.md` and `CHANGELOG.DE.md`.

## Files of interest

| File | Purpose |
| --- | --- |
| `Dockerfile` | Main image definition (amd64 + multi-arch buildx) |
| `Dockerfile.aarch64` | Arm64-specific Dockerfile |
| `docker-compose.example.yml` | Reference deployment |
| `.env.example` | Environment template |
| `root/` | s6 services, init scripts, runtime scripts |
| `ui/abs-tracked-ui/` | Integrated UI application |
| `README.md` / `README.DE.md` | Documentation |
| `CHANGELOG.md` / `CHANGELOG.DE.md` | Change history |

## Local validation

- `make validate`
- `make build`
- `make start`
- `make logs`

