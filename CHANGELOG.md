# Changelog

> 🇩🇪 **[German Version](CHANGELOG.DE.md)** | 📖 **English Version**

All notable changes to this project are documented in this file.

## [Unreleased]

### Added
- _No entries yet._

### Changed
- _No entries yet._

### Fixed
- _No entries yet._

## [0.1.1] - 2026-02-24

### Added
- Manual "Rebuild Progress from ABS" action in Sync settings.
- Manual "Clean Collected Library" action in Sync settings.
- Dedicated Hadolint configuration (`.hadolint.yaml`) and workflow (`.github/workflows/hadolint.yml`) for all Dockerfiles.
- Repository-wide single-container-only deployment mode (`abs-tracked`) in compose, env, Makefile, and workflows.
- GitHub community standards inspired by LSIO `docker-freshrss`: issue templates, PR template, and contributing guide.
- `greetings.yml` workflow for first-time issue/PR messages.
- Automated `SECURITY.md` synchronization workflow (`.github/workflows/security-policy-sync.yml`) based on `VERSION`.

### Changed
- Dark mode contrast refresh for navigation, cards, stats widgets and forms.
- Header brand title simplified to `abs-tracked` (without tracker suffix).
- Navigation reduced to `Home`, `Audiobooks`, `Podcasts`, `Settings` (history removed from top menu).
- Deployment defaults now rely on UI-managed ABS accounts and `targets.json`.
- Docker release workflow now publishes only the single image `abs-tracked`.
- Docker release workflow now supports both manual dispatch and tag-push triggers (`v*`).
- CI/Security/Hadolint pipelines validate single-container Dockerfiles only.
- Make defaults use `abs-tracked` and single-container compose flows only.
- Legacy multi-container compose services (`abs-tracked-db`, `abs-tracked-ui`) and legacy Dockerfiles were removed from active maintenance paths.
- Weblate/Crowdin integration files and references were removed from the repository.
- `permissions.yml` now performs executable permission checks for init/service scripts via LSIO reusable workflow.
- Sync Security Policy workflow YAML was fixed so scheduled/push runs execute correctly.

### Fixed
- Sync settings form nesting issue (`cancel edit` no longer uses nested `<form>`).
- Runtime sync warning now points to UI account setup / `targets.json` only.

## [0.1.0] - 2026-02-23

### Added
- LinuxServer-style repository structure (`Dockerfile`, `Dockerfile.aarch64`, `root/` at top-level).
- Split documentation files (`README.md`, `README.DE.md`, `CHANGELOG.md`, `CHANGELOG.DE.md`).
- Optional abs-tracked UI service (`ui/abs-tracked-ui`) for browsing latest/history sync data.
- Multi-target sync foundation for multiple ABS servers/users in one container.
- Composite identity model in DB: `target_id + user_id + library_item_id + episode_id`.
- Canonical key matching bridge (`ASIN` -> `ISBN` -> `title+author+duration`).
- Cross-target finished-state propagation using canonical keys and shared `principalId`.
- Target library identity indexing for migration scenarios.
- Automation baseline: `Makefile` and `VERSION`.
- Dependabot configuration (`.github/dependabot.yml`).
- CI workflow (`.github/workflows/ci.yml`).
- Security workflow (`.github/workflows/security.yml`).
- Manual Docker publish workflow for Docker Hub + GHCR (`.github/workflows/docker-release.yml`).
- Workflow permissions check (`.github/workflows/permissions.yml`).

### Changed
- Docker compose and env templates moved to repository root.
- Sync engine supports target profiles from `/config/app/targets.json`.
- Matching strategy is now configurable via `ABS_MATCH_PRIORITY`.

### Fixed
- Backup write behavior is atomic (`.tmp` + move).
- Sync schema avoids reserved SQL naming conflicts.
