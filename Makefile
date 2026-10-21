FRONTEND := frontend
API      := backend/core_api
AGENT    := backend/core_agent
VAULT    := backend/data-vault

_NPM_STAMP := $(FRONTEND)/node_modules/.package-lock.json
_API_STAMP  := $(API)/.venv
_AGENT_STAMP := $(AGENT)/.venv

# ── Feature-branch selection ─────────────────────────────────────────────
# Per-developer ref choice lives in `dev.env` (gitignored — never committed),
# so each dev works on their own module branches without touching shared
# files or CI. Example dev.env:
#     REF=feat/my-thing            # all modules…
#     API_REF=feat/server-thing    # …or override per module
-include dev.env

REF          ?= main
API_REF      ?= $(REF)
AGENT_REF    ?= $(REF)
FRONTEND_REF ?= $(REF)

# Both run paths follow the same refs:
#   • `make dev`/`dev-web` run the LOCAL submodule source, so they follow
#     whatever branch is checked out (use `make use` to switch).
#   • the Electron desktop server is a uv-tool install keyed by these env
#     vars (frontend/src/main/server-source.ts) — exported here so `make app`
#     / `make server` install the matching branch.
export COWORK_SERVER_REF := $(API_REF)
export ANTON_REF := $(AGENT_REF)
# Only inject a non-default anton override (uv rejects a redundant --with).
_ANTON_WITH := $(if $(filter-out main,$(AGENT_REF)),--with "anton-agent @ git+https://github.com/mindsdb/anton.git@$(AGENT_REF)",)

.PHONY: help setup dev dev-web build dist-mac dist-win docker-build docker-up docker-down flush use pin baseline server server-local app app-local pack-local watch refs

.DEFAULT_GOAL := help

help:   ## list available targets
	@echo "Minds Platform — make targets:"
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| sort \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Module branches: set refs in dev.env (see dev.env.example), then 'make use'."
	@echo "Current refs → frontend=$(FRONTEND_REF) core_api=$(API_REF) core_agent=$(AGENT_REF)"

$(_NPM_STAMP): $(FRONTEND)/package-lock.json
	npm --prefix $(FRONTEND) ci

$(_API_STAMP): $(API)/uv.lock
	uv sync --directory $(API)

$(_AGENT_STAMP): $(AGENT)/uv.lock
	uv sync --directory $(AGENT)

setup: $(_NPM_STAMP) $(_API_STAMP) $(_AGENT_STAMP)  ## install all module dependencies (npm + uv venvs)

dev: $(_NPM_STAMP) $(_API_STAMP) $(_AGENT_STAMP)  ## run desktop dev from local module source (hot reload)
	@trap 'kill 0' SIGINT SIGTERM EXIT; \
	uv run --directory $(API) uvicorn cowork.server:app --reload \
		--reload-dir $(CURDIR)/$(API)/cowork \
		--reload-dir $(CURDIR)/$(AGENT)/anton & \
	npm --prefix $(FRONTEND) run dev

dev-web: $(_NPM_STAMP) $(_API_STAMP) $(_AGENT_STAMP)  ## run web dev in the browser from local module source
	@trap 'kill 0' SIGINT SIGTERM EXIT; \
	uv run --directory $(API) uvicorn cowork.server:app --reload \
		--reload-dir $(CURDIR)/$(API)/cowork \
		--reload-dir $(CURDIR)/$(AGENT)/anton & \
	cd $(FRONTEND) && BUILD_TARGET=web npm run dev:renderer -- --open

build: $(_NPM_STAMP)  ## production renderer build
	npm --prefix $(FRONTEND) run build

dist-mac: $(_NPM_STAMP)  ## package the macOS desktop app
	npm --prefix $(FRONTEND) run dist:mac

dist-win: $(_NPM_STAMP)  ## package the Windows desktop app
	npm --prefix $(FRONTEND) run dist:win

docker-build:  ## build the docker images
	@set -e; \
	anton_version=$$(cd $(AGENT) && uvx --with hatch-vcs hatchling version); \
	server_version=$$(cd $(API) && uvx --with hatch-vcs hatchling version); \
	ANTON_VERSION="$$anton_version" COWORK_SERVER_VERSION="$$server_version" docker compose build

docker-up:  ## start the docker stack
	docker compose up

docker-down:  ## stop the docker stack
	docker compose down

# Wipe every local install + all app state, returning the machine to a
# pre-install state. Covers BOTH runtime models (the Makefile dev venvs
# AND the Electron app's `uv tool install cowork-server`) plus user data.
# Removes:
#   • cowork-server uv tool (and the legacy anton-agent tool)
#   • submodule venvs: $(API)/.venv, $(AGENT)/.venv
#   • ~/.anton   — provider keys / .env
#   • ~/.cowork  — database, hermes, projects
# The next `make setup` (or app launch) reinstalls from scratch.
# Set FORCE=1 to skip the confirmation prompt (CI / scripts).
flush:  ## wipe all local installs + app state (fresh start; FORCE=1 to skip prompt)
	@echo "This will PERMANENTLY remove:"
	@echo "  • cowork-server uv tool (+ legacy anton-agent)"
	@echo "  • $(API)/.venv and $(AGENT)/.venv"
	@echo "  • ~/.anton   (provider keys / .env)"
	@echo "  • ~/.cowork  (database, hermes, projects)"
	@if [ "$(FORCE)" != "1" ]; then \
		read -p "Proceed? [y/N] " ans; [ "$$ans" = y ] || [ "$$ans" = Y ] || { echo "Aborted."; exit 1; }; \
	fi
	-uv tool uninstall cowork-server
	-uv tool uninstall anton-agent
	rm -rf $(API)/.venv $(AGENT)/.venv
	rm -rf $$HOME/.anton $$HOME/.cowork
	@echo "✓ Flushed. Run 'make setup' (or relaunch the app) for a fresh install."

# ── Module-branch workflow ───────────────────────────────────────────────
# The superproject pins each submodule to a commit; `.gitmodules` sets
# `ignore = all` so day-to-day branch work never shows up as superproject
# changes (git stays clean). Pins move ONLY via `make pin` — a deliberate,
# reviewable commit. See CLAUDE.md for the full workflow.

refs:   ## print the refs the next make/run will use
	@echo "frontend : $(FRONTEND_REF)"
	@echo "core_api : $(API_REF)"
	@echo "core_agent: $(AGENT_REF)"

use:    ## check out the configured refs across submodules (REF=… or dev.env)
	-git -C $(FRONTEND) fetch -q origin $(FRONTEND_REF)
	-git -C $(FRONTEND) checkout $(FRONTEND_REF)
	-git -C $(API) fetch -q origin $(API_REF)
	-git -C $(API) checkout $(API_REF)
	-git -C $(AGENT) fetch -q origin $(AGENT_REF)
	-git -C $(AGENT) checkout $(AGENT_REF)
	@echo "✓ on: frontend=$(FRONTEND_REF) core_api=$(API_REF) core_agent=$(AGENT_REF)"

baseline:  ## snap every submodule back to the superproject's pinned commits
	git submodule update --init --recursive
	@echo "✓ submodules reset to pinned baseline."

pin:    ## record the submodules' CURRENT commits as the superproject pins
	git add $(FRONTEND) $(API) $(AGENT) $(VAULT)
	@git diff --cached --quiet -- $(FRONTEND) $(API) $(AGENT) $(VAULT) \
		&& echo "Nothing to pin — submodule commits already match." \
		|| git commit -m "chore: bump submodule pins"
	@echo "→ push the superproject to share the new pins."

server: ## (re)install the Electron desktop server from the configured API_REF/AGENT_REF
	UV_PYTHON_PREFERENCE=only-managed uv tool install \
		"git+https://github.com/mindsdb/cowork-server.git@$(API_REF)" $(_ANTON_WITH) \
		--force --reinstall --python '>=3.12,<3.14'
	@echo "✓ desktop server installed from core_api@$(API_REF)."

server-local: ## install Electron desktop server from LOCAL uncommitted source (no push needed)
	UV_PYTHON_PREFERENCE=only-managed uv tool install \
		"$(CURDIR)/$(API)" \
		--force --reinstall --python '>=3.12,<3.14'
	@echo "✓ desktop server installed from local source: $(CURDIR)/$(API)"
	@echo "  anton resolved from core_api/pyproject.toml [tool.uv.sources]"
	@echo "  To also use local core_agent, set:"
	@echo "    [tool.uv.sources] anton-agent = { path = \"../../core_agent\" }"
	@echo "  in backend/core_api/pyproject.toml, then re-run server-local."

app: $(_NPM_STAMP)  ## run the Electron desktop app against the configured branch (no auto-update)
	cd $(FRONTEND) && COWORK_SERVER_DISABLE_AUTOUPDATE=1 npm run dev

app-local: $(_NPM_STAMP)  ## run the Electron desktop app using LOCAL source (implies server-local)
	$(MAKE) server-local
	cd $(FRONTEND) && COWORK_SERVER_DISABLE_AUTOUPDATE=1 \
		COWORK_SERVER_PACKAGE="$(CURDIR)/$(API)" npm run dev

pack-local: $(_NPM_STAMP)  ## build macOS .app from LOCAL uncommitted source, iCloud-safe (no DMG)
	$(MAKE) server-local
	cd $(FRONTEND) && PATH="/opt/homebrew/opt/node@20/bin:$$PATH" \
		npm run build && \
		npx electron-builder --mac --arm64 --dir \
		--config.directories.output=/tmp/minds-build
	rm -rf $(FRONTEND)/release/mac-arm64
	cp -R /tmp/minds-build/mac-arm64 $(FRONTEND)/release/
	@echo "✓ Built: $(FRONTEND)/release/mac-arm64/MindsHub Cowork.app"
	@echo "  Launch: COWORK_SERVER_DISABLE_AUTOUPDATE=1 '$(CURDIR)/$(FRONTEND)/release/mac-arm64/MindsHub Cowork.app/Contents/MacOS/MindsHub Cowork' &"

watch: $(_NPM_STAMP) $(_API_STAMP) $(_AGENT_STAMP)  ## live reload — Electron app + Python hot-reload (alias for dev)
	@trap 'kill 0' SIGINT SIGTERM EXIT; \
	uv run --directory $(API) uvicorn cowork.server:app --reload \
		--reload-dir $(CURDIR)/$(API)/cowork \
		--reload-dir $(CURDIR)/$(AGENT)/anton & \
	npm --prefix $(FRONTEND) run dev
