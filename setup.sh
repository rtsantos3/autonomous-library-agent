#!/usr/bin/env bash
#
# setup.sh — post-clone bootstrap for the autonomous-library-agent pipeline.
#
# Idempotent and safe to re-run: it will NOT recreate an existing conda env and
# will NOT overwrite an existing .env. Steps:
#   1. verify conda is available
#   2. create the ./setup prefix env from environment.yml (only if absent)
#   3. create .env from .env.example (only if absent) and point its
#      TRELLIS_WORKSPACE at this repo (self-contained workspace)
#   4. verify the `trellis` CLI is on PATH (verify-only; does not install it)
#   5. report the resolved Trellis workspace and whether it is initialized
#   6. run the offline test suite as a smoke check
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_ROOT"
ENV_PREFIX="$REPO_ROOT/setup"

say()  { printf '\n\033[1;34m==>\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m  ok\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m  !!\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31m  xx\033[0m %s\n' "$*" >&2; exit 1; }

# 1. conda ---------------------------------------------------------------------
say "Checking for conda"
command -v conda >/dev/null 2>&1 || die "conda not found on PATH. Install Miniconda/Anaconda first."
ok "conda: $(command -v conda)"

# 2. conda env (prefix env at ./setup, gitignored). Never recreate an existing
#    one — matches the project rule of not destroying conda environments.
say "Setting up conda environment at ./setup"
if [ -d "$ENV_PREFIX" ]; then
  ok "env already exists at ./setup (left untouched)"
else
  conda env create -p "$ENV_PREFIX" -f environment.yml
  ok "created conda env at ./setup"
fi

# 3. .env (never overwrite) ----------------------------------------------------
say "Setting up .env"
if [ -f .env ]; then
  ok ".env already exists (left untouched)"
else
  cp .env.example .env
  # Point the fresh .env at this repo so the Trellis workspace is self-contained.
  if grep -qE '^TRELLIS_WORKSPACE=' .env; then
    tmp="$(mktemp)"
    sed "s|^TRELLIS_WORKSPACE=.*|TRELLIS_WORKSPACE=$REPO_ROOT|" .env > "$tmp" && mv "$tmp" .env
  else
    printf '\nTRELLIS_WORKSPACE=%s\n' "$REPO_ROOT" >> .env
  fi
  warn "created .env from .env.example — edit it and fill in your API keys"
fi

# 4. trellis CLI (verify only) -------------------------------------------------
say "Verifying Trellis CLI"
if command -v trellis >/dev/null 2>&1; then
  ok "trellis: $(command -v trellis)"
else
  die "trellis CLI not found on PATH. Install the Trellis Node CLI (e.g. via npm/nvm), ensure \`trellis\` is on PATH, then re-run setup.sh."
fi

# 5. Trellis workspace ---------------------------------------------------------
#    The workspace is the directory Trellis runs in (it holds .trellis/).
#    TRELLIS_WORKSPACE in .env wins; otherwise default to the repo root.
say "Resolving Trellis workspace"
WS="$(grep -E '^TRELLIS_WORKSPACE=' .env 2>/dev/null | tail -1 | cut -d= -f2- || true)"
WS="${WS:-$REPO_ROOT}"
ok "workspace: $WS"
if [ -d "$WS/.trellis" ]; then
  ok "workspace is initialized (.trellis/ present)"
else
  warn "no .trellis/ at $WS yet — Trellis instantiates a workspace in the directory it runs in,"
  warn "so it is created automatically on the first ingestion run (cwd = workspace)."
fi

# 6. smoke test (offline only) -------------------------------------------------
say "Running offline test suite (smoke check)"
conda run -p "$ENV_PREFIX" python -m pytest tests/ -q

say "Setup complete."
cat <<EOF

Next steps:
  1. Edit .env and add your API keys (NCBI_API_KEY, S2_API_KEY, CROSSREF_EMAIL, ...).
  2. Activate the environment:
       conda activate $ENV_PREFIX
  3. Try a sample batch ingest with the DOIs in samples/seed_dois.txt:
       python -c "from pipeline.ingestion import ingest_batch; \\
dois=[l.strip() for l in open('samples/seed_dois.txt') if l.strip() and not l.startswith('#')]; \\
o,m=ingest_batch(dois); print(len(o),'ingested')"
EOF
