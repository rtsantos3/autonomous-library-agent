#!/usr/bin/env bash
#
# setup.sh — post-clone bootstrap for the autonomous-library-agent pipeline.
#
# Idempotent and safe to re-run: it will NOT recreate an existing conda env and
# will NOT overwrite an existing .env. Steps:
#   1. verify conda is available
#   2. create the named conda env 'autonomous-library-agent' from environment.yml (only if absent)
#   3. create .env from templates/.env.example (only if absent)
#   4. verify the `trellis` CLI is on PATH (verify-only; does not install it)
#   5. report the resolved Trellis workspace and whether it is initialized
#   6. run the offline test suite as a smoke check
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_ROOT"
ENV_NAME="autonomous-library-agent"
# The Trellis workspace defaults to the parent of this pipeline repo (matches
# pipeline/trellis.py's default and the submodule-in-library layout). The graph
# export lives at <workspace>/graph/. Override via TRELLIS_WORKSPACE in .env.
WS_DEFAULT="$(dirname "$REPO_ROOT")"

say()  { printf '\n\033[1;34m==>\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m  ok\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m  !!\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31m  xx\033[0m %s\n' "$*" >&2; exit 1; }

# 1. conda ---------------------------------------------------------------------
say "Checking for conda"
command -v conda >/dev/null 2>&1 || die "conda not found on PATH. Install Miniconda/Anaconda first."
ok "conda: $(command -v conda)"

# 2. conda env (named env: autonomous-library-agent). Never recreate an existing
#    one — matches the project rule of not destroying conda environments.
say "Setting up conda environment '$ENV_NAME'"
if conda env list | awk '{print $1}' | grep -qx "$ENV_NAME"; then
  ok "env '$ENV_NAME' already exists (left untouched)"
else
  conda env create -n "$ENV_NAME" -f environment.yml
  ok "created conda env '$ENV_NAME'"
fi

# 3. .env (never overwrite) ----------------------------------------------------
say "Setting up .env"
if [ -f .env ]; then
  ok ".env already exists (left untouched)"
else
  cp templates/.env.example .env
  warn "created .env from templates/.env.example — edit it and fill in your API keys"
fi

# 3b. config.yml + Trellis workspace (prompt once) -----------------------------
#     Non-secret tuneables live in config.yml (secrets stay in .env). The
#     workspace is the library directory the agent ingests into; one agent serves
#     many libraries. Prompt for it once; default to the upper directory.
say "Setting up config.yml"
CFG="$REPO_ROOT/config.yml"
if [ ! -f "$CFG" ]; then
  cp templates/config.yml.example "$CFG"
  ok "created config.yml from templates/config.yml.example"
fi
CONFIG_WS="$(sed -nE 's/^workspace:[[:space:]]*"?([^"]*)"?[[:space:]]*$/\1/p' "$CFG" 2>/dev/null | head -1 || true)"
if [ -n "$CONFIG_WS" ]; then
  ok "workspace already set in config.yml: $CONFIG_WS"
else
  if [ -t 0 ]; then
    printf '  Trellis workspace — the library dir holding .trellis/ [%s]: ' "$WS_DEFAULT"
    read -r WS_INPUT
  else
    WS_INPUT=""
    warn "non-interactive shell; using default workspace"
  fi
  WS_SET="${WS_INPUT:-$WS_DEFAULT}"
  if grep -qE '^workspace:' "$CFG"; then
    sed -i -E "s|^workspace:.*|workspace: \"$WS_SET\"|" "$CFG"
  else
    printf 'workspace: "%s"\n' "$WS_SET" >> "$CFG"
  fi
  ok "workspace set in config.yml: $WS_SET"
fi

# 4. trellis CLI (verify only) -------------------------------------------------
say "Verifying Trellis CLI"
if command -v trellis >/dev/null 2>&1; then
  ok "trellis: $(command -v trellis)"
else
  die "trellis CLI not found on PATH. Install the Trellis Node CLI (e.g. via npm/nvm), ensure \`trellis\` is on PATH, then re-run setup.sh."
fi

# 4b. git-lfs (verify only) ----------------------------------------------------
#     The graph export (graph/trellis_export.jsonl) is tracked via Git LFS. If
#     git-lfs was not installed when the workspace was cloned, the working file
#     is an unresolved pointer; the hydrate step (5) detects that and fetches it.
say "Verifying Git LFS"
if command -v git-lfs >/dev/null 2>&1; then
  ok "git-lfs: $(command -v git-lfs)"
else
  warn "git-lfs not found — required only if the graph export is an unresolved LFS pointer"
fi

# 5. Trellis workspace ---------------------------------------------------------
#    The workspace is the directory Trellis runs in (it holds .trellis/).
#    Precedence: live TRELLIS_WORKSPACE env, then .env's TRELLIS_WORKSPACE,
#    then config.yml `workspace:`, then the default (upper directory).
say "Resolving Trellis workspace"
WS="${TRELLIS_WORKSPACE:-}"
if [ -z "$WS" ] && [ -f .env ]; then
  # Read .env as data, not shell: KEY=VALUE lines only; comments ignored.
  WS="$(awk -F= '
    /^[[:space:]]*#/ || /^[[:space:]]*$/ { next }
    {
      key = $1
      sub(/^[[:space:]]*/, "", key)
      sub(/[[:space:]]*$/, "", key)
      if (key == "TRELLIS_WORKSPACE") {
        val = substr($0, index($0, "=") + 1)
        sub(/^[[:space:]]*/, "", val)
        sub(/[[:space:]]*$/, "", val)
        if ((val ~ /^".*"$/) || (val ~ /^'\''.*'\''$/)) {
          val = substr(val, 2, length(val) - 2)
        }
        print val
        exit
      }
    }
  ' .env)"
fi
[ -z "$WS" ] && WS="$(sed -nE 's/^workspace:[[:space:]]*"?([^"]*)"?[[:space:]]*$/\1/p' "$CFG" 2>/dev/null | head -1 || true)"
WS="${WS:-$WS_DEFAULT}"
ok "workspace: $WS"
GRAPH_EXPORT="$WS/graph/trellis_export.jsonl"
if [ -d "$WS/.trellis" ]; then
  ok "workspace already initialized (.trellis/ present) — leaving it untouched"
elif [ -f "$GRAPH_EXPORT" ]; then
  # The export is tracked via Git LFS. If git-lfs was absent at clone time the
  # working file is an unresolved pointer (first line is the LFS spec URL), and
  # importing it would silently load nothing — materialize it first so this stays
  # a single-run bootstrap.
  if head -n1 "$GRAPH_EXPORT" | grep -q '^version https://git-lfs'; then
    say "Graph export is an unresolved Git LFS pointer — fetching via git-lfs"
    command -v git-lfs >/dev/null 2>&1 || die "graph/trellis_export.jsonl is a Git LFS pointer but git-lfs is not installed. Install it (e.g. 'sudo apt-get install git-lfs' or 'brew install git-lfs'), then re-run setup.sh."
    ( cd "$WS" && git lfs install --local >/dev/null 2>&1 && git lfs pull --include="graph/trellis_export.jsonl" )
    head -n1 "$GRAPH_EXPORT" | grep -q '^version https://git-lfs' \
      && die "git lfs pull did not materialize graph/trellis_export.jsonl — check your LFS remote/credentials, then re-run setup.sh."
    ok "materialized LFS export"
  fi
  # Hydrate the local SQLite db from the JSONL topology. SQLite is regenerable;
  # the JSONL export is the shared source of truth.
  say "Hydrating graph from $GRAPH_EXPORT"
  ( cd "$WS" && { trellis init >/dev/null 2>&1 || true; } && trellis import --path "$GRAPH_EXPORT" )
  ok "imported graph topology into $WS/.trellis"
else
  # No existing workspace and no committed topology to import — instantiate a
  # fresh, empty Trellis db so the pipeline has a workspace to write into.
  say "Instantiating an empty Trellis workspace"
  ( cd "$WS" && trellis init )
  ok "created empty workspace at $WS/.trellis"
  warn "once you have a graph, snapshot it with scripts/export_graph.sh and commit graph/trellis_export.jsonl."
fi

# 6. smoke test (offline only) -------------------------------------------------
say "Running offline test suite (smoke check)"
conda run -n "$ENV_NAME" python -m pytest tests/ -q

say "Setup complete."
cat <<EOF

Next steps:
  1. Edit .env and add your API keys (NCBI_API_KEY, S2_API_KEY, CROSSREF_EMAIL, ...).
  2. Activate the environment:
       conda activate $ENV_NAME
  3. Try a sample batch ingest with the DOIs in samples/seed_dois.txt:
       python -c "from pipeline.ingestion import ingest_batch; \\
dois=[l.strip() for l in open('samples/seed_dois.txt') if l.strip() and not l.startswith('#')]; \\
o,m=ingest_batch(dois); print(len(o),'ingested')"
  4. To share the resulting graph topology via git (writes to the workspace root,
     $WS_DEFAULT/graph/):
       ./scripts/export_graph.sh
       # then, from the workspace root:
       git add graph/trellis_export.jsonl && git commit -m "Update graph topology"
EOF
