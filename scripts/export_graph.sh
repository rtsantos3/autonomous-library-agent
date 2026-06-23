#!/usr/bin/env bash
#
# export_graph.sh — snapshot the Trellis graph to a committable JSONL file.
#
# The SQLite db is local runtime state; this JSONL export is the durable, shared
# source of truth (text → diffable/mergeable, full-fidelity per the Trellis
# export/import contract). Run this before committing topology changes, then
# commit graph/trellis_export.jsonl.
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Resolve the workspace the same way setup.sh / pipeline.trellis does.
WS="$(grep -E '^TRELLIS_WORKSPACE=' .env 2>/dev/null | tail -1 | cut -d= -f2- || true)"
WS="${WS:-$REPO_ROOT}"

GRAPH_DIR="$REPO_ROOT/graph"
GRAPH_EXPORT="$GRAPH_DIR/trellis_export.jsonl"
mkdir -p "$GRAPH_DIR"

command -v trellis >/dev/null 2>&1 || { echo "trellis CLI not on PATH" >&2; exit 1; }

# Checkpoint the WAL so the export reflects all writes (the WAL is gitignored).
if [ -f "$WS/.trellis/trellis.db" ] && command -v sqlite3 >/dev/null 2>&1; then
  sqlite3 "$WS/.trellis/trellis.db" 'PRAGMA wal_checkpoint(TRUNCATE);' >/dev/null 2>&1 || true
fi

( cd "$WS" && trellis export --path "$GRAPH_EXPORT" )

echo "Exported graph -> $GRAPH_EXPORT"
echo "Review:  git diff -- graph/trellis_export.jsonl"
echo "Commit:  git add graph/trellis_export.jsonl && git commit -m 'Update graph topology'"
