#!/usr/bin/env bash
#
# export_graph.sh — snapshot the Trellis graph to a shareable JSONL file.
#
# The SQLite db is local runtime state; this JSONL export is the durable, shared
# source of truth (text -> diffable/mergeable, re-importable per the Trellis
# export/import contract). A fresh clone rebuilds the live db from it via
# `trellis import` (see setup.sh). Run this after topology changes.
#
# The export is SLIM: `trellis export` dumps the full mutation_log (append-only
# change history, ~90% of the bytes and tens of thousands of records). That
# history is not needed to reconstruct the graph, so we keep only node / edge /
# annotation records. Verified lossless: re-import reproduces every node, edge
# and annotation.
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
CFG="$REPO_ROOT/config.yml"

# Resolve the workspace with the SAME precedence as setup.sh / pipeline.trellis:
#   TRELLIS_WORKSPACE env  >  config.yml `workspace:`  >  parent of this repo.
WS="${TRELLIS_WORKSPACE:-}"
if [ -z "$WS" ] && [ -f "$CFG" ]; then
  WS="$(sed -nE 's/^workspace:[[:space:]]*"?([^"]*)"?[[:space:]]*$/\1/p' "$CFG" 2>/dev/null | head -1 || true)"
fi
WS="${WS:-$(dirname "$REPO_ROOT")}"

[ -d "$WS/.trellis" ] || { echo "No Trellis workspace at $WS (.trellis/ missing)" >&2; exit 1; }

GRAPH_DIR="$WS/graph"
GRAPH_EXPORT="$GRAPH_DIR/trellis_export.jsonl"
mkdir -p "$GRAPH_DIR"

command -v trellis >/dev/null 2>&1 || { echo "trellis CLI not on PATH" >&2; exit 1; }

# Checkpoint the WAL so the export reflects all writes (the WAL is gitignored).
if [ -f "$WS/.trellis/trellis.db" ] && command -v sqlite3 >/dev/null 2>&1; then
  sqlite3 "$WS/.trellis/trellis.db" 'PRAGMA wal_checkpoint(TRUNCATE);' >/dev/null 2>&1 || true
fi

# Export full, then strip mutation_log records (anything that is not a node /
# edge / annotation) into the durable slim artifact.
TMP_FULL="$(mktemp)"
trap 'rm -f "$TMP_FULL"' EXIT
( cd "$WS" && trellis export --path "$TMP_FULL" )
python3 - "$TMP_FULL" "$GRAPH_EXPORT" <<'PY'
import json, sys
src, dst = sys.argv[1], sys.argv[2]
KEEP = {"node", "edge", "annotation"}
kept = dropped = 0
with open(src) as fi, open(dst, "w") as fo:
    for line in fi:
        try:
            kind = json.loads(line).get("kind")
        except json.JSONDecodeError:
            dropped += 1
            continue
        if kind in KEEP:
            fo.write(line)
            kept += 1
        else:
            dropped += 1
print(f"slim export: kept {kept} graph records, dropped {dropped} mutation_log records")
PY

echo "Exported graph -> $GRAPH_EXPORT"
echo "Review:  git diff -- graph/trellis_export.jsonl"
echo "Commit:  git add graph/trellis_export.jsonl && git commit -m 'Update graph topology'"
