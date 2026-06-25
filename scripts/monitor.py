#!/usr/bin/env python3
"""
monitor.py - Read-only HTTP endpoint over the Trellis mutation_log.

Exposes the graph's live activity feed for monitoring long-running jobs (e.g. a
backfill) without touching the writing process. The SQLite connection is opened
read-only (mode=ro), so this can never lock or mutate the graph the pipeline is
actively writing.

Endpoints:
  GET /health    - liveness + db path
  GET /stats     - node/edge/references counts, last mutation, recent activity
  GET /mutations - recent mutation_log rows (JSON), filterable

Query params for /mutations:
  limit        max rows (default 50, cap 1000)
  since        lookback window: bare integer = seconds ago; otherwise an ISO
               timestamp lower bound (e.g. 2026-06-24T14:00:00)
  operation    filter by operation (create|update|delete|...)
  entity_type  filter by entity_type (node|edge|annotation|...)
  actor_id     filter by actor_id (e.g. daedalus)

Run:
  python scripts/monitor.py --port 8901
  curl -s localhost:8901/stats | python -m json.tool
  curl -s 'localhost:8901/mutations?since=120&entity_type=node&limit=20'
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

# --- Declaration -----------------------------------------------------------

# Workspace resolution mirrors pipeline/trellis.py so the monitor always reads
# the same graph the pipeline writes.
_DEFAULT_WORKSPACE = str(Path(__file__).resolve().parents[2])


def _db_path() -> str:
    workspace = os.environ.get("TRELLIS_WORKSPACE", _DEFAULT_WORKSPACE)
    return str(Path(workspace) / ".trellis" / "trellis.db")


_MUTATION_COLUMNS = (
    "id", "timestamp", "actor", "actor_id", "entity_type", "entity_id", "operation"
)
_MAX_LIMIT = 1000


# --- Body ------------------------------------------------------------------

def _connect() -> sqlite3.Connection:
    # Read-only URI connection: cannot lock out or corrupt the live writer.
    conn = sqlite3.connect(f"file:{_db_path()}?mode=ro", uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    return conn


def _maybe_json(value):
    # diff / metadata are stored as JSON text; parse for clean output, but never
    # fail the request on a malformed blob.
    if value is None:
        return None
    try:
        return json.loads(value)
    except (ValueError, TypeError):
        return value


def query_stats() -> dict:
    with _connect() as conn:
        def scalar(sql: str) -> int:
            return conn.execute(sql).fetchone()[0]

        last = conn.execute(
            "SELECT timestamp, operation, entity_type FROM mutation_log "
            "ORDER BY timestamp DESC LIMIT 1"
        ).fetchone()
        # Soft-deleted nodes keep their row (status='deleted') and their
        # pipeline:* tag_links; exclude them so counts reflect the live graph.
        pipeline = {
            row[0]: row[1]
            for row in conn.execute(
                "SELECT substr(t.tag, 10) AS status, COUNT(*) "
                "FROM tag_links t JOIN nodes n ON n.id = t.owner_id "
                "WHERE t.tag LIKE 'pipeline:%' "
                "AND COALESCE(n.status, '') != 'deleted' "
                "GROUP BY t.tag ORDER BY 2 DESC"
            ).fetchall()
        }
        return {
            "nodes": scalar(
                "SELECT COUNT(*) FROM nodes WHERE COALESCE(status, '') != 'deleted'"
            ),
            "nodes_deleted": scalar(
                "SELECT COUNT(*) FROM nodes WHERE status = 'deleted'"
            ),
            "pipeline": pipeline,
            "edges_total": scalar("SELECT COUNT(*) FROM edges"),
            "references_edges": scalar(
                "SELECT COUNT(*) FROM edges WHERE relationship='references'"
            ),
            "mutations_total": scalar("SELECT COUNT(*) FROM mutation_log"),
            "mutations_last_min": scalar(
                "SELECT COUNT(*) FROM mutation_log "
                "WHERE timestamp > datetime('now', '-1 minute')"
            ),
            "mutations_last_5min": scalar(
                "SELECT COUNT(*) FROM mutation_log "
                "WHERE timestamp > datetime('now', '-5 minutes')"
            ),
            "last_mutation": dict(last) if last else None,
        }


def query_mutations(params: dict) -> list[dict]:
    limit = min(int((params.get("limit") or ["50"])[0]), _MAX_LIMIT)
    where = []
    args: list = []

    since = (params.get("since") or [None])[0]
    if since:
        if since.isdigit():
            where.append("timestamp > datetime('now', ?)")
            args.append(f"-{int(since)} seconds")
        else:
            where.append("timestamp >= ?")
            args.append(since)
    for field in ("operation", "entity_type", "actor_id"):
        value = (params.get(field) or [None])[0]
        if value:
            where.append(f"{field} = ?")
            args.append(value)

    clause = (" WHERE " + " AND ".join(where)) if where else ""
    sql = (
        "SELECT id, timestamp, actor, actor_id, entity_type, entity_id, "
        "operation, diff, metadata FROM mutation_log"
        f"{clause} ORDER BY timestamp DESC LIMIT ?"
    )
    args.append(limit)

    with _connect() as conn:
        rows = conn.execute(sql, args).fetchall()
    out = []
    for row in rows:
        record = {col: row[col] for col in _MUTATION_COLUMNS}
        record["diff"] = _maybe_json(row["diff"])
        record["metadata"] = _maybe_json(row["metadata"])
        out.append(record)
    return out


class _Handler(BaseHTTPRequestHandler):
    def _send(self, payload: object, status: int = 200) -> None:
        body = json.dumps(payload, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 (http.server API)
        parsed = urlparse(self.path)
        route = parsed.path.rstrip("/") or "/"
        params = parse_qs(parsed.query)
        try:
            if route in ("/", "/health"):
                self._send({"ok": True, "db": _db_path()})
            elif route == "/stats":
                self._send(query_stats())
            elif route == "/mutations":
                self._send(query_mutations(params))
            else:
                self._send({"error": f"unknown route {route!r}"}, status=404)
        except sqlite3.OperationalError as exc:
            # Most commonly: db file missing or unreadable.
            self._send({"error": str(exc), "db": _db_path()}, status=503)
        except Exception as exc:  # defensive: a monitor must not crash the loop
            self._send({"error": str(exc)}, status=500)

    def log_message(self, *_args) -> None:
        # Silence default per-request stderr noise; this is a monitoring daemon.
        return


# --- End -------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8901)
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), _Handler)
    print(f"mutation_log monitor on http://{args.host}:{args.port}  (db: {_db_path()})")
    print("  GET /health  /stats  /mutations")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
