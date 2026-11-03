"""Runs and their events, kept in SQLite so they outlive the process.

A redeploy replaces the container mid-run; with the store on the mounted volume
the web app can still list the run, replay what it streamed, and see it resume.
One process owns the store (the server runs a single worker), so a run found
``queued`` or ``running`` at startup was cut off by the last shutdown.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

QUEUED, RUNNING, COMPLETED, FAILED, CANCELLED, INTERRUPTED = (
    "queued", "running", "completed", "failed", "cancelled", "interrupted",
)
ACTIVE = frozenset({QUEUED, RUNNING})
FINISHED = frozenset({COMPLETED, FAILED, CANCELLED, INTERRUPTED})

_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    ticker TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    asset_type TEXT NOT NULL,
    analysts TEXT NOT NULL,
    request TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    rating TEXT,
    error TEXT,
    report_dir TEXT,
    stats TEXT,
    result TEXT
);
CREATE INDEX IF NOT EXISTS runs_created ON runs (created_at);
CREATE TABLE IF NOT EXISTS events (
    run_id TEXT NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
    seq INTEGER NOT NULL,
    type TEXT NOT NULL,
    data TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, seq)
);
"""

_JSON_COLUMNS = ("analysts", "request", "stats", "result")


def now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


class RunStore:
    """Thread-safe: the request handlers and the run workers share one connection."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._db = sqlite3.connect(str(self.path), check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        with self._lock:
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.execute("PRAGMA foreign_keys=ON")
            self._db.executescript(_SCHEMA)
            self._db.commit()

    def close(self) -> None:
        with self._lock:
            self._db.close()

    # Runs

    def create_run(self, *, ticker: str, trade_date: str, asset_type: str,
                   analysts: list[str], request: dict) -> dict:
        run_id = uuid.uuid4().hex
        with self._lock:
            self._db.execute(
                "INSERT INTO runs (id, ticker, trade_date, asset_type, analysts, request, status,"
                " created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (run_id, ticker, trade_date, asset_type, json.dumps(analysts),
                 json.dumps(request), QUEUED, now()),
            )
            self._db.commit()
        return self.get_run(run_id)

    def update_run(self, run_id: str, **fields: Any) -> None:
        if not fields:
            return
        values = [json.dumps(v) if k in _JSON_COLUMNS and v is not None else v
                  for k, v in fields.items()]
        assignments = ", ".join(f"{k} = ?" for k in fields)
        with self._lock:
            self._db.execute(f"UPDATE runs SET {assignments} WHERE id = ?", (*values, run_id))
            self._db.commit()

    def start_attempt(self, run_id: str) -> int:
        """Mark the run running and count the attempt; returns the attempt number."""
        with self._lock:
            self._db.execute(
                "UPDATE runs SET status = ?, attempts = attempts + 1, started_at = ?,"
                " finished_at = NULL, error = NULL WHERE id = ?",
                (RUNNING, now(), run_id),
            )
            self._db.commit()
            row = self._db.execute("SELECT attempts FROM runs WHERE id = ?", (run_id,)).fetchone()
        return row["attempts"]

    def get_run(self, run_id: str, *, with_result: bool = False) -> dict | None:
        with self._lock:
            row = self._db.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
        return _run(row, with_result) if row else None

    def list_runs(self, *, ticker: str | None = None, status: str | None = None,
                  limit: int = 50, offset: int = 0) -> list[dict]:
        where, params = [], []
        if ticker:
            where.append("ticker = ?")
            params.append(ticker)
        if status:
            where.append("status = ?")
            params.append(status)
        clause = f"WHERE {' AND '.join(where)}" if where else ""
        with self._lock:
            rows = self._db.execute(
                f"SELECT * FROM runs {clause} ORDER BY created_at DESC, rowid DESC LIMIT ? OFFSET ?",
                (*params, limit, offset),
            ).fetchall()
        return [_run(row, False) for row in rows]

    def active_run_for(self, ticker: str, trade_date: str) -> dict | None:
        with self._lock:
            row = self._db.execute(
                "SELECT * FROM runs WHERE ticker = ? AND trade_date = ? AND status IN (?, ?)",
                (ticker, trade_date, *ACTIVE),
            ).fetchone()
        return _run(row, False) if row else None

    def runs_with_status(self, *statuses: str) -> list[dict]:
        marks = ", ".join("?" for _ in statuses)
        with self._lock:
            rows = self._db.execute(
                f"SELECT * FROM runs WHERE status IN ({marks}) ORDER BY created_at", statuses
            ).fetchall()
        return [_run(row, False) for row in rows]

    def delete_run(self, run_id: str) -> None:
        with self._lock:
            self._db.execute("DELETE FROM runs WHERE id = ?", (run_id,))
            self._db.commit()

    # Events

    def append_event(self, run_id: str, type_: str, data: dict) -> int:
        with self._lock:
            row = self._db.execute(
                "SELECT COALESCE(MAX(seq), 0) + 1 AS seq FROM events WHERE run_id = ?", (run_id,)
            ).fetchone()
            seq = row["seq"]
            self._db.execute(
                "INSERT INTO events (run_id, seq, type, data, created_at) VALUES (?, ?, ?, ?, ?)",
                (run_id, seq, type_, json.dumps(data, ensure_ascii=False, default=str), now()),
            )
            self._db.commit()
        return seq

    def events_after(self, run_id: str, after: int = 0, limit: int = 500) -> list[dict]:
        with self._lock:
            rows = self._db.execute(
                "SELECT seq, type, data, created_at FROM events WHERE run_id = ? AND seq > ?"
                " ORDER BY seq LIMIT ?",
                (run_id, after, limit),
            ).fetchall()
        return [{"seq": r["seq"], "type": r["type"], "data": json.loads(r["data"]),
                 "created_at": r["created_at"]} for r in rows]


def _run(row: sqlite3.Row, with_result: bool) -> dict:
    run = dict(row)
    for key in _JSON_COLUMNS:
        if run.get(key) is not None:
            run[key] = json.loads(run[key])
    if not with_result:
        run.pop("result", None)
    return run
