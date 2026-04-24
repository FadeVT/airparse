"""Per-user WiFi observations DB.

Keyed by BSSID; every observation carries its source transid so import is
idempotent. Aggregate-by-BSSID query is what the Dashboard map renders.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

DB_PATH = Path.home() / "AirParse" / "wifi_points.db"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS wifi_observations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    bssid           TEXT NOT NULL,
    ssid            TEXT,
    encryption      TEXT,
    signal_dbm      REAL,
    accuracy_m      REAL,
    lat             REAL NOT NULL,
    lon             REAL NOT NULL,
    seen_at         TEXT,
    source_transid  TEXT NOT NULL,
    UNIQUE (bssid, source_transid, seen_at)
);

CREATE INDEX IF NOT EXISTS idx_wifi_bssid ON wifi_observations(bssid);
CREATE INDEX IF NOT EXISTS idx_wifi_bbox  ON wifi_observations(lat, lon);
CREATE INDEX IF NOT EXISTS idx_wifi_enc   ON wifi_observations(encryption);

CREATE TABLE IF NOT EXISTS wifi_imported_transids (
    transid         TEXT PRIMARY KEY NOT NULL,
    imported_at     TEXT NOT NULL,
    observation_count INTEGER NOT NULL DEFAULT 0
);
"""


def _ensure_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.executescript(_SCHEMA)
        conn.commit()


@contextmanager
def connect() -> Iterator[sqlite3.Connection]:
    _ensure_db()
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.row_factory = sqlite3.Row
        yield conn
    finally:
        conn.close()


def stats() -> dict:
    _ensure_db()
    with connect() as conn:
        obs = conn.execute("SELECT COUNT(*) FROM wifi_observations").fetchone()[0]
        nets = conn.execute("SELECT COUNT(DISTINCT bssid) FROM wifi_observations").fetchone()[0]
        imported = conn.execute("SELECT COUNT(*) FROM wifi_imported_transids").fetchone()[0]
    return {
        "observations": obs,
        "networks": nets,
        "imported_transids": imported,
    }


def imported_transids() -> set[str]:
    _ensure_db()
    with connect() as conn:
        return {r[0] for r in conn.execute("SELECT transid FROM wifi_imported_transids")}


def query_networks(
    limit: int = 200_000,
    bbox: tuple[float, float, float, float] | None = None,
) -> list[dict]:
    """Aggregate by BSSID — one row per unique network with best lat/lon.
    `bbox` is (lat_south, lat_north, lon_west, lon_east) if viewport-limited."""
    clauses: list[str] = []
    params: list = []
    if bbox is not None:
        lat_s, lat_n, lon_w, lon_e = bbox
        clauses.append("lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?")
        params.extend([lat_s, lat_n, lon_w, lon_e])
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    sql = f"""
        SELECT
            bssid,
            ssid,
            encryption,
            MAX(signal_dbm)            AS best_signal,
            AVG(lat)                   AS lat,
            AVG(lon)                   AS lon,
            COUNT(*)                   AS observations
        FROM wifi_observations
        {where}
        GROUP BY bssid
        LIMIT {int(limit)}
    """
    with connect() as conn:
        return [dict(r) for r in conn.execute(sql, params)]


def query_observations(
    limit: int = 200_000,
    bbox: tuple[float, float, float, float] | None = None,
) -> list[dict]:
    clauses: list[str] = []
    params: list = []
    if bbox is not None:
        lat_s, lat_n, lon_w, lon_e = bbox
        clauses.append("lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?")
        params.extend([lat_s, lat_n, lon_w, lon_e])
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    sql = f"""
        SELECT bssid, ssid, encryption, signal_dbm, lat, lon
        FROM wifi_observations
        {where}
        LIMIT {int(limit)}
    """
    with connect() as conn:
        return [dict(r) for r in conn.execute(sql, params)]
