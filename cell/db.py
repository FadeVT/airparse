"""Dedicated SQLite for cell observations. Separate file so the WiFi-side
AirParse DB (and its schemas) are never touched.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

DB_PATH = Path.home() / "AirParse" / "cells.db"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS cells (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    operator_key    TEXT NOT NULL,          -- MCCMNC_XAC_CID, verbatim from KML
    mcc             TEXT NOT NULL,
    mnc             TEXT NOT NULL,
    xac             INTEGER NOT NULL,       -- LAC (GSM/WCDMA) or TAC (LTE/NR)
    cid             INTEGER NOT NULL,
    radio_type      TEXT NOT NULL,          -- LTE, NR, GSM, WCDMA, CDMA
    carrier         TEXT NOT NULL,
    name_from_kml   TEXT,                   -- whatever WiGLE named it
    signal_dbm      REAL,
    accuracy_m      REAL,
    lat             REAL NOT NULL,
    lon             REAL NOT NULL,
    first_seen      TEXT,
    last_seen       TEXT,
    -- band resolution (populated in Slice 2 via WiGLE API enrichment)
    earfcn          INTEGER,
    band_number     INTEGER,
    band_label      TEXT,
    -- provenance
    source_transid  TEXT NOT NULL,          -- WiGLE transaction file we pulled from
    UNIQUE (operator_key, source_transid, first_seen)
);

CREATE INDEX IF NOT EXISTS idx_cells_operator ON cells(operator_key);
CREATE INDEX IF NOT EXISTS idx_cells_carrier  ON cells(carrier);
CREATE INDEX IF NOT EXISTS idx_cells_radio    ON cells(radio_type);
CREATE INDEX IF NOT EXISTS idx_cells_band     ON cells(band_number);
CREATE INDEX IF NOT EXISTS idx_cells_bbox     ON cells(lat, lon);

CREATE TABLE IF NOT EXISTS imported_transids (
    transid         TEXT PRIMARY KEY NOT NULL,
    imported_at     TEXT NOT NULL,
    cell_count      INTEGER NOT NULL DEFAULT 0
);
"""


def _ensure_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.executescript(_SCHEMA)
        conn.commit()


@contextmanager
def connect() -> Iterator[sqlite3.Connection]:
    """Yield a SQLite connection with foreign keys on and WAL enabled."""
    _ensure_db()
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.row_factory = sqlite3.Row
        yield conn
    finally:
        conn.close()


def stats() -> dict:
    """Quick summary for the UI's header banner."""
    _ensure_db()
    with connect() as conn:
        total = conn.execute("SELECT COUNT(*) FROM cells").fetchone()[0]
        by_carrier = {
            r["carrier"]: r["n"]
            for r in conn.execute(
                "SELECT carrier, COUNT(*) AS n FROM cells "
                "GROUP BY carrier ORDER BY n DESC"
            )
        }
        by_radio = {
            r["radio_type"]: r["n"]
            for r in conn.execute(
                "SELECT radio_type, COUNT(*) AS n FROM cells "
                "GROUP BY radio_type ORDER BY n DESC"
            )
        }
        imported = conn.execute(
            "SELECT COUNT(*) FROM imported_transids"
        ).fetchone()[0]
    return {
        "total_cells": total,
        "by_carrier": by_carrier,
        "by_radio": by_radio,
        "imported_transids": imported,
    }


def imported_transids() -> set[str]:
    """Already-ingested transids — skipped on re-import unless force is set."""
    _ensure_db()
    with connect() as conn:
        return {r[0] for r in conn.execute("SELECT transid FROM imported_transids")}
