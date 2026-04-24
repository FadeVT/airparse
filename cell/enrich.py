"""Band enrichment — fill in EARFCN + band for cells we already have.

Strategy: call WiGLE `/api/v2/cell/search` with a bounding box, match returned
cells against our local DB by `operator_key`, update `earfcn`, `band_number`,
and `band_label` columns. Respects the existing cell observations — only fills
in the missing band columns; doesn't overwrite carrier/position/signal.

Also inserts any cells WiGLE returns that we don't already have (this is how
"coverage research for roads I haven't driven" gets its data). Those rows are
tagged with a synthetic `source_transid = 'wigle-bbox-<timestamp>'` so we can
distinguish them from KML-ingested cells later.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

from cell import bands as cb
from cell import carriers as cc
from cell import db
from cell import wigle_api

log = logging.getLogger(__name__)


@dataclass
class EnrichReport:
    bbox: tuple[float, float, float, float]
    cells_fetched: int = 0
    rows_enriched: int = 0        # existing rows that got band/EARFCN filled in
    rows_inserted: int = 0        # brand-new cells from WiGLE we didn't have
    rows_skipped_no_band: int = 0 # WiGLE returned cell but no channel → no band
    error: Optional[str] = None


def enrich_bbox(
    lat_south: float,
    lat_north: float,
    lon_west: float,
    lon_east: float,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> EnrichReport:
    report = EnrichReport(bbox=(lat_south, lat_north, lon_west, lon_east))
    synthetic_transid = f"wigle-bbox-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"

    if progress_cb:
        progress_cb(
            f"Fetching cells from WiGLE in bbox "
            f"[{lat_south:.3f},{lon_west:.3f}] → [{lat_north:.3f},{lon_east:.3f}]…"
        )

    try:
        with db.connect() as conn:
            for row in wigle_api.search_bbox(
                lat_south, lat_north, lon_west, lon_east, progress_cb=progress_cb
            ):
                report.cells_fetched += 1
                _apply_row(conn, row, synthetic_transid, report)
            conn.commit()
    except Exception as e:
        log.exception("Enrich failed")
        report.error = str(e)

    return report


def _apply_row(conn, row: wigle_api.CellSearchRow, synthetic_transid: str,
               report: EnrichReport) -> None:
    # Resolve band from channel + radio type
    band = cb.resolve(row.radio_type, row.channel) if row.channel else None
    band_number = band.number if band else None
    band_label = band.label if band else None

    # First: is this cell already in our DB?
    existing = conn.execute(
        "SELECT id, earfcn, band_number FROM cells WHERE operator_key = ? LIMIT 1",
        (row.operator_key,),
    ).fetchone()

    if existing is not None:
        # Update only the band columns across ALL observations of this cell
        # (same operator_key → same band regardless of when/where we saw it).
        if row.channel is None:
            report.rows_skipped_no_band += 1
            return
        updated = conn.execute(
            """
            UPDATE cells
            SET earfcn = ?, band_number = ?, band_label = ?
            WHERE operator_key = ? AND (band_number IS NULL OR band_number != ?)
            """,
            (row.channel, band_number, band_label, row.operator_key, band_number or -1),
        ).rowcount
        if updated:
            report.rows_enriched += updated
        return

    # Not in our DB — insert as a WiGLE-discovered cell.
    mcc, mnc, xac_s, cid_s = cc.split_operator_key(row.operator_key)
    try:
        xac = int(xac_s)
        cid = int(cid_s)
    except ValueError:
        return
    if not mcc or not mnc:
        return

    conn.execute(
        """
        INSERT OR IGNORE INTO cells (
            operator_key, mcc, mnc, xac, cid, radio_type, carrier,
            name_from_kml, signal_dbm, accuracy_m, lat, lon,
            first_seen, last_seen, earfcn, band_number, band_label,
            source_transid
        ) VALUES (
            :operator_key, :mcc, :mnc, :xac, :cid, :radio_type, :carrier,
            :name_from_kml, :signal_dbm, :accuracy_m, :lat, :lon,
            :first_seen, :last_seen, :earfcn, :band_number, :band_label,
            :source_transid
        )
        """,
        {
            "operator_key": row.operator_key,
            "mcc": mcc,
            "mnc": mnc,
            "xac": xac,
            "cid": cid,
            "radio_type": row.radio_type or "LTE",
            "carrier": cc.lookup(mcc, mnc, unknown_name_fallback=row.carrier),
            "name_from_kml": row.carrier,
            "signal_dbm": None,
            "accuracy_m": None,
            "lat": row.lat,
            "lon": row.lon,
            "first_seen": None,
            "last_seen": None,
            "earfcn": row.channel,
            "band_number": band_number,
            "band_label": band_label,
            "source_transid": synthetic_transid,
        },
    )
    report.rows_inserted += 1
