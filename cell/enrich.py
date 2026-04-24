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
from cell import reader as creader
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


# ─── Grid walk: enrich every unenriched cell in the DB ──────────────

@dataclass
class BulkEnrichReport:
    tiles_total: int = 0
    tiles_done: int = 0
    tiles_skipped_empty: int = 0
    cells_fetched: int = 0
    rows_enriched: int = 0
    rows_inserted: int = 0
    rows_skipped_no_band: int = 0
    cancelled: bool = False
    error: Optional[str] = None


def enrich_all_unenriched(
    tile_size_deg: float = 1.0,
    progress_cb: Optional[Callable[[str, int, int], None]] = None,
    is_cancelled: Optional[Callable[[], bool]] = None,
) -> BulkEnrichReport:
    """Walk a grid of bboxes covering every cell we have without band info.
    Skips tiles that contain zero unenriched towers to keep API calls honest.
    `progress_cb(msg, done, total)` is fired per tile; `is_cancelled()` is
    polled between tiles so the UI can abort mid-run without orphaning a
    worker thread."""
    rep = BulkEnrichReport()

    bbox = creader.unenriched_bbox()
    if bbox is None:
        return rep

    south, north, west, east = bbox
    # pad by half a tile so boundary cells aren't missed by snapping
    south -= tile_size_deg / 2
    north += tile_size_deg / 2
    west -= tile_size_deg / 2
    east += tile_size_deg / 2

    # Build the grid of tiles
    tiles: list[tuple[float, float, float, float]] = []
    lat = south
    while lat < north:
        lon = west
        while lon < east:
            tiles.append((
                lat, min(lat + tile_size_deg, north),
                lon, min(lon + tile_size_deg, east),
            ))
            lon += tile_size_deg
        lat += tile_size_deg

    # Pre-filter tiles to those that contain ≥1 unenriched cell — avoids
    # paying API cost on empty ocean / wilderness squares inside the bbox.
    non_empty: list[tuple[float, float, float, float]] = []
    with db.connect() as conn:
        for t in tiles:
            s, n, w, e = t
            has_any = conn.execute(
                "SELECT 1 FROM cells "
                "WHERE band_number IS NULL "
                "  AND lat BETWEEN ? AND ? AND lon BETWEEN ? AND ? "
                "LIMIT 1",
                (s, n, w, e),
            ).fetchone()
            if has_any:
                non_empty.append(t)
            else:
                rep.tiles_skipped_empty += 1

    rep.tiles_total = len(non_empty)

    synthetic_transid = f"wigle-bulk-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    try:
        with db.connect() as conn:
            for idx, tile in enumerate(non_empty, 1):
                if is_cancelled and is_cancelled():
                    rep.cancelled = True
                    break
                if progress_cb:
                    progress_cb(
                        f"Tile {idx}/{len(non_empty)} "
                        f"[{tile[0]:.2f},{tile[2]:.2f}]→[{tile[1]:.2f},{tile[3]:.2f}]",
                        idx - 1, len(non_empty),
                    )
                try:
                    for row in wigle_api.search_bbox(*tile):
                        rep.cells_fetched += 1
                        _apply_row_bulk(conn, row, synthetic_transid, rep)
                    conn.commit()
                except Exception as e:
                    log.warning("Tile %s enrich failed: %s", tile, e)
                    # Keep going; one bad tile shouldn't kill the whole run.
                rep.tiles_done += 1
            if progress_cb and not rep.cancelled:
                progress_cb("Done", rep.tiles_done, rep.tiles_total)
    except Exception as e:
        log.exception("Bulk enrich failed")
        rep.error = str(e)
    return rep


def _apply_row_bulk(conn, row: wigle_api.CellSearchRow,
                    synthetic_transid: str, rep: BulkEnrichReport) -> None:
    """Mirrors _apply_row but funnels counts into BulkEnrichReport instead."""
    band = cb.resolve(row.radio_type, row.channel) if row.channel else None
    band_number = band.number if band else None
    band_label = band.label if band else None

    existing = conn.execute(
        "SELECT id FROM cells WHERE operator_key = ? LIMIT 1",
        (row.operator_key,),
    ).fetchone()

    if existing is not None:
        if row.channel is None:
            rep.rows_skipped_no_band += 1
            return
        updated = conn.execute(
            """
            UPDATE cells
            SET earfcn = ?, band_number = ?, band_label = ?
            WHERE operator_key = ? AND band_number IS NULL
            """,
            (row.channel, band_number, band_label, row.operator_key),
        ).rowcount
        if updated:
            rep.rows_enriched += updated
        return

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
            "mcc": mcc, "mnc": mnc, "xac": xac, "cid": cid,
            "radio_type": row.radio_type or "LTE",
            "carrier": cc.lookup(mcc, mnc, unknown_name_fallback=row.carrier),
            "name_from_kml": row.carrier,
            "signal_dbm": None, "accuracy_m": None,
            "lat": row.lat, "lon": row.lon,
            "first_seen": None, "last_seen": None,
            "earfcn": row.channel,
            "band_number": band_number, "band_label": band_label,
            "source_transid": synthetic_transid,
        },
    )
    rep.rows_inserted += 1
