"""Parse the `Cellular Networks` layer out of WiGLE KML downloads into cells.db.

Operates on the same `~/AirParse/Wigle/` directory the WiFi side uses, but
strictly read-only. Extracted description fields (Network ID, Time, Signal,
Accuracy, Type) are parsed into structured columns plus carrier + operator key
resolution.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Optional

try:
    from osgeo import ogr
    ogr.UseExceptions()
    _HAS_GDAL = True
except ImportError:
    ogr = None
    _HAS_GDAL = False

from cell import carriers as cc
from cell import db
from cell import bands as cb

log = logging.getLogger(__name__)


_KML_DIR = Path.home() / "AirParse" / "Wigle"
_CELLULAR_LAYER = "Cellular Networks"


# Fields we expect in the `Description` blob of a WiGLE cell placemark.
# Format WiGLE emits (one line per key):
#   Network ID: 310260_21235_224504331
#   Time: 2025-04-24T22:45:24.000-07:00
#   Signal: -108.0
#   Accuracy: 71.9388
#   Type: LTE
_DESC_KEY_RE = re.compile(r"^([A-Za-z ]+):\s*(.+?)\s*$", re.MULTILINE)


@dataclass
class ImportReport:
    transids_scanned: int = 0
    transids_imported: int = 0
    transids_skipped: int = 0
    cells_inserted: int = 0
    files_without_cell_layer: int = 0
    errors: list[str] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []


def import_all(
    progress_cb: Optional[Callable[[str], None]] = None,
    force: bool = False,
) -> ImportReport:
    """Walk ~/AirParse/Wigle/*.kml and ingest every new transid's cells.

    Idempotent via the `imported_transids` table. Set `force=True` to
    re-ingest (useful for schema changes or after manual cleanup).
    """
    if not _HAS_GDAL:
        raise RuntimeError("python-gdal not available — install it first")

    rep = ImportReport()
    kmls = sorted(_KML_DIR.glob("*.kml")) if _KML_DIR.exists() else []
    already = set() if force else db.imported_transids()

    with db.connect() as conn:
        for idx, kml in enumerate(kmls, 1):
            rep.transids_scanned += 1
            transid = kml.stem
            if transid in already:
                rep.transids_skipped += 1
                continue
            if progress_cb:
                progress_cb(f"Importing cells from {kml.name} ({idx}/{len(kmls)})")
            try:
                added, had_layer = _import_one_kml(conn, kml, transid)
                rep.cells_inserted += added
                rep.transids_imported += 1
                if not had_layer:
                    rep.files_without_cell_layer += 1
                conn.execute(
                    "INSERT OR REPLACE INTO imported_transids "
                    "(transid, imported_at, cell_count) VALUES (?, ?, ?)",
                    (transid, _now_iso(), added),
                )
                conn.commit()
            except Exception as e:
                log.exception("Cell import failed on %s", kml.name)
                rep.errors.append(f"{kml.name}: {e}")

    return rep


def _import_one_kml(conn, kml_path: Path, transid: str) -> tuple[int, bool]:
    """Pull the Cellular Networks layer from one KML. Returns (added, had_layer)."""
    src = ogr.Open(str(kml_path))
    if src is None:
        return 0, False
    try:
        layer = None
        for i in range(src.GetLayerCount()):
            candidate = src.GetLayerByIndex(i)
            if candidate.GetName() == _CELLULAR_LAYER:
                layer = candidate
                break
        if layer is None:
            return 0, False

        added = 0
        layer.ResetReading()
        for feat in layer:
            parsed = _parse_feature(feat)
            if parsed is None:
                continue
            parsed["source_transid"] = transid
            _insert(conn, parsed)
            added += 1
        return added, True
    finally:
        src = None


def _parse_feature(feat) -> Optional[dict]:
    geom = feat.GetGeometryRef()
    if geom is None:
        return None
    # KMLs have geometries with potentially-altitude-carrying points. Take x/y only.
    x, y = geom.GetX(), geom.GetY()
    if not x and not y:
        return None

    name_from_kml = feat.GetField("Name") or ""
    desc = feat.GetField("Description") or ""
    kv = dict(_DESC_KEY_RE.findall(desc))

    op_key = kv.get("Network ID", "").strip()
    if not op_key:
        return None
    mcc, mnc, xac_s, cid_s = cc.split_operator_key(op_key)
    if not mcc or not mnc:
        return None

    try:
        xac = int(xac_s)
        cid = int(cid_s)
    except ValueError:
        return None

    radio_type = kv.get("Type", "").strip().upper()
    signal = _to_float(kv.get("Signal"))
    accuracy = _to_float(kv.get("Accuracy"))
    # Time is a single timestamp per placemark — treat it as both first and
    # last until we have multiple observations per cell to distinguish.
    ts = kv.get("Time", "").strip() or None

    return {
        "operator_key": op_key,
        "mcc": mcc,
        "mnc": mnc,
        "xac": xac,
        "cid": cid,
        "radio_type": radio_type,
        "carrier": cc.lookup(mcc, mnc, unknown_name_fallback=name_from_kml),
        "name_from_kml": name_from_kml,
        "signal_dbm": signal,
        "accuracy_m": accuracy,
        "lat": y,
        "lon": x,
        "first_seen": ts,
        "last_seen": ts,
        "earfcn": None,         # Slice 2 enrichment
        "band_number": None,
        "band_label": None,
    }


def _insert(conn, row: dict) -> None:
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
        row,
    )


def _to_float(v: Optional[str]) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except ValueError:
        return None


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ─── Query helpers used by the UI ──────────────────────────────────

def query_cells(
    carriers: Optional[Iterable[str]] = None,
    radio_types: Optional[Iterable[str]] = None,
    bands: Optional[Iterable[str]] = None,
    limit: Optional[int] = None,
) -> list[dict]:
    """Filter for the map view — carrier + radio_type + band multi-select."""
    clauses = []
    params: list = []
    if carriers:
        marks = ",".join("?" for _ in carriers)
        clauses.append(f"carrier IN ({marks})")
        params.extend(carriers)
    if radio_types:
        marks = ",".join("?" for _ in radio_types)
        clauses.append(f"radio_type IN ({marks})")
        params.extend(radio_types)
    if bands:
        marks = ",".join("?" for _ in bands)
        clauses.append(f"band_label IN ({marks})")
        params.extend(bands)

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    lim = f"LIMIT {int(limit)}" if limit else ""
    sql = f"""
        SELECT id, operator_key, mcc, mnc, xac, cid, radio_type, carrier,
               name_from_kml, signal_dbm, accuracy_m, lat, lon,
               first_seen, last_seen, earfcn, band_number, band_label
        FROM cells
        {where}
        {lim}
    """
    with db.connect() as conn:
        return [dict(r) for r in conn.execute(sql, params)]


def distinct_carriers() -> list[str]:
    with db.connect() as conn:
        return [r[0] for r in conn.execute(
            "SELECT DISTINCT carrier FROM cells ORDER BY carrier"
        )]


def distinct_radio_types() -> list[str]:
    with db.connect() as conn:
        return [r[0] for r in conn.execute(
            "SELECT DISTINCT radio_type FROM cells ORDER BY radio_type"
        )]


def distinct_bands() -> list[str]:
    """Band labels that at least one cell has been resolved to. Sorted so the
    B-number and n-number groups read naturally (B2 < B4 < … < n41 < n71)."""
    with db.connect() as conn:
        rows = [r[0] for r in conn.execute(
            "SELECT DISTINCT band_label FROM cells "
            "WHERE band_label IS NOT NULL AND band_label != '' "
            "ORDER BY band_label"
        )]
    # Group LTE first (B-prefix), then NR (n-prefix), each in numeric order.
    def sort_key(label: str) -> tuple[int, int]:
        tier = 0 if label.startswith("B") else 1
        digits = "".join(c for c in label if c.isdigit())
        return (tier, int(digits) if digits else 0)
    return sorted(rows, key=sort_key)


def band_counts() -> dict[str, int]:
    """Distinct-cell counts per resolved band_label. Used to annotate the Band
    filter checkboxes with ("B66 — Extended AWS (1,247)") so the user can see
    at a glance how much data they have per band without running a filter."""
    with db.connect() as conn:
        return {
            r[0]: int(r[1])
            for r in conn.execute(
                "SELECT band_label, COUNT(DISTINCT operator_key) "
                "FROM cells "
                "WHERE band_label IS NOT NULL AND band_label != '' "
                "GROUP BY band_label"
            )
        }


def unenriched_bbox() -> tuple[float, float, float, float] | None:
    """Bounding box of every cell whose band hasn't been resolved yet.
    Returns (south, north, west, east) or None if there are no unenriched cells.
    Drives the 'Enrich All Unenriched' flow — we walk this bbox in a grid."""
    with db.connect() as conn:
        row = conn.execute(
            "SELECT MIN(lat), MAX(lat), MIN(lon), MAX(lon), COUNT(*) "
            "FROM cells "
            "WHERE (band_number IS NULL) "
            "  AND lat IS NOT NULL AND lon IS NOT NULL"
        ).fetchone()
    if not row or not row[4]:
        return None
    south, north, west, east = row[0], row[1], row[2], row[3]
    if None in (south, north, west, east):
        return None
    return (float(south), float(north), float(west), float(east))


def unenriched_operator_count() -> int:
    """Unique towers (operator_keys) that don't yet have band info."""
    with db.connect() as conn:
        return int(conn.execute(
            "SELECT COUNT(DISTINCT operator_key) FROM cells WHERE band_number IS NULL"
        ).fetchone()[0])
