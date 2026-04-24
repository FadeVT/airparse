"""Parse the `Wifi Networks` layer out of WiGLE KML downloads into wifi_points.db.

Fields from the Description blob match what the cell reader extracts — just a
different set of keys:
  Network ID: AA:BB:CC:DD:EE:FF     ← BSSID
  Encryption: WPA2
  Time: 2025-04-24T17:38:23.000-07:00
  Signal: -81.0
  Accuracy: 3.06122
  Type: WIFI
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

try:
    from osgeo import ogr
    ogr.UseExceptions()
    _HAS_GDAL = True
except ImportError:
    ogr = None
    _HAS_GDAL = False

from wifi import db

log = logging.getLogger(__name__)

_KML_DIR = Path.home() / "AirParse" / "Wigle"
_WIFI_LAYER = "Wifi Networks"

_DESC_KEY_RE = re.compile(r"^([A-Za-z ]+):\s*(.+?)\s*$", re.MULTILINE)


@dataclass
class ImportReport:
    transids_scanned: int = 0
    transids_imported: int = 0
    transids_skipped: int = 0
    observations_inserted: int = 0
    files_without_wifi_layer: int = 0
    errors: list[str] = field(default_factory=list)


def import_all(
    progress_cb: Optional[Callable[[str], None]] = None,
    force: bool = False,
) -> ImportReport:
    """Incremental KML ingest. Batched via a single transaction per file so
    SQLite doesn't fsync per row — 3M+ observations still complete in ~30s."""
    if not _HAS_GDAL:
        raise RuntimeError("python-gdal not available")

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
                progress_cb(f"WiFi import: {kml.name} ({idx}/{len(kmls)})")
            try:
                conn.execute("BEGIN")
                added, had_layer = _import_one_kml(conn, kml, transid)
                conn.execute(
                    "INSERT OR REPLACE INTO wifi_imported_transids "
                    "(transid, imported_at, observation_count) VALUES (?, ?, ?)",
                    (transid, _now_iso(), added),
                )
                conn.execute("COMMIT")
            except Exception as e:
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
                log.exception("WiFi import failed on %s", kml.name)
                rep.errors.append(f"{kml.name}: {e}")
                continue
            rep.observations_inserted += added
            rep.transids_imported += 1
            if not had_layer:
                rep.files_without_wifi_layer += 1
    return rep


def _import_one_kml(conn, kml_path: Path, transid: str) -> tuple[int, bool]:
    src = ogr.Open(str(kml_path))
    if src is None:
        return 0, False
    try:
        layer = None
        for i in range(src.GetLayerCount()):
            candidate = src.GetLayerByIndex(i)
            if candidate.GetName() == _WIFI_LAYER:
                layer = candidate
                break
        if layer is None:
            return 0, False

        rows: list[tuple] = []
        layer.ResetReading()
        for feat in layer:
            parsed = _parse_feature(feat, transid)
            if parsed is not None:
                rows.append(parsed)
        if not rows:
            return 0, True

        conn.executemany(
            """
            INSERT OR IGNORE INTO wifi_observations
            (bssid, ssid, encryption, signal_dbm, accuracy_m, lat, lon,
             seen_at, source_transid)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        return len(rows), True
    finally:
        src = None


def _parse_feature(feat, transid: str) -> Optional[tuple]:
    geom = feat.GetGeometryRef()
    if geom is None:
        return None
    lon, lat = geom.GetX(), geom.GetY()
    if (lat == 0 and lon == 0) or lat is None or lon is None:
        return None

    ssid = feat.GetField("Name") or ""
    desc = feat.GetField("Description") or ""
    kv = dict(_DESC_KEY_RE.findall(desc))

    bssid = kv.get("Network ID", "").strip()
    if not bssid:
        return None

    encryption = kv.get("Encryption", "").strip() or None
    signal = _to_float(kv.get("Signal"))
    accuracy = _to_float(kv.get("Accuracy"))
    seen_at = kv.get("Time", "").strip() or None

    return (
        bssid, ssid, encryption, signal, accuracy, lat, lon, seen_at, transid,
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
