"""Persistent cache of per-KML scan results.

Keyed by transid → `{size, mtime, wifi, cell, bt}`. On every Database-page
scan we compare each file's current mtime against the cache; only files
that are new or changed get re-opened with GDAL. Entries for files that
disappeared from `~/AirParse/Wigle/` get pruned on load.

Shared by the WiGLE Database page (UI) and — in a future round — the WiFi
subsystem, which will use these totals as fast aggregate stats without
re-parsing the KMLs itself.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Callable, Optional

log = logging.getLogger(__name__)

_MANIFEST_PATH = Path.home() / ".config" / "airparse" / "kml_manifest.json"


@dataclass
class KmlEntry:
    transid: str
    size: int
    mtime: float
    wifi: int
    cell: int
    bt: int
    error: str = ""


def _load_raw() -> dict[str, dict]:
    if not _MANIFEST_PATH.exists():
        return {}
    try:
        data = json.loads(_MANIFEST_PATH.read_text())
        if not isinstance(data, dict):
            return {}
        return data
    except (json.JSONDecodeError, OSError) as e:
        log.warning("Manifest unreadable (%s); starting fresh", e)
        return {}


def _write_raw(data: dict[str, dict]) -> None:
    _MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = _MANIFEST_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, separators=(",", ":")))
    tmp.replace(_MANIFEST_PATH)


def scan(
    kml_dir: Path,
    progress_cb: Optional[Callable[[int, int], None]] = None,
    file_cb: Optional[Callable[[str, KmlEntry], None]] = None,
    force: bool = False,
) -> tuple[list[KmlEntry], dict]:
    """Return (entries, aggregate) after an incremental scan of kml_dir.

    Only re-parses KMLs whose (size, mtime) differs from the cache. Missing
    files are pruned from the persisted manifest. If `file_cb` is given,
    it's called for every entry (including cache hits) in the order they're
    encountered, so the UI can populate its tree incrementally.
    """
    from osgeo import ogr
    ogr.UseExceptions()

    cache = {} if force else _load_raw()
    live_kmls = sorted(kml_dir.glob("*.kml")) if kml_dir.exists() else []
    live_transids = {k.stem for k in live_kmls}

    # Prune disappeared files from the cache
    for transid in list(cache.keys()):
        if transid not in live_transids:
            cache.pop(transid, None)

    entries: list[KmlEntry] = []
    total = len(live_kmls)
    parsed = 0
    changed = False

    for idx, kml in enumerate(live_kmls, 1):
        try:
            st = kml.stat()
        except OSError:
            continue
        prev = cache.get(kml.stem)
        fresh = (
            prev is not None
            and prev.get("size") == st.st_size
            and abs(float(prev.get("mtime", 0.0)) - st.st_mtime) < 0.001
        )
        if fresh:
            entry = KmlEntry(
                transid=kml.stem,
                size=prev["size"],
                mtime=prev["mtime"],
                wifi=int(prev.get("wifi", 0)),
                cell=int(prev.get("cell", 0)),
                bt=int(prev.get("bt", 0)),
                error=prev.get("error", ""),
            )
        else:
            counts = _count_layers(ogr, kml)
            entry = KmlEntry(
                transid=kml.stem,
                size=st.st_size,
                mtime=st.st_mtime,
                wifi=counts["wifi"],
                cell=counts["cell"],
                bt=counts["bt"],
                error=counts.get("error", ""),
            )
            cache[kml.stem] = asdict(entry)
            parsed += 1
            changed = True
        entries.append(entry)
        if file_cb:
            file_cb(kml.stem, entry)
        if progress_cb:
            progress_cb(idx, total)

    if changed:
        _write_raw(cache)

    agg = _aggregate(entries, parsed)
    return entries, agg


def read_cached(kml_dir: Path) -> tuple[list[KmlEntry], dict]:
    """Return whatever's in the manifest right now, trimmed to files that
    still exist on disk. No parsing, no writes. Used by subsystems that
    want aggregate totals without triggering a full scan."""
    cache = _load_raw()
    live = {k.stem for k in kml_dir.glob("*.kml")} if kml_dir.exists() else set()
    entries = [
        KmlEntry(
            transid=t,
            size=int(v.get("size", 0)),
            mtime=float(v.get("mtime", 0.0)),
            wifi=int(v.get("wifi", 0)),
            cell=int(v.get("cell", 0)),
            bt=int(v.get("bt", 0)),
            error=v.get("error", ""),
        )
        for t, v in cache.items()
        if t in live
    ]
    entries.sort(key=lambda e: e.transid)
    return entries, _aggregate(entries, 0)


def _count_layers(ogr_mod, kml_path: Path) -> dict:
    row = {"wifi": 0, "cell": 0, "bt": 0}
    try:
        ds = ogr_mod.Open(str(kml_path))
    except Exception as e:
        return {**row, "error": str(e)}
    if ds is None:
        return {**row, "error": "Couldn't open KML"}
    try:
        for i in range(ds.GetLayerCount()):
            lyr = ds.GetLayerByIndex(i)
            name = lyr.GetName()
            count = lyr.GetFeatureCount()
            if name == "Wifi Networks":
                row["wifi"] = count
            elif name == "Cellular Networks":
                row["cell"] = count
            elif name == "Bluetooth Networks":
                row["bt"] = count
    finally:
        ds = None
    return row


def _aggregate(entries: list[KmlEntry], parsed_this_run: int) -> dict:
    agg = {
        "files": len(entries),
        "size_bytes": sum(e.size for e in entries),
        "wifi": sum(e.wifi for e in entries),
        "cell": sum(e.cell for e in entries),
        "bt": sum(e.bt for e in entries),
        "earliest": None,
        "latest": None,
        "parsed_this_run": parsed_this_run,
    }
    for e in entries:
        date = e.transid[:8]
        if len(date) == 8 and date.isdigit():
            if agg["earliest"] is None or date < agg["earliest"]:
                agg["earliest"] = date
            if agg["latest"] is None or date > agg["latest"]:
                agg["latest"] = date
    return agg
