"""QGIS GeoPackage merge — write new WiGLE KMLs into per-year GPKGs.

Style preservation note: QGIS stores layer styles either in the project file
or in the GeoPackage's own `layer_styles` table. As long as we only INSERT
features into the target layer and never ALTER its schema or touch any
metadata/style tables, existing QGIS styling is preserved untouched.

State tracking: a `_airparse_merge_state` table inside each GPKG records the
transids we've already imported into that file. Re-running the export only
imports KMLs whose transid isn't in the state table, making the operation
idempotent.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Callable, Optional

try:
    from osgeo import ogr, osr
    ogr.UseExceptions()
    _HAS_GDAL = True
except ImportError:
    ogr = None
    osr = None
    _HAS_GDAL = False

log = logging.getLogger(__name__)

CONFIG_PATH = Path.home() / '.config' / 'airparse' / 'qgis_config.json'
STATE_TABLE = '_airparse_merge_state'


# ─── Config ────────────────────────────────────────────────────────

@dataclass
class QgisConfig:
    """User-configurable paths + patterns for the QGIS GeoPackage export."""

    folder: str = ''
    gpkg_pattern: str = '{year} WIFI.gpkg'
    layer_pattern: str = '{year} WIFI'
    skip_imported_transids: bool = True
    create_missing_year: bool = False

    @classmethod
    def load(cls) -> 'QgisConfig':
        if not CONFIG_PATH.exists():
            return cls()
        try:
            raw = json.loads(CONFIG_PATH.read_text())
            return cls(**{k: v for k, v in raw.items() if k in cls.__annotations__})
        except Exception as e:
            log.warning("Couldn't read QGIS config (%s); using defaults", e)
            return cls()

    def save(self) -> None:
        CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        CONFIG_PATH.write_text(json.dumps(asdict(self), indent=2))

    def resolve_gpkg(self, year: str) -> Path:
        return Path(self.folder).expanduser() / self.gpkg_pattern.format(year=year)

    def resolve_layer(self, year: str) -> str:
        return self.layer_pattern.format(year=year)


# ─── Year helpers ──────────────────────────────────────────────────

_YEAR_RE = re.compile(r'^(\d{4})')


def year_from_transid(stem: str) -> str:
    """WiGLE transid filenames are `YYYYMMDD-NNNNN` — the 4-digit year prefix."""
    m = _YEAR_RE.match(stem)
    return m.group(1) if m else 'unknown'


def group_kmls_by_year(kml_dir: Path) -> dict[str, list[Path]]:
    buckets: dict[str, list[Path]] = {}
    if not kml_dir.exists():
        return buckets
    for kml in sorted(kml_dir.glob('*.kml')):
        buckets.setdefault(year_from_transid(kml.stem), []).append(kml)
    return buckets


# ─── Detect / Validate ─────────────────────────────────────────────

@dataclass
class YearReport:
    year: str
    gpkg_path: Path
    gpkg_exists: bool
    target_layer: str
    layer_found: bool
    layers_in_gpkg: list[str] = field(default_factory=list)
    feature_count: int = 0
    field_names: list[str] = field(default_factory=list)
    already_imported_transids: set[str] = field(default_factory=set)
    kml_files: list[Path] = field(default_factory=list)
    new_transids: list[str] = field(default_factory=list)


def detect(config: QgisConfig, kml_dir: Path) -> list[YearReport]:
    """Inspect what's in the config'd folder — one report per year we have KMLs for."""
    if not _HAS_GDAL:
        raise RuntimeError("osgeo.ogr not available — install python-gdal")

    reports: list[YearReport] = []
    buckets = group_kmls_by_year(kml_dir)
    for year, kmls in sorted(buckets.items()):
        r = YearReport(
            year=year,
            gpkg_path=config.resolve_gpkg(year),
            gpkg_exists=False,
            target_layer=config.resolve_layer(year),
            layer_found=False,
            kml_files=kmls,
        )
        if r.gpkg_path.exists():
            r.gpkg_exists = True
            _inspect_gpkg(r)
        r.new_transids = [
            p.stem for p in kmls if p.stem not in r.already_imported_transids
        ]
        reports.append(r)
    return reports


_FALLBACK_SKIP_PREFIXES = ('gpkg_', 'rtree_', 'sqlite_')
_FALLBACK_SKIP_EXACT = {'layer_styles', STATE_TABLE}


def _resolve_target_layer(ds, expected_name: str) -> tuple[Optional[str], object]:
    """Find the right features table. Tries exact → case-insensitive →
    first features-looking layer. Returns (actual_name, layer_handle)."""
    layer = ds.GetLayerByName(expected_name)
    if layer is not None:
        return expected_name, layer

    all_names = [ds.GetLayerByIndex(i).GetName() for i in range(ds.GetLayerCount())]
    # Case-insensitive match
    for name in all_names:
        if name.lower() == expected_name.lower():
            return name, ds.GetLayerByName(name)

    # Fallback: first user-features layer (skip GPKG internals + our own state table)
    for name in all_names:
        if name in _FALLBACK_SKIP_EXACT:
            continue
        if any(name.startswith(p) for p in _FALLBACK_SKIP_PREFIXES):
            continue
        return name, ds.GetLayerByName(name)

    return None, None


def _inspect_gpkg(r: YearReport) -> None:
    """Populate layer list, feature count, schema, and imported-transids from the GPKG."""
    try:
        ds = ogr.Open(str(r.gpkg_path), update=0)
    except Exception as e:
        log.warning("Couldn't open %s: %s", r.gpkg_path, e)
        return
    if ds is None:
        return
    try:
        r.layers_in_gpkg = [ds.GetLayerByIndex(i).GetName() for i in range(ds.GetLayerCount())]
        actual_name, layer = _resolve_target_layer(ds, r.target_layer)
        if layer is not None:
            r.layer_found = True
            if actual_name and actual_name != r.target_layer:
                # Rewrite the target to reflect what we'll actually use so
                # downstream messaging and merge() both agree.
                r.target_layer = actual_name
            r.feature_count = layer.GetFeatureCount()
            d = layer.GetLayerDefn()
            r.field_names = [d.GetFieldDefn(i).GetName() for i in range(d.GetFieldCount())]
        r.already_imported_transids = _read_imported_transids(ds)
    finally:
        ds = None  # closes


# ─── Merge ─────────────────────────────────────────────────────────

@dataclass
class MergeResult:
    year: str
    gpkg_path: Path
    layer: str
    action: str  # 'merged', 'created', 'skipped_no_gpkg', 'skipped_no_layer', 'up_to_date'
    files_imported: int = 0
    features_added: int = 0
    transids_imported: list[str] = field(default_factory=list)
    message: str = ''


def merge(
    config: QgisConfig,
    kml_dir: Path,
    years: Optional[set[str]] = None,
    progress_cb: Optional[Callable[[str], None]] = None,
    force_reimport: bool = False,
) -> list[MergeResult]:
    """Walk every year bucket, write new KMLs into the matching GPKG."""
    if not _HAS_GDAL:
        raise RuntimeError("osgeo.ogr not available — install python-gdal")
    if not config.folder:
        raise RuntimeError("QGIS folder not configured — open the QGIS page and set it first")

    results: list[MergeResult] = []
    buckets = group_kmls_by_year(kml_dir)

    for year, kmls in sorted(buckets.items()):
        if years is not None and year not in years:
            continue

        gpkg_path = config.resolve_gpkg(year)
        layer_name = config.resolve_layer(year)
        result = MergeResult(year=year, gpkg_path=gpkg_path, layer=layer_name, action='up_to_date')

        if not gpkg_path.exists():
            if not config.create_missing_year:
                result.action = 'skipped_no_gpkg'
                result.message = f"No GeoPackage at {gpkg_path}"
                results.append(result)
                continue
            if progress_cb:
                progress_cb(f"{year}: creating {gpkg_path.name} from KML schema")
            _create_gpkg_from_first_kml(gpkg_path, layer_name, kmls[0])
            result.action = 'created'

        if progress_cb:
            progress_cb(f"{year}: opening {gpkg_path.name}")
        ds = ogr.Open(str(gpkg_path), update=1)
        if ds is None:
            result.action = 'skipped_no_gpkg'
            result.message = f"Couldn't open {gpkg_path}"
            results.append(result)
            continue

        try:
            actual_name, layer = _resolve_target_layer(ds, layer_name)
            if layer is None:
                result.action = 'skipped_no_layer'
                available = ', '.join(ds.GetLayerByIndex(i).GetName()
                                      for i in range(ds.GetLayerCount()))
                result.message = (f"No usable layer in {gpkg_path.name}. "
                                  f"Layers found: {available}")
                results.append(result)
                continue
            if actual_name != layer_name:
                # Inform the user we auto-resolved to a different table name.
                result.layer = actual_name
                if progress_cb:
                    progress_cb(
                        f"{year}: using layer '{actual_name}' "
                        f"(expected '{layer_name}', auto-detected)"
                    )

            _ensure_state_table(ds)
            already = _read_imported_transids(ds)

            to_import = kmls if force_reimport else [
                p for p in kmls if p.stem not in already
            ]
            if not to_import:
                result.message = f"All {len(kmls)} KML(s) already imported."
                results.append(result)
                continue

            # Pre-read the target schema once — drop KML fields that don't
            # exist on the target and use NULLs for target fields the KML
            # doesn't supply. Never ALTER the target schema.
            target_defn = layer.GetLayerDefn()
            target_fields = {
                target_defn.GetFieldDefn(i).GetName(): i
                for i in range(target_defn.GetFieldCount())
            }

            total_added = 0
            for idx, kml_path in enumerate(to_import, 1):
                if progress_cb:
                    progress_cb(f"{year}: importing {kml_path.name} ({idx}/{len(to_import)})")
                # One transaction per KML: every feature insert + the state
                # record commit atomically. Without this, GPKG's default is
                # one commit per CreateFeature, which is ~100x slower on big
                # files because SQLite fsyncs and updates the R-tree index
                # per row.
                ds.StartTransaction()
                try:
                    added = _import_one_kml(kml_path, layer, target_fields)
                    _record_transid(ds, kml_path.stem, added)
                    ds.CommitTransaction()
                except Exception:
                    ds.RollbackTransaction()
                    raise
                total_added += added
                result.transids_imported.append(kml_path.stem)

            result.action = 'merged' if result.action != 'created' else 'created'
            result.files_imported = len(to_import)
            result.features_added = total_added
            result.message = (
                f"Added {total_added:,} features from {len(to_import)} KML "
                f"file(s). {len(kmls) - len(to_import)} file(s) already on "
                f"record, skipped."
            )
        finally:
            ds = None

        results.append(result)

    return results


def _import_one_kml(
    kml_path: Path, target_layer, target_fields: dict[str, int]
) -> int:
    """Read every feature from kml_path, write to target_layer. Returns added count."""
    src = ogr.Open(str(kml_path))
    if src is None:
        log.warning("Couldn't open KML %s", kml_path)
        return 0
    added = 0
    try:
        target_defn = target_layer.GetLayerDefn()
        for src_layer in src:
            src_defn = src_layer.GetLayerDefn()
            src_field_names = [
                src_defn.GetFieldDefn(i).GetName()
                for i in range(src_defn.GetFieldCount())
            ]
            shared_fields = [name for name in src_field_names if name in target_fields]

            src_layer.ResetReading()
            for src_feat in src_layer:
                new_feat = ogr.Feature(target_defn)
                for name in shared_fields:
                    val = src_feat.GetField(name)
                    if val is not None:
                        new_feat.SetField(name, val)
                src_geom = src_feat.GetGeometryRef()
                if src_geom is not None:
                    new_feat.SetGeometry(src_geom.Clone())
                if target_layer.CreateFeature(new_feat) == 0:
                    added += 1
                new_feat = None
    finally:
        src = None
    return added


# ─── State table ───────────────────────────────────────────────────
#
# State is a plain SQLite table inside the GPKG (not an OGR layer). GPKG is
# just SQLite with a schema on top, so ExecuteSQL gets us full access without
# having to register our tracking table as a QGIS-visible layer. Keeping it
# off the OGR layer registry also means it never shows up in QGIS's Layers
# panel — which is what we want.

def _ensure_state_table(ds) -> None:
    sql = (
        f"CREATE TABLE IF NOT EXISTS {STATE_TABLE} ("
        f"transid TEXT PRIMARY KEY NOT NULL, "
        f"imported_at TEXT NOT NULL, "
        f"feature_count INTEGER NOT NULL DEFAULT 0)"
    )
    ds.ExecuteSQL(sql)


def _read_imported_transids(ds) -> set[str]:
    """Read the state table via SQL. Returns empty set if the table is absent."""
    out: set[str] = set()
    try:
        lyr = ds.ExecuteSQL(f"SELECT transid FROM {STATE_TABLE}")
    except Exception:
        return out
    if lyr is None:
        return out
    try:
        lyr.ResetReading()
        for ft in lyr:
            t = ft.GetField('transid')
            if t:
                out.add(t)
    finally:
        ds.ReleaseResultSet(lyr)
    return out


def _record_transid(ds, transid: str, feature_count: int) -> None:
    escaped = transid.replace("'", "''")
    sql = (
        f"INSERT OR REPLACE INTO {STATE_TABLE} "
        f"(transid, imported_at, feature_count) "
        f"VALUES ('{escaped}', '{_now_iso()}', {int(feature_count)})"
    )
    ds.ExecuteSQL(sql)


def _now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


# ─── Create missing GPKG ───────────────────────────────────────────

def _create_gpkg_from_first_kml(gpkg_path: Path, layer_name: str, kml_path: Path) -> None:
    """Build a new GPKG whose layer mirrors the KML's schema + WGS84 SRS.

    Used only when `create_missing_year` is on. The first QGIS open will
    show this layer unstyled — that's expected; QGIS styles are bound to
    a layer's source path + schema, and there's nothing styled yet.
    """
    gpkg_path.parent.mkdir(parents=True, exist_ok=True)
    driver = ogr.GetDriverByName('GPKG')
    if gpkg_path.exists():
        return
    dst = driver.CreateDataSource(str(gpkg_path))
    try:
        src = ogr.Open(str(kml_path))
        if src is None:
            raise RuntimeError(f"Couldn't open KML {kml_path}")
        try:
            src_layer = src.GetLayerByIndex(0)
            srs = osr.SpatialReference()
            srs.ImportFromEPSG(4326)
            new_layer = dst.CreateLayer(
                layer_name, srs, ogr.wkbPoint,
            )
            src_defn = src_layer.GetLayerDefn()
            for i in range(src_defn.GetFieldCount()):
                new_layer.CreateField(src_defn.GetFieldDefn(i))
        finally:
            src = None
    finally:
        dst = None
