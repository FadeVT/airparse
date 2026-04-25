"""WiGLE tab — dashboard with map, upload, transactions, search, and downloads."""

import json
import logging
import shutil
import xml.etree.ElementTree as ET
from pathlib import Path

from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QFrame, QGroupBox, QScrollArea, QStackedWidget, QTreeWidget,
    QTreeWidgetItem, QHeaderView, QFileDialog, QProgressBar,
    QLineEdit, QDateEdit, QCheckBox, QSizePolicy, QDialog,
    QTextEdit, QMessageBox, QTableWidget, QTableWidgetItem,
    QAbstractItemView, QMenu, QDialogButtonBox, QRadioButton,
    QButtonGroup, QPlainTextEdit,
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QTimer, QDate
from PyQt6.QtGui import QFont, QColor, QAction

try:
    from PyQt6.QtWebEngineWidgets import QWebEngineView
    HAS_WEBENGINE = True
except ImportError:
    HAS_WEBENGINE = False

from database.wigle_api import WigleApiClient
from export import qgis_geopackage as qg

log = logging.getLogger(__name__)

_KML_DIR = Path.home() / 'AirParse' / 'Wigle'
_STAGE_DIR = Path.home() / '.config' / 'airparse' / 'wigle_uploads'
_IGNORED_PATH = Path.home() / '.config' / 'airparse' / 'wigle_ignored_transids.json'

_LABEL_STYLE = "color: #e0e0e0; border: none; background: transparent;"
_DIM_STYLE = "color: #999; border: none; background: transparent; font-size: 11px;"
_GREEN = "color: #2ecc71; border: none; background: transparent;"
_RED = "color: #e74c3c; border: none; background: transparent;"
_YELLOW = "color: #f39c12; border: none; background: transparent;"

_GROUP_STYLE = """
    QGroupBox { color: #e0e0e0; border: 1px solid #444; border-radius: 6px;
                margin-top: 8px; padding-top: 16px; }
    QGroupBox::title { subcontrol-origin: margin; padding: 0 6px; }
"""
_TREE_STYLE = """
    QTreeWidget {
        background-color: #2b2b2b; color: #e0e0e0;
        border: 1px solid #444; border-radius: 4px;
        alternate-background-color: #313131;
    }
    QTreeWidget::item:selected { background-color: #2980b9; }
    QHeaderView::section {
        background-color: #333; color: #e0e0e0;
        border: 1px solid #444; padding: 4px; font-size: 11px;
    }
"""
_INPUT_STYLE = """
    QLineEdit, QDateEdit {
        background-color: #3c3f41; color: #e0e0e0;
        border: 1px solid #555; border-radius: 3px; padding: 4px;
    }
    QCheckBox { color: #e0e0e0; }
"""

# Transaction columns matching WiGLE web UI
_STATUS_CODES = {
    'T': 'Trilaterating',
    'S': 'Success',
    'F': 'Failed',
    'Q': 'Queued',
    'P': 'Processing',
    'G': 'Geolocating',
    'N': 'New',
    'D': 'Done',
}

_TX_COLUMNS = [
    "Trans ID", "Status", "Uploaded", "File", "Size",
    "WiFi New", "WiFi Upd", "WiFi Total",
    "Cell New", "Cell Upd", "Cell Total",
    "BT New", "BT Upd", "BT Total",
    "Queue",
]


def _action_btn(text, color="#3c3f41", text_color="#e0e0e0", bold=False):
    btn = QPushButton(text)
    weight = "bold" if bold else "normal"
    btn.setStyleSheet(f"""
        QPushButton {{
            background-color: {color}; color: {text_color};
            border: 1px solid #555; border-radius: 4px;
            padding: 6px 16px; font-weight: {weight};
        }}
        QPushButton:hover {{ background-color: {color}; opacity: 0.8; }}
        QPushButton:disabled {{ background-color: #333; color: #666; }}
    """)
    btn.setCursor(Qt.CursorShape.PointingHandCursor)
    return btn


def _populate_tx_tree(tree: QTreeWidget, txns: list):
    """Populate a transaction tree with full WiGLE column data."""
    tree.clear()
    for tx in txns:
        transid = tx.get('transid', '')
        if not transid:
            continue
        raw_status = tx.get('status', '')
        status = _STATUS_CODES.get(raw_status, raw_status)
        uploaded = tx.get('firstTime', '')
        if uploaded:
            uploaded = uploaded.split('T')[0]
        elif len(transid) >= 8:
            d = transid[:8]
            uploaded = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
        fname = tx.get('fileName', '')
        fsize = tx.get('fileSize', 0)
        if isinstance(fsize, (int, float)) and fsize > 0:
            fsize = _fmt_bytes(int(fsize))
        else:
            fsize = str(fsize) if fsize else ''

        wifi_disc = tx.get('discovered', 0) or 0
        wifi_total_val = tx.get('total', 0) or 0
        wifi_upd = wifi_total_val - wifi_disc if wifi_total_val >= wifi_disc else 0
        cell_disc = tx.get('genDiscovered', 0) or 0
        cell_total_val = tx.get('genTotal', 0) or 0
        cell_upd = cell_total_val - cell_disc if cell_total_val >= cell_disc else 0
        bt_disc = tx.get('btDiscovered', 0) or 0
        bt_total_val = tx.get('btTotal', 0) or 0
        bt_upd = bt_total_val - bt_disc if bt_total_val >= bt_disc else 0

        wifi_new = str(wifi_disc)
        wifi_upd = str(wifi_upd)
        wifi_total = str(wifi_total_val)
        cell_new = str(cell_disc)
        cell_upd = str(cell_upd)
        cell_total = str(cell_total_val)
        bt_new = str(bt_disc)
        bt_upd = str(bt_upd)
        bt_total = str(bt_total_val)
        wait = str(tx.get('wait', ''))

        item = QTreeWidgetItem([
            transid, status, uploaded, fname, fsize,
            wifi_new, wifi_upd, wifi_total,
            cell_new, cell_upd, cell_total,
            bt_new, bt_upd, bt_total,
            wait,
        ])
        item.setData(0, Qt.ItemDataRole.UserRole, transid)

        # Color the status
        s = status.lower()
        if s in ('success', 'done', 'downloaded'):
            item.setForeground(1, QColor('#2ecc71'))
        elif s in ('trilaterating', 'processing', 'geolocating', 'queued', 'new'):
            item.setForeground(1, QColor('#f39c12'))
        elif s in ('failed', 'error'):
            item.setForeground(1, QColor('#e74c3c'))

        tree.addTopLevelItem(item)


def _make_tx_tree() -> QTreeWidget:
    """Create a transaction tree widget with the full WiGLE columns."""
    tree = QTreeWidget()
    tree.setHeaderLabels(_TX_COLUMNS)
    tree.setAlternatingRowColors(True)
    tree.setStyleSheet(_TREE_STYLE)
    tree.setRootIsDecorated(False)
    tree.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
    h = tree.header()
    h.setStretchLastSection(False)
    h.setSectionResizeMode(0, QHeaderView.ResizeMode.Interactive)
    h.resizeSection(0, 180)
    h.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
    h.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
    h.setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
    for i in range(4, len(_TX_COLUMNS)):
        h.setSectionResizeMode(i, QHeaderView.ResizeMode.ResizeToContents)
    return tree


def _fmt_bytes(size: int) -> str:
    for unit in ('B', 'KB', 'MB', 'GB'):
        if size < 1024:
            return f"{size:.1f} {unit}" if unit != 'B' else f"{size} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def _load_ignored_transids() -> set:
    if _IGNORED_PATH.exists():
        try:
            return set(json.loads(_IGNORED_PATH.read_text()))
        except (json.JSONDecodeError, OSError):
            pass
    return set()


def _save_ignored_transids(ignored: set):
    _IGNORED_PATH.parent.mkdir(parents=True, exist_ok=True)
    _IGNORED_PATH.write_text(json.dumps(sorted(ignored)))


def _add_ignored_transid(transid: str):
    ignored = _load_ignored_transids()
    ignored.add(transid)
    _save_ignored_transids(ignored)


# ─── Workers ────────────────────────────────────────────────────────

class _StatsWorker(QThread):
    result = pyqtSignal(dict)
    def run(self):
        self.result.emit(WigleApiClient().get_user_stats())


class _TransactionsWorker(QThread):
    result = pyqtSignal(list)
    def run(self):
        self.result.emit(WigleApiClient().get_transactions())


class _UploadWorker(QThread):
    file_done = pyqtSignal(str, bool, str)
    all_done = pyqtSignal()
    def __init__(self, file_paths: list[str]):
        super().__init__()
        self._files = file_paths
    def run(self):
        client = WigleApiClient()
        for fp in self._files:
            ok, msg = client.upload_file(fp)
            self.file_done.emit(fp, ok, msg)
        self.all_done.emit()


class _KmlParseWorker(QThread):
    """Legacy WiFi parser — deprecated.

    Replaced by `_WifiLoadWorker`, which queries `wifi/db` (populated by the
    new `wifi` subsystem's state-tracked incremental KML importer). Kept so
    any stale references don't blow up during the transition; safe to delete
    once nothing references it.
    """
    result = pyqtSignal(list)
    def run(self):
        self.result.emit([])


class _WifiLoadWorker(QThread):
    """Incrementally import any new WiGLE KMLs into `wifi_points.db`, then
    return one dedup'd point per unique BSSID for the dashboard map.
    Everything past the first run is near-instant — only new transids get
    re-parsed, then the aggregate query runs straight out of SQLite."""
    result = pyqtSignal(dict)

    def run(self):
        try:
            from wifi import reader as wreader, db as wdb
        except ImportError as e:
            self.result.emit({"points": [], "hint": f"WiFi module error: {e}"})
            return

        already = wdb.imported_transids()
        live_transids = set()
        if _KML_DIR.exists():
            live_transids = {p.stem for p in _KML_DIR.glob("*.kml")}
        new_transids = live_transids - already

        if new_transids:
            try:
                wreader.import_all(progress_cb=lambda _s: None)
            except Exception as e:
                log.exception("WiFi incremental import failed")
                # Continue to query whatever's already in the DB.

        try:
            networks = wdb.query_networks(limit=2_000_000)
        except Exception as e:
            log.exception("WiFi DB query failed")
            self.result.emit({"points": [], "hint": f"WiFi DB error: {e}"})
            return

        points = [
            {"lat": n["lat"], "lon": n["lon"], "name": n.get("ssid") or ""}
            for n in networks
            if n.get("lat") is not None and n.get("lon") is not None
        ]
        self.result.emit({"points": points})


class _KmlBatchWorker(QThread):
    # status: "ok" (new file written or already present), "empty" (server
    # returned 0 bytes — transaction has no content), "error" (HTTP error,
    # timeout, etc. — transient, worth retrying later).
    file_done = pyqtSignal(str, str)
    all_done = pyqtSignal(int)
    def __init__(self, transids: list[str]):
        super().__init__()
        self._transids = transids
        self._cancelled = False
    def cancel(self):
        self._cancelled = True
    def run(self):
        client = WigleApiClient()
        downloaded = 0
        for tid in self._transids:
            if self._cancelled:
                break
            out = _KML_DIR / f"{tid}.kml"
            if out.exists():
                self.file_done.emit(tid, "ok")
                downloaded += 1
                continue
            ok, data = client.download_kml(tid)
            if self._cancelled:
                break
            if ok and data:
                _KML_DIR.mkdir(parents=True, exist_ok=True)
                out.write_bytes(data)
                downloaded += 1
                status = "ok"
            elif ok:
                status = "empty"
            else:
                status = "error"
            self.file_done.emit(tid, status)
        self.all_done.emit(downloaded)


class _KmlMergeWorker(QThread):
    progress = pyqtSignal(str)
    finished_ok = pyqtSignal(dict)
    failed = pyqtSignal(str)

    def __init__(self, years: set[str] | None = None):
        super().__init__()
        self._years = years

    def run(self):
        try:
            results = _merge_kmls_by_year(
                _KML_DIR, Path.home() / 'Downloads',
                progress_cb=self.progress.emit,
                years=self._years,
            )
            self.finished_ok.emit(results)
        except Exception as e:
            log.exception("QGIS export failed")
            self.failed.emit(str(e))


class _GpkgMergeWorker(QThread):
    """Merges new WiGLE KMLs into the user's per-year GeoPackages via osgeo.ogr."""
    progress = pyqtSignal(str)
    finished_ok = pyqtSignal(list)  # list[qg.MergeResult]
    failed = pyqtSignal(str)

    def __init__(self, config: 'qg.QgisConfig', years: set[str] | None = None,
                 force_reimport: bool = False):
        super().__init__()
        self._config = config
        self._years = years
        self._force = force_reimport

    def run(self):
        try:
            results = qg.merge(
                self._config, _KML_DIR,
                years=self._years,
                progress_cb=self.progress.emit,
                force_reimport=self._force,
            )
            self.finished_ok.emit(results)
        except Exception as e:
            log.exception("GeoPackage merge failed")
            self.failed.emit(str(e))


class _KmlSelectedMergeWorker(QThread):
    """Merge a user-chosen subset of KMLs (by transid) into one file."""
    progress = pyqtSignal(str)
    finished_ok = pyqtSignal(Path, int, int)  # out_path, placemarks, files_used
    failed = pyqtSignal(str)

    def __init__(self, files: list[Path], out_path: Path, label: str):
        super().__init__()
        self._files = files
        self._out_path = out_path
        self._label = label

    def run(self):
        try:
            self.progress.emit(f"Merging {len(self._files)} file(s)...")
            pm = _merge_kml_files(self._files, self._out_path, self._label)
            self.finished_ok.emit(self._out_path, pm, len(self._files))
        except Exception as e:
            log.exception("Selected KML merge failed")
            self.failed.emit(str(e))


# ─── KML Parser ────────────────────────────────────────────────────

def _parse_kml_points(kml_path: Path) -> list[dict]:
    points = []
    try:
        tree = ET.parse(kml_path)
        root = tree.getroot()
        ns = ''
        if root.tag.startswith('{'):
            ns = root.tag.split('}')[0] + '}'
        for pm in root.iter(f'{ns}Placemark'):
            name_el = pm.find(f'{ns}name')
            name = name_el.text if name_el is not None else ''
            coord_el = pm.find(f'.//{ns}coordinates')
            if coord_el is not None and coord_el.text:
                parts = coord_el.text.strip().split(',')
                if len(parts) >= 2:
                    try:
                        lon, lat = float(parts[0]), float(parts[1])
                        if lat != 0 and lon != 0:
                            points.append({'lat': lat, 'lon': lon, 'name': name or ''})
                    except ValueError:
                        pass
    except Exception as e:
        log.warning("Failed to parse KML %s: %s", kml_path.name, e)
    return points


_KML_NS = 'http://www.opengis.net/kml/2.2'


def _year_from_transid(stem: str) -> str:
    if len(stem) >= 8 and stem[:8].isdigit():
        return stem[:4]
    return 'unknown'


def _scan_years(kml_dir: Path) -> dict[str, int]:
    """Return {year: file_count} for KMLs present in kml_dir."""
    counts: dict[str, int] = {}
    if not kml_dir.exists():
        return counts
    for kml in kml_dir.glob('*.kml'):
        y = _year_from_transid(kml.stem)
        counts[y] = counts.get(y, 0) + 1
    return counts


def _merge_kml_files(files: list[Path], out_path: Path, doc_name: str) -> int:
    """Merge Placemarks from the given KMLs into one KML at out_path.
    Returns the number of Placemarks written."""
    ET.register_namespace('', _KML_NS)
    kml_root = ET.Element(f'{{{_KML_NS}}}kml')
    doc = ET.SubElement(kml_root, f'{{{_KML_NS}}}Document')
    name = ET.SubElement(doc, f'{{{_KML_NS}}}name')
    name.text = doc_name

    pm_count = 0
    for src in files:
        try:
            src_root = ET.parse(src).getroot()
            for pm in src_root.iter(f'{{{_KML_NS}}}Placemark'):
                doc.append(pm)
                pm_count += 1
        except ET.ParseError:
            log.warning("Skipping unparseable KML: %s", src)
            continue

    out_path.parent.mkdir(parents=True, exist_ok=True)
    ET.ElementTree(kml_root).write(out_path, encoding='utf-8',
                                   xml_declaration=True)
    return pm_count


def _merge_kmls_by_year(kml_dir: Path, out_dir: Path,
                        progress_cb=None,
                        years: set[str] | None = None) -> dict[str, Path]:
    if not kml_dir.exists():
        return {}
    out_dir.mkdir(parents=True, exist_ok=True)

    buckets: dict[str, list[Path]] = {}
    for kml in sorted(kml_dir.glob('*.kml')):
        y = _year_from_transid(kml.stem)
        if years is not None and y not in years:
            continue
        buckets.setdefault(y, []).append(kml)

    written: dict[str, Path] = {}
    for year, files in sorted(buckets.items()):
        if progress_cb:
            progress_cb(f"Merging {year}: {len(files)} file(s)...")
        out_path = out_dir / f'{year}.kml'
        _merge_kml_files(files, out_path, f'WiGLE {year}')
        written[year] = out_path

    return written


# ─── Main View ──────────────────────────────────────────────────────

class _KmlScanWorker(QThread):
    """Incremental KML scan backed by the persistent manifest — only re-opens
    files whose size/mtime changed since the last run, so the Database page
    loads instantly after the first full scan.
    """
    progress = pyqtSignal(int, int)              # (done, total)
    file_done = pyqtSignal(str, dict)            # (transid, row_dict)
    finished_all = pyqtSignal(dict)              # aggregate summary

    def __init__(self, force: bool = False):
        super().__init__()
        self._force = force

    def run(self):
        try:
            from database import kml_manifest
        except ImportError as e:
            self.finished_all.emit({"error": str(e)})
            return

        def _file_cb(transid, entry):
            self.file_done.emit(transid, {
                "transid": transid,
                "path": str(_KML_DIR / f"{transid}.kml"),
                "size": entry.size,
                "wifi": entry.wifi,
                "cell": entry.cell,
                "bt": entry.bt,
                "error": entry.error,
            })

        try:
            _, agg = kml_manifest.scan(
                _KML_DIR,
                progress_cb=lambda d, t: self.progress.emit(d, t),
                file_cb=_file_cb,
                force=self._force,
            )
            self.finished_all.emit(agg)
        except Exception as e:
            log.exception("KML manifest scan failed")
            self.finished_all.emit({"error": str(e)})


def _fmt_date(d: str) -> str:
    """YYYYMMDD → YYYY-MM-DD."""
    if len(d) == 8 and d.isdigit():
        return f"{d[:4]}-{d[4:6]}-{d[6:]}"
    return d


def _fmt_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


class WigleView(QWidget):
    PAGE_DASHBOARD = 0
    PAGE_UPLOAD = 1
    PAGE_DOWNLOADS = 2
    PAGE_DATABASE = 3
    PAGE_QGIS = 4

    def __init__(self, parent=None):
        super().__init__(parent)
        self._workers: list[QThread] = []
        self._auto_refresh_timer = QTimer(self)
        self._auto_refresh_timer.timeout.connect(self._refresh_stats)
        self._map_ready = False
        self._map_initialized = False
        self._pending_points = None
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        self._stack = QStackedWidget()
        self._stack.addWidget(self._build_dashboard_page())
        self._stack.addWidget(self._build_upload_page())
        self._stack.addWidget(self._build_downloads_page())
        self._stack.addWidget(self._build_database_page())
        self._stack.addWidget(self._build_qgis_page())
        layout.addWidget(self._stack)

    def show_page(self, index: int):
        self._stack.setCurrentIndex(index)
        if index == self.PAGE_DATABASE:
            self._ensure_db_scanned()

    # ─── Dashboard Page ─────────────────────────────────────────────

    def _build_dashboard_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setSpacing(4)
        layout.setContentsMargins(8, 8, 8, 8)

        # ── Compact Stats Strip ──
        stats_frame = QFrame()
        stats_frame.setStyleSheet("QFrame { background-color: #2a2a2a; border: 1px solid #444; border-radius: 4px; }")
        stats_frame.setMaximumHeight(60)
        stats_layout = QHBoxLayout(stats_frame)
        stats_layout.setContentsMargins(12, 4, 12, 4)
        stats_layout.setSpacing(24)

        # Discovered
        self._discovered_label = QLabel("--")
        self._discovered_label.setFont(QFont('', 14, QFont.Weight.Bold))
        self._discovered_label.setStyleSheet(_GREEN)
        stats_layout.addWidget(QLabel("Discovered:"))
        stats_layout.addWidget(self._discovered_label)
        self._discovered_detail = QLabel("")
        self._discovered_detail.setStyleSheet(_DIM_STYLE)
        stats_layout.addWidget(self._discovered_detail)

        stats_layout.addWidget(self._vsep())

        # Monthly Rank
        self._monthly_rank_label = QLabel("--")
        self._monthly_rank_label.setFont(QFont('', 14, QFont.Weight.Bold))
        self._monthly_rank_label.setStyleSheet(_YELLOW)
        stats_layout.addWidget(QLabel("Monthly:"))
        stats_layout.addWidget(self._monthly_rank_label)
        self._monthly_detail = QLabel("")
        self._monthly_detail.setStyleSheet(_DIM_STYLE)
        stats_layout.addWidget(self._monthly_detail)

        stats_layout.addWidget(self._vsep())

        # Overall Rank
        self._overall_rank_label = QLabel("--")
        self._overall_rank_label.setFont(QFont('', 14, QFont.Weight.Bold))
        self._overall_rank_label.setStyleSheet(_YELLOW)
        stats_layout.addWidget(QLabel("Overall:"))
        stats_layout.addWidget(self._overall_rank_label)
        self._overall_detail = QLabel("")
        self._overall_detail.setStyleSheet(_DIM_STYLE)
        stats_layout.addWidget(self._overall_detail)

        stats_layout.addStretch()

        self._refresh_btn = _action_btn("Refresh", "#2980b9", "white", bold=True)
        self._refresh_btn.setMaximumHeight(28)
        self._refresh_btn.clicked.connect(self._refresh_stats)
        stats_layout.addWidget(self._refresh_btn)

        layout.addWidget(stats_frame)

        # ── Map (fills remaining space) ──
        if HAS_WEBENGINE:
            try:
                self._map_view = QWebEngineView()
                self._map_view.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
                layout.addWidget(self._map_view, 1)
            except Exception:
                self._map_view = None
                layout.addWidget(QLabel("Map unavailable — WebEngine init failed"), 1)
        else:
            self._map_view = None
            layout.addWidget(QLabel("Map unavailable — install PyQt6-WebEngine"), 1)

        # Reload button row
        map_btn_row = QHBoxLayout()
        map_btn_row.addStretch()
        self._reload_map_btn = _action_btn("Reload KML Data")
        self._reload_map_btn.clicked.connect(self._load_kml_to_map)
        map_btn_row.addWidget(self._reload_map_btn)
        layout.addLayout(map_btn_row)

        return page

    @staticmethod
    def _vsep() -> QFrame:
        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.VLine)
        sep.setStyleSheet("color: #555;")
        sep.setMaximumHeight(30)
        return sep

    # ─── Upload Page ─────────────────────────────────────────────────

    def _build_upload_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setSpacing(12)
        layout.setContentsMargins(16, 16, 16, 16)

        title = QLabel("Upload to WiGLE")
        title.setFont(QFont('', 16, QFont.Weight.Bold))
        title.setStyleSheet(_LABEL_STYLE)
        layout.addWidget(title)

        # ── File selection ──
        file_group = QGroupBox("Staged Files")
        file_group.setStyleSheet(_GROUP_STYLE)
        file_layout = QVBoxLayout(file_group)

        self._upload_tree = QTreeWidget()
        self._upload_tree.setHeaderLabels(["File", "Size", "Status"])
        self._upload_tree.setAlternatingRowColors(True)
        self._upload_tree.setStyleSheet(_TREE_STYLE)
        self._upload_tree.setRootIsDecorated(False)
        self._upload_tree.setMinimumHeight(120)
        self._upload_tree.setMaximumHeight(180)
        header = self._upload_tree.header()
        header.setStretchLastSection(False)
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        file_layout.addWidget(self._upload_tree)

        self._upload_progress = QProgressBar()
        self._upload_progress.setVisible(False)
        self._upload_progress.setStyleSheet("""
            QProgressBar { background-color: #333; border: 1px solid #555;
                           border-radius: 4px; text-align: center; color: #e0e0e0; }
            QProgressBar::chunk { background-color: #2980b9; border-radius: 3px; }
        """)
        file_layout.addWidget(self._upload_progress)

        upload_btn_row = QHBoxLayout()
        self._add_files_btn = _action_btn("Add Files...")
        self._add_files_btn.clicked.connect(self._add_upload_files)
        upload_btn_row.addWidget(self._add_files_btn)
        self._scan_local_btn = _action_btn("Scan Staged Files")
        self._scan_local_btn.clicked.connect(self._scan_local_wiglecsv)
        upload_btn_row.addWidget(self._scan_local_btn)
        self._filter_btn = _action_btn("Filter Staged", "#8e44ad", "white")
        self._filter_btn.setToolTip("Strip blocked MACs from staged CSVs before upload")
        self._filter_btn.clicked.connect(self._filter_staged_files)
        upload_btn_row.addWidget(self._filter_btn)
        upload_btn_row.addStretch()
        self._upload_btn = _action_btn("Upload Selected", "#27ae60", "white", bold=True)
        self._upload_btn.clicked.connect(self._upload_selected)
        upload_btn_row.addWidget(self._upload_btn)
        file_layout.addLayout(upload_btn_row)

        layout.addWidget(file_group)

        # ── Transaction History (full columns) ──
        tx_group = QGroupBox("Transaction History")
        tx_group.setStyleSheet(_GROUP_STYLE)
        tx_layout = QVBoxLayout(tx_group)

        # Queue status banner
        self._queue_status = QLabel("")
        self._queue_status.setStyleSheet("color: #f39c12; font-weight: bold; border: none; background: transparent;")
        self._queue_status.setVisible(False)
        tx_layout.addWidget(self._queue_status)

        self._tx_tree = _make_tx_tree()
        tx_layout.addWidget(self._tx_tree)

        tx_btn_row = QHBoxLayout()
        self._refresh_tx_btn = _action_btn("Refresh Transactions", "#2980b9", "white", bold=True)
        self._refresh_tx_btn.clicked.connect(self._refresh_transactions)
        tx_btn_row.addWidget(self._refresh_tx_btn)
        tx_btn_row.addStretch()
        tx_layout.addLayout(tx_btn_row)

        layout.addWidget(tx_group, 1)
        return page

    # ─── Downloads Page ──────────────────────────────────────────────

    def _build_downloads_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setSpacing(12)
        layout.setContentsMargins(16, 16, 16, 16)
        page.setStyleSheet(_INPUT_STYLE)

        title = QLabel("KML Downloads")
        title.setFont(QFont('', 16, QFont.Weight.Bold))
        title.setStyleSheet(_LABEL_STYLE)
        layout.addWidget(title)

        # Controls
        ctrl_group = QGroupBox("Download Options")
        ctrl_group.setStyleSheet(_GROUP_STYLE)
        ctrl_layout = QVBoxLayout(ctrl_group)

        date_row = QHBoxLayout()
        date_row.addWidget(QLabel("Start Date:"))
        self._dl_start = QDateEdit()
        self._dl_start.setCalendarPopup(True)
        self._dl_start.setDate(QDate.currentDate().addMonths(-1))
        self._dl_start.setDisplayFormat("yyyy-MM-dd")
        date_row.addWidget(self._dl_start)
        date_row.addWidget(QLabel("End Date:"))
        self._dl_end = QDateEdit()
        self._dl_end.setCalendarPopup(True)
        self._dl_end.setDate(QDate.currentDate())
        self._dl_end.setDisplayFormat("yyyy-MM-dd")
        date_row.addWidget(self._dl_end)
        date_row.addStretch()
        ctrl_layout.addLayout(date_row)

        opt_row = QHBoxLayout()
        self._dl_only_new = QCheckBox("Only download files I don't have locally")
        self._dl_only_new.setChecked(True)
        opt_row.addWidget(self._dl_only_new)
        self._dl_ignore_failed = QCheckBox("Ignore failed transactions")
        self._dl_ignore_failed.setChecked(True)
        opt_row.addWidget(self._dl_ignore_failed)
        opt_row.addStretch()
        ignored_count = len(_load_ignored_transids())
        self._dl_clear_ignored_btn = _action_btn(
            f"Clear Ignore List ({ignored_count})" if ignored_count else "Clear Ignore List")
        self._dl_clear_ignored_btn.clicked.connect(self._clear_ignored)
        self._dl_clear_ignored_btn.setEnabled(ignored_count > 0)
        opt_row.addWidget(self._dl_clear_ignored_btn)
        ctrl_layout.addLayout(opt_row)

        btn_row = QHBoxLayout()
        self._dl_find_btn = _action_btn("Find Transactions", "#2980b9", "white", bold=True)
        self._dl_find_btn.clicked.connect(self._find_downloadable)
        btn_row.addWidget(self._dl_find_btn)
        btn_row.addStretch()
        self._dl_all_btn = _action_btn("Download All", "#27ae60", "white", bold=True)
        self._dl_all_btn.clicked.connect(self._download_all)
        self._dl_all_btn.setEnabled(False)
        btn_row.addWidget(self._dl_all_btn)
        self._dl_cancel_btn = _action_btn("Cancel", "#c0392b", "white", bold=True)
        self._dl_cancel_btn.clicked.connect(self._cancel_download)
        self._dl_cancel_btn.setVisible(False)
        btn_row.addWidget(self._dl_cancel_btn)
        ctrl_layout.addLayout(btn_row)

        layout.addWidget(ctrl_group)

        # Transaction list with full columns
        dl_group = QGroupBox("Transactions")
        dl_group.setStyleSheet(_GROUP_STYLE)
        dl_layout = QVBoxLayout(dl_group)

        self._dl_tree = _make_tx_tree()
        self._dl_tree.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self._dl_tree.customContextMenuRequested.connect(self._on_dl_tree_menu)
        dl_layout.addWidget(self._dl_tree)

        self._dl_progress = QProgressBar()
        self._dl_progress.setVisible(False)
        self._dl_progress.setStyleSheet("""
            QProgressBar { background-color: #333; border: 1px solid #555;
                           border-radius: 4px; text-align: center; color: #e0e0e0; }
            QProgressBar::chunk { background-color: #8e44ad; border-radius: 3px; }
        """)
        dl_layout.addWidget(self._dl_progress)

        self._dl_status = QLabel("")
        self._dl_status.setStyleSheet(_DIM_STYLE)
        dl_layout.addWidget(self._dl_status)

        layout.addWidget(dl_group, 1)
        return page

    # ─── Database Page ───────────────────────────────────────────────

    def _build_database_page(self) -> QWidget:
        page = QWidget()
        page.setStyleSheet(_INPUT_STYLE)
        layout = QVBoxLayout(page)
        layout.setSpacing(10)
        layout.setContentsMargins(16, 16, 16, 16)

        title = QLabel("KML Database")
        title.setFont(QFont('', 16, QFont.Weight.Bold))
        title.setStyleSheet(_LABEL_STYLE)
        layout.addWidget(title)

        # Aggregate banner
        banner = QFrame()
        banner.setStyleSheet("QFrame { background-color: #2a2a2a; border: 1px solid #444; border-radius: 6px; }")
        b = QHBoxLayout(banner)
        b.setContentsMargins(14, 10, 14, 10)
        b.setSpacing(22)

        def stat_col(label_text, color_style=_GREEN):
            col = QVBoxLayout()
            col.setSpacing(2)
            v = QLabel("--")
            v.setFont(QFont('', 14, QFont.Weight.Bold))
            v.setStyleSheet(color_style)
            k = QLabel(label_text)
            k.setStyleSheet(_DIM_STYLE)
            col.addWidget(v)
            col.addWidget(k)
            return col, v

        files_col, self._db_files = stat_col("Files", _LABEL_STYLE)
        size_col, self._db_size = stat_col("On disk", _LABEL_STYLE)
        wifi_col, self._db_wifi = stat_col("WiFi observations", _GREEN)
        cell_col, self._db_cell = stat_col("Cell observations", _YELLOW)
        bt_col, self._db_bt = stat_col("Bluetooth observations", "color: #58A6FF; border: none; background: transparent;")
        date_col, self._db_dates = stat_col("Date range", _DIM_STYLE)

        for c in (files_col, size_col, wifi_col, cell_col, bt_col, date_col):
            b.addLayout(c)
        b.addStretch(1)

        self._db_refresh_btn = _action_btn("Refresh", "#2980b9", "white", bold=True)
        self._db_refresh_btn.clicked.connect(self._rescan_kmls)
        b.addWidget(self._db_refresh_btn)

        layout.addWidget(banner)

        # Progress + status
        self._db_progress = QProgressBar()
        self._db_progress.setVisible(False)
        self._db_progress.setStyleSheet("""
            QProgressBar { background-color: #333; border: 1px solid #555;
                           border-radius: 4px; text-align: center; color: #e0e0e0; }
            QProgressBar::chunk { background-color: #2980b9; border-radius: 3px; }
        """)
        layout.addWidget(self._db_progress)

        self._db_status = QLabel("")
        self._db_status.setStyleSheet(_DIM_STYLE)
        layout.addWidget(self._db_status)

        # File table
        self._db_tree = QTreeWidget()
        self._db_tree.setStyleSheet(_TREE_STYLE)
        self._db_tree.setAlternatingRowColors(True)
        self._db_tree.setHeaderLabels([
            "Transid", "Date", "Size", "WiFi", "Cell", "BT",
        ])
        self._db_tree.setSortingEnabled(True)
        self._db_tree.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self._db_tree.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self._db_tree.customContextMenuRequested.connect(self._on_db_tree_menu)
        hdr = self._db_tree.header()
        hdr.setSectionResizeMode(0, QHeaderView.ResizeMode.Interactive)
        hdr.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        hdr.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        hdr.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        hdr.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
        hdr.setSectionResizeMode(5, QHeaderView.ResizeMode.ResizeToContents)
        self._db_tree.setColumnWidth(0, 260)
        layout.addWidget(self._db_tree, 1)

        self._db_scanned = False
        return page

    def _ensure_db_scanned(self):
        """Kick off a scan on first entry to the Database page."""
        if self._db_scanned:
            return
        self._rescan_kmls()

    def _rescan_kmls(self):
        if hasattr(self, '_db_scan_worker') and self._db_scan_worker and self._db_scan_worker.isRunning():
            return
        self._db_scanned = True
        self._db_tree.setSortingEnabled(False)
        self._db_tree.clear()
        self._db_progress.setVisible(True)
        self._db_progress.setValue(0)
        self._db_status.setText("Scanning KML files…")

        self._db_scan_worker = _KmlScanWorker()
        self._db_scan_worker.progress.connect(self._on_db_scan_progress)
        self._db_scan_worker.file_done.connect(self._on_db_file_done)
        self._db_scan_worker.finished_all.connect(self._on_db_scan_done)
        self._db_scan_worker.start()

    def _on_db_scan_progress(self, done: int, total: int):
        self._db_progress.setMaximum(total)
        self._db_progress.setValue(done)
        self._db_status.setText(f"Scanning KMLs… {done:,} / {total:,}")

    def _on_db_file_done(self, transid: str, row: dict):
        item = QTreeWidgetItem([
            transid,
            _fmt_date(transid[:8]),
            _fmt_size(row.get("size", 0)),
            f"{row.get('wifi', 0):,}",
            f"{row.get('cell', 0):,}",
            f"{row.get('bt', 0):,}",
        ])
        item.setData(0, Qt.ItemDataRole.UserRole, row.get("path", ""))
        # Right-justify numeric columns.
        for col in (2, 3, 4, 5):
            item.setTextAlignment(col, Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        if row.get("error"):
            item.setForeground(0, QColor('#e74c3c'))
            item.setToolTip(0, row["error"])
        self._db_tree.addTopLevelItem(item)

    def _on_db_scan_done(self, agg: dict):
        self._db_progress.setVisible(False)
        if agg.get("error"):
            self._db_status.setText(f"Scan failed: {agg['error']}")
            return
        self._db_tree.setSortingEnabled(True)
        self._db_tree.sortItems(0, Qt.SortOrder.DescendingOrder)

        self._db_files.setText(f"{agg.get('files', 0):,}")
        self._db_size.setText(_fmt_size(agg.get('size_bytes', 0)))
        self._db_wifi.setText(f"{agg.get('wifi', 0):,}")
        self._db_cell.setText(f"{agg.get('cell', 0):,}")
        self._db_bt.setText(f"{agg.get('bt', 0):,}")

        earliest = agg.get("earliest")
        latest = agg.get("latest")
        if earliest and latest:
            self._db_dates.setText(f"{_fmt_date(earliest)} → {_fmt_date(latest)}")
        else:
            self._db_dates.setText("—")

        self._db_status.setText(
            f"Scanned {agg.get('files', 0):,} KML file(s) in {_KML_DIR}"
        )

    def _on_db_tree_menu(self, pos):
        selected = self._db_tree.selectedItems()
        clicked = self._db_tree.itemAt(pos)
        if clicked and clicked not in selected:
            scope = [clicked]
        else:
            scope = selected or ([clicked] if clicked else [])
        if not scope:
            return

        menu = QMenu(self)
        multi = len(scope) > 1

        if not multi:
            act_open = QAction("Open containing folder", menu)
            act_open.triggered.connect(lambda: self._open_containing_folder(scope[0]))
            menu.addAction(act_open)

        act_qgis = QAction(
            f"Re-import to QGIS ({len(scope)})" if multi else "Re-import to QGIS",
            menu,
        )
        act_qgis.setToolTip(
            "Jump to the QGIS page with 'force re-import' enabled for the selected transids"
        )
        act_qgis.triggered.connect(lambda: self._reimport_to_qgis(scope))
        menu.addAction(act_qgis)

        act_cell = QAction(
            f"Re-import cells ({len(scope)})" if multi else "Re-import cells",
            menu,
        )
        act_cell.setToolTip(
            "Force-reimport the selected transids into the Cell subsystem"
        )
        act_cell.triggered.connect(lambda: self._reimport_to_cells(scope))
        menu.addAction(act_cell)

        act_wifi = QAction(
            f"Re-import WiFi ({len(scope)})" if multi else "Re-import WiFi",
            menu,
        )
        act_wifi.setToolTip(
            "Force-reimport the selected transids into the WiFi subsystem"
        )
        act_wifi.triggered.connect(lambda: self._reimport_to_wifi(scope))
        menu.addAction(act_wifi)

        menu.addSeparator()
        act_delete = QAction(
            f"Delete file(s) ({len(scope)})" if multi else "Delete file",
            menu,
        )
        act_delete.triggered.connect(lambda: self._delete_selected_kmls(scope))
        menu.addAction(act_delete)

        menu.exec(self._db_tree.viewport().mapToGlobal(pos))

    def _open_containing_folder(self, item):
        from PyQt6.QtGui import QDesktopServices
        from PyQt6.QtCore import QUrl
        path = item.data(0, Qt.ItemDataRole.UserRole)
        if path:
            QDesktopServices.openUrl(QUrl.fromLocalFile(str(Path(path).parent)))

    def _delete_selected_kmls(self, scope):
        if not scope:
            return
        msg = (f"Permanently delete {len(scope)} KML file(s) from "
               f"{_KML_DIR}?\n\nThey can be re-downloaded from WiGLE later.")
        resp = QMessageBox.question(
            self, "Delete KML files", msg,
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if resp != QMessageBox.StandardButton.Yes:
            return
        removed = 0
        for item in scope:
            path = item.data(0, Qt.ItemDataRole.UserRole)
            if path and Path(path).exists():
                try:
                    Path(path).unlink()
                    removed += 1
                except OSError as e:
                    log.warning("Couldn't delete %s: %s", path, e)
        self._db_status.setText(f"Deleted {removed} file(s)")
        self._rescan_kmls()

    def _reimport_to_qgis(self, scope):
        # Stash the selection somewhere persistent enough that the QGIS page
        # could pick it up — for v1 we just hand the transids to the Cell
        # subsystem's known API path and rely on the user clicking Export.
        transids = [item.text(0) for item in scope]
        QMessageBox.information(
            self, "Re-import to QGIS",
            f"Selected {len(transids)} transid(s). Head to the QGIS page and "
            f"check 'Force re-import of KMLs already recorded' before clicking "
            f"Export to QGIS — those files will be processed again."
        )

    def _reimport_to_cells(self, scope):
        # Direct cell-side re-import of just these transids.
        try:
            from cell import reader as cell_reader, db as cell_db
        except Exception as e:
            QMessageBox.warning(self, "Cell subsystem unavailable", str(e))
            return
        transids = [item.text(0) for item in scope]
        with cell_db.connect() as conn:
            placeholders = ",".join("?" for _ in transids)
            conn.execute(
                f"DELETE FROM imported_transids WHERE transid IN ({placeholders})",
                transids,
            )
            conn.execute(
                f"DELETE FROM cells WHERE source_transid IN ({placeholders})",
                transids,
            )
            conn.commit()
        rep = cell_reader.import_all(progress_cb=lambda s: None)
        QMessageBox.information(
            self, "Cell re-import complete",
            f"Forced re-ingest of {len(transids)} transid(s).\n"
            f"Net result: {rep.cells_inserted:,} cell observations re-added."
        )

    def _reimport_to_wifi(self, scope):
        try:
            from wifi import reader as wifi_reader, db as wifi_db
        except Exception as e:
            QMessageBox.warning(self, "WiFi subsystem unavailable", str(e))
            return
        transids = [item.text(0) for item in scope]
        with wifi_db.connect() as conn:
            placeholders = ",".join("?" for _ in transids)
            conn.execute(
                f"DELETE FROM wifi_imported_transids WHERE transid IN ({placeholders})",
                transids,
            )
            conn.execute(
                f"DELETE FROM wifi_observations WHERE source_transid IN ({placeholders})",
                transids,
            )
            conn.commit()
        rep = wifi_reader.import_all(progress_cb=lambda s: None)
        QMessageBox.information(
            self, "WiFi re-import complete",
            f"Forced re-ingest of {len(transids)} transid(s).\n"
            f"Net result: {rep.observations_inserted:,} WiFi observations re-added."
        )

    # ─── QGIS Page ───────────────────────────────────────────────────

    def _build_qgis_page(self) -> QWidget:
        self._qgis_config = qg.QgisConfig.load()

        page = QWidget()
        page.setStyleSheet(_INPUT_STYLE)
        layout = QVBoxLayout(page)
        layout.setSpacing(12)
        layout.setContentsMargins(16, 16, 16, 16)

        title = QLabel("QGIS Integration")
        title.setFont(QFont('', 16, QFont.Weight.Bold))
        title.setStyleSheet(_LABEL_STYLE)
        layout.addWidget(title)

        sub = QLabel(
            "Merge new WiGLE KMLs into your per-year GeoPackages so QGIS picks "
            "them up on next open without losing your existing styles."
        )
        sub.setStyleSheet(_DIM_STYLE)
        sub.setWordWrap(True)
        layout.addWidget(sub)

        # ── Config card ──
        cfg_group = QGroupBox("Paths")
        cfg_group.setStyleSheet(_GROUP_STYLE)
        cfg_layout = QVBoxLayout(cfg_group)

        folder_row = QHBoxLayout()
        folder_row.addWidget(QLabel("GeoPackage folder:"))
        self._qgis_folder_edit = QLineEdit(self._qgis_config.folder)
        self._qgis_folder_edit.setPlaceholderText("~/QGIS/Wardriving")
        self._qgis_folder_edit.editingFinished.connect(self._save_qgis_config)
        folder_row.addWidget(self._qgis_folder_edit, 1)
        browse_btn = _action_btn("Browse…")
        browse_btn.clicked.connect(self._pick_qgis_folder)
        folder_row.addWidget(browse_btn)
        cfg_layout.addLayout(folder_row)

        gpkg_row = QHBoxLayout()
        gpkg_row.addWidget(QLabel("GPKG filename pattern:"))
        self._qgis_gpkg_edit = QLineEdit(self._qgis_config.gpkg_pattern)
        self._qgis_gpkg_edit.setPlaceholderText("{year} WIFI.gpkg")
        self._qgis_gpkg_edit.editingFinished.connect(self._save_qgis_config)
        gpkg_row.addWidget(self._qgis_gpkg_edit, 1)
        cfg_layout.addLayout(gpkg_row)

        layer_row = QHBoxLayout()
        layer_row.addWidget(QLabel("Layer name pattern:"))
        self._qgis_layer_edit = QLineEdit(self._qgis_config.layer_pattern)
        self._qgis_layer_edit.setPlaceholderText("{year} WIFI")
        self._qgis_layer_edit.editingFinished.connect(self._save_qgis_config)
        layer_row.addWidget(self._qgis_layer_edit, 1)
        cfg_layout.addLayout(layer_row)

        tokens_hint = QLabel(
            "Use <code>{year}</code> in either pattern — it's pulled from the "
            "KML filename's YYYYMMDD prefix."
        )
        tokens_hint.setStyleSheet(_DIM_STYLE)
        tokens_hint.setWordWrap(True)
        cfg_layout.addWidget(tokens_hint)

        self._qgis_create_missing = QCheckBox(
            "Create the GeoPackage for a year if it doesn't exist yet "
            "(will use the KML schema — style it once in QGIS)"
        )
        self._qgis_create_missing.setChecked(self._qgis_config.create_missing_year)
        self._qgis_create_missing.toggled.connect(self._save_qgis_config)
        cfg_layout.addWidget(self._qgis_create_missing)

        self._qgis_force_reimport = QCheckBox(
            "Force re-import of KMLs already recorded (disabled by default — "
            "normal runs skip previously-imported transids)"
        )
        self._qgis_force_reimport.setChecked(False)
        cfg_layout.addWidget(self._qgis_force_reimport)

        layout.addWidget(cfg_group)

        # ── Action row ──
        act_row = QHBoxLayout()
        self._qgis_detect_btn = _action_btn("Detect Layers", "#2980b9", "white")
        self._qgis_detect_btn.setToolTip(
            "Inspect the folder and list what GeoPackages and layers were found")
        self._qgis_detect_btn.clicked.connect(self._qgis_detect)
        act_row.addWidget(self._qgis_detect_btn)

        self._qgis_validate_btn = _action_btn("Dry Run", "#2980b9", "white")
        self._qgis_validate_btn.setToolTip(
            "Preview what would be imported without writing anything")
        self._qgis_validate_btn.clicked.connect(self._qgis_validate)
        act_row.addWidget(self._qgis_validate_btn)

        act_row.addStretch()

        self._qgis_kml_btn = _action_btn(
            "Per-year KML Export → ~/Downloads", "#8e44ad", "white")
        self._qgis_kml_btn.setToolTip(
            "Ad-hoc per-year KML export to ~/Downloads (the old behavior)")
        self._qgis_kml_btn.clicked.connect(self._export_for_qgis)
        act_row.addWidget(self._qgis_kml_btn)

        self._qgis_export_btn = _action_btn(
            "Export to QGIS", "#27ae60", "white", bold=True)
        self._qgis_export_btn.setToolTip(
            "Merge new KMLs into your per-year GeoPackages")
        self._qgis_export_btn.clicked.connect(self._export_to_qgis_gpkg)
        act_row.addWidget(self._qgis_export_btn)

        layout.addLayout(act_row)

        # ── Status + log ──
        self._qgis_status = QLabel("")
        self._qgis_status.setStyleSheet(_DIM_STYLE)
        self._qgis_status.setWordWrap(True)
        layout.addWidget(self._qgis_status)

        self._qgis_log = QPlainTextEdit()
        self._qgis_log.setReadOnly(True)
        self._qgis_log.setStyleSheet(
            "QPlainTextEdit { background-color: #2b2b2b; color: #e0e0e0; "
            "border: 1px solid #444; border-radius: 4px; "
            "font-family: monospace; font-size: 11px; }"
        )
        self._qgis_log.setPlaceholderText(
            "Detect / Dry Run / Export results land here…")
        layout.addWidget(self._qgis_log, 1)

        if not qg._HAS_GDAL:
            warn = QLabel(
                "⚠ python-gdal is not installed. Install it with "
                "<code>sudo pacman -S python-gdal</code> (or your distro's "
                "equivalent) to enable GeoPackage merge. Per-year KML export "
                "above still works without it."
            )
            warn.setStyleSheet(_YELLOW)
            warn.setWordWrap(True)
            layout.addWidget(warn)

        return page

    # ─── QGIS Page — handlers ────────────────────────────────────────

    def _save_qgis_config(self):
        self._qgis_config.folder = self._qgis_folder_edit.text().strip()
        self._qgis_config.gpkg_pattern = self._qgis_gpkg_edit.text().strip() or '{year} WIFI.gpkg'
        self._qgis_config.layer_pattern = self._qgis_layer_edit.text().strip() or '{year} WIFI'
        self._qgis_config.create_missing_year = self._qgis_create_missing.isChecked()
        self._qgis_config.save()

    def _pick_qgis_folder(self):
        start = self._qgis_folder_edit.text().strip() or str(Path.home())
        picked = QFileDialog.getExistingDirectory(
            self, "Pick the folder that holds your per-year WiGLE GeoPackages",
            start,
        )
        if picked:
            self._qgis_folder_edit.setText(picked)
            self._save_qgis_config()

    def _qgis_detect(self):
        self._save_qgis_config()
        if not qg._HAS_GDAL:
            self._qgis_log.appendPlainText(
                "python-gdal not available — can't inspect GeoPackages.")
            return
        if not self._qgis_config.folder:
            self._qgis_log.appendPlainText(
                "Set the GeoPackage folder first.")
            return
        try:
            reports = qg.detect(self._qgis_config, _KML_DIR)
        except Exception as e:
            self._qgis_log.appendPlainText(f"Detect failed: {e}")
            return
        if not reports:
            self._qgis_log.appendPlainText(
                f"No KMLs in {_KML_DIR}. Download some from the Downloads page.")
            return
        self._qgis_log.appendPlainText("── Detect ────────────────────────────────")
        for r in reports:
            self._qgis_log.appendPlainText(
                f"{r.year}: {len(r.kml_files)} KML(s), "
                f"{len(r.new_transids)} new"
            )
            if r.gpkg_exists:
                layer_msg = (
                    f"  ✓ {r.gpkg_path.name} — layer "
                    f"{'found' if r.layer_found else 'MISSING'}: "
                    f"'{r.target_layer}'  "
                    f"({r.feature_count:,} existing features, "
                    f"{len(r.already_imported_transids)} transids on record)"
                )
                self._qgis_log.appendPlainText(layer_msg)
                if not r.layer_found and r.layers_in_gpkg:
                    self._qgis_log.appendPlainText(
                        f"    layers in file: {', '.join(r.layers_in_gpkg)}")
            else:
                self._qgis_log.appendPlainText(
                    f"  ✗ {r.gpkg_path.name} — missing"
                    f"{' (would be created)' if self._qgis_config.create_missing_year else ''}")
        self._qgis_status.setText(f"Detected {len(reports)} year(s).")

    def _qgis_validate(self):
        """Dry-run: enumerate what would be written, without writing anything."""
        self._save_qgis_config()
        if not qg._HAS_GDAL:
            self._qgis_log.appendPlainText(
                "python-gdal not available — can't dry-run.")
            return
        if not self._qgis_config.folder:
            self._qgis_log.appendPlainText(
                "Set the GeoPackage folder first.")
            return
        try:
            reports = qg.detect(self._qgis_config, _KML_DIR)
        except Exception as e:
            self._qgis_log.appendPlainText(f"Dry-run failed: {e}")
            return
        self._qgis_log.appendPlainText("── Dry Run ──────────────────────────────")
        any_work = False
        for r in reports:
            new_count = len(r.new_transids)
            if not new_count:
                self._qgis_log.appendPlainText(
                    f"{r.year}: up to date ({len(r.kml_files)} KML(s) already on record)")
                continue
            any_work = True
            if r.gpkg_exists and r.layer_found:
                self._qgis_log.appendPlainText(
                    f"{r.year}: would append {new_count} KML(s) to "
                    f"{r.gpkg_path.name}:{r.target_layer}")
            elif r.gpkg_exists and not r.layer_found:
                self._qgis_log.appendPlainText(
                    f"{r.year}: SKIP — '{r.target_layer}' not in {r.gpkg_path.name} "
                    f"(layers: {', '.join(r.layers_in_gpkg)})")
            elif self._qgis_config.create_missing_year:
                self._qgis_log.appendPlainText(
                    f"{r.year}: would CREATE {r.gpkg_path.name} and append {new_count} KML(s)")
            else:
                self._qgis_log.appendPlainText(
                    f"{r.year}: SKIP — {r.gpkg_path.name} doesn't exist "
                    f"(enable 'Create the GeoPackage…' to let AirParse make one)")
        if not any_work:
            self._qgis_status.setText("Nothing to import — everything is up to date.")
        else:
            self._qgis_status.setText("Dry run complete — see log.")

    def _export_to_qgis_gpkg(self):
        self._save_qgis_config()
        if not qg._HAS_GDAL:
            QMessageBox.warning(
                self, "GDAL missing",
                "python-gdal isn't installed. Install it and restart AirParse.")
            return
        if not self._qgis_config.folder:
            QMessageBox.information(
                self, "Export to QGIS",
                "Set the GeoPackage folder first (top of this page).")
            return
        if hasattr(self, '_gpkg_worker') and self._gpkg_worker \
                and self._gpkg_worker.isRunning():
            self._qgis_status.setText("An export is already running.")
            return

        self._qgis_export_btn.setEnabled(False)
        self._qgis_status.setText("Exporting to GeoPackage…")
        self._qgis_log.appendPlainText("── Export ───────────────────────────────")
        self._gpkg_worker = _GpkgMergeWorker(
            self._qgis_config,
            years=None,
            force_reimport=self._qgis_force_reimport.isChecked(),
        )
        self._gpkg_worker.progress.connect(self._qgis_status.setText)
        self._gpkg_worker.progress.connect(self._qgis_log.appendPlainText)
        self._gpkg_worker.finished_ok.connect(self._on_gpkg_done)
        self._gpkg_worker.failed.connect(self._on_gpkg_failed)
        self._gpkg_worker.start()

    def _on_gpkg_done(self, results: list):
        self._qgis_export_btn.setEnabled(True)
        total_added = sum(r.features_added for r in results)
        total_files = sum(r.files_imported for r in results)
        self._qgis_log.appendPlainText("── Summary ──────────────────────────────")
        for r in results:
            self._qgis_log.appendPlainText(
                f"{r.year} [{r.action}]: {r.message or '-'}")
        if total_added == 0:
            self._qgis_status.setText(
                "Export complete — everything was already up to date.")
        else:
            self._qgis_status.setText(
                f"Export complete — added {total_added:,} feature(s) from "
                f"{total_files} KML(s)."
            )
        log.info("GeoPackage export complete: %s",
                 [(r.year, r.action, r.features_added) for r in results])

    def _on_gpkg_failed(self, msg: str):
        self._qgis_export_btn.setEnabled(True)
        self._qgis_status.setText(f"Export failed: {msg}")
        self._qgis_log.appendPlainText(f"Export failed: {msg}")
        QMessageBox.warning(self, "Export to QGIS failed", msg)

    # ─── Lifecycle ───────────────────────────────────────────────────

    def showEvent(self, event):
        super().showEvent(event)
        self._scan_local_wiglecsv()
        if not WigleApiClient.has_credentials():
            self._discovered_label.setText("--")
            self._discovered_detail.setText("Configure API key in Settings")
            return
        self._refresh_stats()
        if not self._auto_refresh_timer.isActive():
            self._auto_refresh_timer.start(60000)
        if self._map_view and not self._map_initialized:
            self._init_map()

    def hideEvent(self, event):
        super().hideEvent(event)
        self._auto_refresh_timer.stop()

    # ─── Map ─────────────────────────────────────────────────────────

    def _init_map(self):
        if not self._map_view:
            return
        if not self._map_initialized:
            self._map_view.loadFinished.connect(self._on_map_loaded)
            self._map_initialized = True
        self._map_view.setHtml(self._generate_map_html())

    def _on_map_loaded(self, ok: bool):
        if not ok:
            self._map_ready = False
            return
        self._map_ready = True
        if self._pending_points is not None:
            self._send_points_to_map(self._pending_points)
            self._pending_points = None
        else:
            self._load_kml_to_map()

    def _load_kml_to_map(self):
        """Load WiFi networks from wifi_points.db into the dashboard map.
        Incremental import: if there are new KMLs that haven't been ingested
        yet, they get added first, then the map re-queries the DB."""
        self._reload_map_btn.setEnabled(False)
        self._reload_map_btn.setText("Loading WiFi…")
        worker = _WifiLoadWorker()
        worker.result.connect(self._on_wifi_loaded)
        worker.result.connect(lambda _: self._workers.remove(worker))
        self._workers.append(worker)
        worker.start()

    def _on_wifi_loaded(self, payload: dict):
        self._reload_map_btn.setEnabled(True)
        points = payload.get("points") or []
        count = len(points)
        if count:
            if self._map_ready:
                self._send_points_to_map(points)
            else:
                self._pending_points = points
            self._reload_map_btn.setText(
                f"Reload WiFi Networks ({count:,} networks)"
            )
        else:
            msg = payload.get("hint") or "No WiFi networks yet"
            self._reload_map_btn.setText(msg)
            QTimer.singleShot(
                4000, lambda: self._reload_map_btn.setText("Reload WiFi Networks")
            )

    # Back-compat alias — the original parser callback name is referenced
    # elsewhere in the file (showEvent path). Route it to the new handler.
    def _on_kml_parsed(self, all_points: list):
        self._on_wifi_loaded({"points": all_points})

    def _send_points_to_map(self, points: list[dict]):
        """Stream points to the page in 25k-row chunks. Sending the full set
        in a single `runJavaScript` call silently drops past ~10 MB on
        QtWebEngine, which made the 1M-network case render an empty map."""
        if not self._map_view:
            return
        page = self._map_view.page()
        page.runJavaScript("clearPoints();")
        chunk = 25_000
        for i in range(0, len(points), chunk):
            page.runJavaScript(f"appendPoints({json.dumps(points[i:i + chunk])});")
        page.runJavaScript("finalizePoints();")
        if points:
            lats = [p['lat'] for p in points]
            lons = [p['lon'] for p in points]
            page.runJavaScript(
                f"fitBounds({min(lats)}, {min(lons)}, {max(lats)}, {max(lons)});")

    def _generate_map_html(self) -> str:
        return '''<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <style>
        * { margin: 0; padding: 0; }
        html, body { width: 100%; height: 100%; overflow: hidden; }
        #map { width: 100%; height: 100%; background: #1a1a2e; }
        .leaflet-control-zoom a { background-color: #2b2b2b !important; color: #e0e0e0 !important; border-color: #444 !important; }
        .leaflet-control-attribution { background-color: rgba(30,30,30,0.7) !important; color: #999 !important; }
        .leaflet-popup-content-wrapper { background-color: #2b2b2b !important; color: #e0e0e0 !important; border-radius: 6px !important; }
        .leaflet-popup-tip { background-color: #2b2b2b !important; }
    </style>
</head>
<body>
    <div id="map"></div>
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <script>
    var map = L.map('map').setView([39.8, -98.6], 4);
    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
        attribution: '&copy; OpenStreetMap &copy; CARTO', subdomains: 'abcd', maxZoom: 20
    }).addTo(map);
    var pointData = [];
    var dotLayer = null;
    var spatialGrid = {};
    var GRID_RES = 1; // 1-degree grid cells

    function buildGrid(points) {
        spatialGrid = {};
        for (var i = 0; i < points.length; i++) {
            var p = points[i];
            var key = Math.floor(p.lat / GRID_RES) + ',' + Math.floor(p.lon / GRID_RES);
            if (!spatialGrid[key]) spatialGrid[key] = [];
            spatialGrid[key].push(p);
        }
    }

    function getPointsInBounds(minLat, minLon, maxLat, maxLon) {
        var result = [];
        var y0 = Math.floor(minLat / GRID_RES), y1 = Math.floor(maxLat / GRID_RES);
        var x0 = Math.floor(minLon / GRID_RES), x1 = Math.floor(maxLon / GRID_RES);
        for (var y = y0; y <= y1; y++) {
            for (var x = x0; x <= x1; x++) {
                var cell = spatialGrid[y + ',' + x];
                if (cell) result.push.apply(result, cell);
            }
        }
        return result;
    }

    var CanvasDots = L.GridLayer.extend({
        createTile: function(coords) {
            var tile = document.createElement('canvas');
            var size = this.getTileSize();
            tile.width = size.x;
            tile.height = size.y;
            var ctx = tile.getContext('2d');
            var zoom = coords.z;
            var r = zoom >= 14 ? 3 : (zoom >= 10 ? 2 : 1);
            // Convert tile bounds to lat/lon
            var nw = map.unproject([coords.x * size.x, coords.y * size.y], zoom);
            var se = map.unproject([(coords.x + 1) * size.x, (coords.y + 1) * size.y], zoom);
            var pad = GRID_RES;
            var nearby = getPointsInBounds(
                se.lat - pad, nw.lng - pad, nw.lat + pad, se.lng + pad);
            if (nearby.length === 0) return tile;
            ctx.fillStyle = '#2ecc71';
            ctx.globalAlpha = 0.7;
            for (var i = 0; i < nearby.length; i++) {
                var p = nearby[i];
                var pt = map.project([p.lat, p.lon], zoom);
                var x = pt.x - coords.x * size.x;
                var y = pt.y - coords.y * size.y;
                if (x >= -r && x <= size.x + r && y >= -r && y <= size.y + r) {
                    ctx.beginPath();
                    ctx.arc(x, y, r, 0, 6.283);
                    ctx.fill();
                }
            }
            return tile;
        }
    });

    function setPoints(points) {
        // Single-shot variant kept for back-compat with smaller payloads
        // (cell tab + viewport-scoped queries). The dashboard now streams
        // via clearPoints/appendPoints/finalizePoints to dodge the
        // runJavaScript size limit when rendering all WiFi networks.
        clearPoints();
        appendPoints(points);
        finalizePoints();
    }
    function clearPoints() {
        pointData = [];
        spatialGrid = {};
        if (dotLayer) { map.removeLayer(dotLayer); dotLayer = null; }
    }
    function appendPoints(points) {
        for (var i = 0; i < points.length; i++) {
            var p = points[i];
            pointData.push(p);
            var key = Math.floor(p.lat / GRID_RES) + ',' + Math.floor(p.lon / GRID_RES);
            if (!spatialGrid[key]) spatialGrid[key] = [];
            spatialGrid[key].push(p);
        }
    }
    function finalizePoints() {
        if (dotLayer) { map.removeLayer(dotLayer); }
        dotLayer = new CanvasDots({ updateWhenZooming: false, updateWhenIdle: true });
        dotLayer.addTo(map);
    }
    function fitBounds(minLat, minLon, maxLat, maxLon) {
        map.fitBounds([[minLat, minLon], [maxLat, maxLon]], {padding: [30, 30]});
    }
    window.addEventListener('resize', function() { map.invalidateSize(); });
    new ResizeObserver(function() { map.invalidateSize(); }).observe(document.getElementById('map'));
    setTimeout(function() { map.invalidateSize(); }, 200);
    </script>
</body>
</html>'''

    # ─── Stats ───────────────────────────────────────────────────────

    def _refresh_stats(self):
        if not WigleApiClient.has_credentials():
            self._discovered_detail.setText("Configure API key in Settings")
            return
        self._refresh_btn.setEnabled(False)
        self._refresh_btn.setText("...")
        worker = _StatsWorker()
        worker.result.connect(self._on_stats)
        worker.result.connect(lambda _: self._workers.remove(worker))
        self._workers.append(worker)
        worker.start()

    def _on_stats(self, stats: dict):
        self._refresh_btn.setEnabled(True)
        self._refresh_btn.setText("Refresh")
        if not stats:
            self._discovered_label.setText("Error")
            self._discovered_detail.setText("API request failed")
            return

        discovered = stats.get('discovered', 0)
        total = stats.get('total_locations', 0)
        self._discovered_label.setText(f"{discovered:,}")
        self._discovered_detail.setText(f"({total:,} locations)")

        month_rank = stats.get('month_rank', 0)
        prev_month = stats.get('prev_month_rank', 0)
        if month_rank > 0:
            trend = " ^" if prev_month > 0 and month_rank < prev_month else (" v" if prev_month > 0 and month_rank > prev_month else "")
            self._monthly_rank_label.setText(f"#{month_rank:,}{trend}")
            self._monthly_detail.setText(f"(prev #{prev_month:,})" if prev_month > 0 else "")
        else:
            self._monthly_rank_label.setText("--")
            self._monthly_detail.setText("")

        rank = stats.get('rank', 0)
        prev_rank = stats.get('prev_rank', 0)
        if rank > 0:
            trend = " ^" if prev_rank > 0 and rank < prev_rank else (" v" if prev_rank > 0 and rank > prev_rank else "")
            self._overall_rank_label.setText(f"#{rank:,}{trend}")
            self._overall_detail.setText(f"(prev #{prev_rank:,})" if prev_rank > 0 else "")
        else:
            self._overall_rank_label.setText("--")
            self._overall_detail.setText("")

    # ─── Upload ──────────────────────────────────────────────────────

    def _add_upload_files(self):
        files, _ = QFileDialog.getOpenFileNames(
            self, "Select WiGLE CSV Files", "",
            "WiGLE CSV (*.wiglecsv *.csv *.csv.gz);;All Files (*)")
        for fp in files:
            self._add_file_to_upload_tree(fp)

    def _scan_local_wiglecsv(self):
        count = 0
        if _STAGE_DIR.exists():
            for fp in sorted(_STAGE_DIR.iterdir()):
                if fp.suffix.lower() == '.wiglecsv' and fp.is_file():
                    self._add_file_to_upload_tree(str(fp))
                    count += 1
        if count == 0:
            self._scan_local_btn.setText("No staged files")
            QTimer.singleShot(2000, lambda: self._scan_local_btn.setText("Scan Staged Files"))

    def _add_file_to_upload_tree(self, file_path: str):
        for i in range(self._upload_tree.topLevelItemCount()):
            if self._upload_tree.topLevelItem(i).data(0, Qt.ItemDataRole.UserRole) == file_path:
                return
        p = Path(file_path)
        size = p.stat().st_size if p.exists() else 0
        item = QTreeWidgetItem([p.name, _fmt_bytes(size), "Ready"])
        item.setData(0, Qt.ItemDataRole.UserRole, file_path)
        item.setCheckState(0, Qt.CheckState.Checked)
        self._upload_tree.addTopLevelItem(item)

    def _upload_selected(self):
        files = []
        for i in range(self._upload_tree.topLevelItemCount()):
            item = self._upload_tree.topLevelItem(i)
            if item.checkState(0) == Qt.CheckState.Checked and item.text(2) == "Ready":
                files.append(item.data(0, Qt.ItemDataRole.UserRole))
        if not files:
            return
        if not WigleApiClient.has_credentials():
            self._upload_btn.setText("Configure API key first")
            QTimer.singleShot(2000, lambda: self._upload_btn.setText("Upload Selected"))
            return
        self._upload_btn.setEnabled(False)
        self._upload_progress.setVisible(True)
        self._upload_progress.setMaximum(len(files))
        self._upload_progress.setValue(0)
        worker = _UploadWorker(files)
        worker.file_done.connect(self._on_file_uploaded)
        worker.all_done.connect(self._on_upload_complete)
        worker.all_done.connect(lambda: self._workers.remove(worker))
        self._workers.append(worker)
        worker.start()

    def _on_file_uploaded(self, path: str, success: bool, message: str):
        for i in range(self._upload_tree.topLevelItemCount()):
            item = self._upload_tree.topLevelItem(i)
            if item.data(0, Qt.ItemDataRole.UserRole) == path:
                if success:
                    item.setText(2, f"Uploaded ({message})")
                    item.setForeground(2, QColor('#2ecc71'))
                    self._move_to_uploaded(path)
                else:
                    item.setText(2, f"Failed: {message}")
                    item.setForeground(2, QColor('#e74c3c'))
                break
        self._upload_progress.setValue(self._upload_progress.value() + 1)

    def _on_upload_complete(self):
        self._upload_btn.setEnabled(True)
        self._upload_progress.setVisible(False)
        self._refresh_stats()
        self._refresh_transactions()

    @staticmethod
    def _move_to_uploaded(path: str):
        p = Path(path)
        if p.parent == _STAGE_DIR:
            uploaded_dir = _STAGE_DIR / 'uploaded'
            uploaded_dir.mkdir(parents=True, exist_ok=True)
            try:
                shutil.move(str(p), str(uploaded_dir / p.name))
            except Exception as e:
                log.warning("Failed to move uploaded file %s: %s", p.name, e)

    # ─── MAC Blocklist Filter ─────────────────────────────────────────

    _BLOCKLIST_PATH = Path.home() / '.config' / 'airparse' / 'mac_blocklist.txt'
    _WATCHLIST_PATH = Path.home() / '.config' / 'airparse' / 'mac_watchlist.txt'

    @staticmethod
    def _load_blocklist() -> tuple[set[str], list[str]]:
        """Load blocklist. Returns (full_macs, oui_prefixes)."""
        bp = WigleView._BLOCKLIST_PATH
        full = set()
        prefixes = []
        if not bp.exists():
            return full, prefixes
        for line in bp.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            entry = line.upper()
            if len(entry) <= 8:
                prefixes.append(entry)
            else:
                full.add(entry)
        return full, prefixes

    @staticmethod
    def _mac_blocked(mac: str, full_macs: set[str], prefixes: list[str]) -> bool:
        if mac in full_macs:
            return True
        for pfx in prefixes:
            if mac.startswith(pfx):
                return True
        return False

    def _filter_staged_files(self):
        full, prefixes = self._load_blocklist()
        if not full and not prefixes:
            self._filter_btn.setText("No blocklist found")
            QTimer.singleShot(2000, lambda: self._filter_btn.setText("Filter Staged"))
            return

        files = []
        for i in range(self._upload_tree.topLevelItemCount()):
            item = self._upload_tree.topLevelItem(i)
            if item.checkState(0) == Qt.CheckState.Checked and item.text(2) == "Ready":
                files.append((i, item.data(0, Qt.ItemDataRole.UserRole)))

        if not files:
            self._filter_btn.setText("No files to filter")
            QTimer.singleShot(2000, lambda: self._filter_btn.setText("Filter Staged"))
            return

        total_removed = 0
        total_kept = 0
        for idx, fp in files:
            removed, kept = self._filter_csv_file(fp, full, prefixes)
            total_removed += removed
            total_kept += kept
            item = self._upload_tree.topLevelItem(idx)
            new_size = Path(fp).stat().st_size if Path(fp).exists() else 0
            item.setText(1, _fmt_bytes(new_size))

        msg = f"Removed {total_removed:,} rows ({total_kept:,} kept)"
        log.info("WiGLE filter: %s", msg)
        self._filter_btn.setText(msg)
        QTimer.singleShot(4000, lambda: self._filter_btn.setText("Filter Staged"))

    @staticmethod
    def _filter_csv_file(file_path: str, full_macs: set[str], prefixes: list[str]) -> tuple[int, int]:
        p = Path(file_path)
        lines = p.read_text().splitlines()
        if len(lines) < 3:
            return 0, 0

        header_line = lines[0]
        column_line = lines[1]
        data_lines = lines[2:]

        kept = []
        removed = 0
        for line in data_lines:
            mac = line.split(',', 1)[0].upper()
            if WigleView._mac_blocked(mac, full_macs, prefixes):
                removed += 1
            else:
                kept.append(line)

        p.write_text(header_line + '\n' + column_line + '\n' + '\n'.join(kept) + '\n')
        return removed, len(kept)

    _HEADER_SKIP = ('AirParse', 'One MAC', 'OUI prefix', 'These MACs', 'Matching devices')

    @staticmethod
    def _parse_list_file(path: Path) -> list[tuple[str, str, str]]:
        """Parse a MAC list file into (mac, label, type) tuples."""
        if not path.exists():
            return []
        entries = []
        pending_comment = ""
        for line in path.read_text().splitlines():
            stripped = line.strip()
            if not stripped:
                pending_comment = ""
                continue
            if stripped.startswith('#'):
                text = stripped.lstrip('#').strip()
                if text and not any(text.startswith(k) for k in WigleView._HEADER_SKIP):
                    pending_comment = text
                continue
            mac = stripped.upper()
            typ = "OUI Prefix" if len(mac) <= 8 else "Full MAC"
            entries.append((mac, pending_comment, typ))
            pending_comment = ""
        return entries

    @staticmethod
    def _save_list_entries(path: Path, header: str, entries: list[tuple[str, str, str]]):
        """Write entries back to a MAC list file."""
        path.parent.mkdir(parents=True, exist_ok=True)
        lines = [header, ""]
        for mac, label, _ in entries:
            if label:
                lines.append(f"# {label}")
            lines.append(mac)
        lines.append("")
        path.write_text('\n'.join(lines))

    @staticmethod
    def _parse_blocklist_file() -> list[tuple[str, str, str]]:
        return WigleView._parse_list_file(WigleView._BLOCKLIST_PATH)

    @staticmethod
    def _save_blocklist_entries(entries: list[tuple[str, str, str]]):
        WigleView._save_list_entries(
            WigleView._BLOCKLIST_PATH,
            "# AirParse MAC Blocklist\n"
            "# One MAC per line (case-insensitive). Lines starting with # are comments.\n"
            '# OUI prefix matching: use partial MACs like "94:83:C4" to match all devices from that vendor.\n'
            "# These MACs are stripped from WiGLE CSVs before upload.",
            entries,
        )

    def _show_list_editor(self, *, title: str, list_path: Path, header: str,
                          save_fn, extra_buttons: list = None):
        """Shared MAC list editor dialog for blocklist/watchlist."""
        dlg = QDialog(self)
        dlg.setWindowTitle(title)
        dlg.setMinimumSize(620, 480)
        dlg.setStyleSheet("background-color: #1e1e1e; color: #e0e0e0;")

        layout = QVBoxLayout(dlg)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(8)

        _tbl_style = """
            QTableWidget {
                background-color: #2b2b2b; color: #e0e0e0;
                border: 1px solid #444; border-radius: 4px;
                gridline-color: #3a3a3a; font-family: monospace; font-size: 12px;
                alternate-background-color: #313131;
            }
            QTableWidget::item:selected { background-color: #2980b9; }
            QHeaderView::section {
                background-color: #333; color: #e0e0e0; padding: 4px 8px;
                border: none; border-bottom: 1px solid #555; font-weight: bold;
            }
        """
        _btn_style = (
            "QPushButton {{ background-color: {bg}; color: {fg}; padding: 5px 14px;"
            " border-radius: 4px; font-weight: bold; }}"
            "QPushButton:hover {{ background-color: {hover}; }}"
        )

        table = QTableWidget()
        table.setColumnCount(3)
        table.setHorizontalHeaderLabels(["MAC / OUI", "Label", "Type"])
        table.setStyleSheet(_tbl_style)
        table.setAlternatingRowColors(True)
        table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        table.verticalHeader().setVisible(False)

        entries = self._parse_list_file(list_path)

        def _populate():
            table.setRowCount(len(entries))
            for i, (mac, label, typ) in enumerate(entries):
                mac_item = QTableWidgetItem(mac)
                mac_item.setFlags(mac_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                label_item = QTableWidgetItem(label)
                type_item = QTableWidgetItem(typ)
                type_item.setFlags(type_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                if typ == "OUI Prefix":
                    type_item.setForeground(QColor("#f39c12"))
                else:
                    type_item.setForeground(QColor("#999"))
                table.setItem(i, 0, mac_item)
                table.setItem(i, 1, label_item)
                table.setItem(i, 2, type_item)

        _populate()
        layout.addWidget(table)

        add_row = QHBoxLayout()
        mac_input = QLineEdit()
        mac_input.setPlaceholderText("MAC address or OUI prefix (e.g. AA:BB:CC:DD:EE:FF or AA:BB:CC)")
        mac_input.setStyleSheet(
            "QLineEdit { background-color: #2b2b2b; color: #e0e0e0; border: 1px solid #444;"
            " border-radius: 4px; padding: 5px 8px; font-family: monospace; }"
        )
        label_input = QLineEdit()
        label_input.setPlaceholderText("Label (optional)")
        label_input.setStyleSheet(mac_input.styleSheet())
        label_input.setMaximumWidth(180)

        add_btn = QPushButton("Add")
        add_btn.setStyleSheet(_btn_style.format(bg="#2ecc71", fg="white", hover="#27ae60"))

        add_row.addWidget(mac_input, 3)
        add_row.addWidget(label_input, 2)
        add_row.addWidget(add_btn)
        layout.addLayout(add_row)

        def _add():
            raw = mac_input.text().strip().upper()
            if not raw:
                return
            for existing_mac, _, _ in entries:
                if existing_mac == raw:
                    mac_input.setText("")
                    mac_input.setPlaceholderText("Already in list")
                    QTimer.singleShot(2000, lambda: mac_input.setPlaceholderText(
                        "MAC address or OUI prefix (e.g. AA:BB:CC:DD:EE:FF or AA:BB:CC)"))
                    return
            typ = "OUI Prefix" if len(raw) <= 8 else "Full MAC"
            lbl = label_input.text().strip()
            entries.append((raw, lbl, typ))
            _populate()
            mac_input.setText("")
            label_input.setText("")
            count_label.setText(f"{len(entries)} entries")

        add_btn.clicked.connect(_add)
        mac_input.returnPressed.connect(_add)

        bottom_row = QHBoxLayout()

        count_label = QLabel(f"{len(entries)} entries")
        count_label.setStyleSheet(_DIM_STYLE)
        bottom_row.addWidget(count_label)

        bottom_row.addStretch()

        remove_btn = QPushButton("Remove Selected")
        remove_btn.setStyleSheet(_btn_style.format(bg="#c0392b", fg="white", hover="#a93226"))
        bottom_row.addWidget(remove_btn)

        if extra_buttons:
            for btn in extra_buttons:
                btn.setStyleSheet(_btn_style.format(bg="#2980b9", fg="white", hover="#2471a3"))
                bottom_row.addWidget(btn)

        cancel_btn = QPushButton("Cancel")
        cancel_btn.setStyleSheet(_btn_style.format(bg="#555", fg="#e0e0e0", hover="#666"))
        bottom_row.addWidget(cancel_btn)

        save_btn = QPushButton("Save")
        save_btn.setStyleSheet(_btn_style.format(bg="#2ecc71", fg="white", hover="#27ae60"))
        bottom_row.addWidget(save_btn)

        layout.addLayout(bottom_row)

        def _remove():
            rows = sorted(set(idx.row() for idx in table.selectedIndexes()), reverse=True)
            for r in rows:
                entries.pop(r)
            _populate()
            count_label.setText(f"{len(entries)} entries")

        def _collect():
            for i in range(len(entries)):
                lbl_item = table.item(i, 1)
                if lbl_item:
                    entries[i] = (entries[i][0], lbl_item.text().strip(), entries[i][2])
            return entries

        def _save():
            _collect()
            save_fn(entries)
            dlg.accept()

        remove_btn.clicked.connect(_remove)
        cancel_btn.clicked.connect(dlg.reject)
        save_btn.clicked.connect(_save)

        dlg._entries = entries
        dlg._collect = _collect
        dlg._btn_style = _btn_style
        dlg.exec()

    def _show_blocklist_editor(self):
        sync_btn = QPushButton("Sync to Kismet")
        sync_btn.setToolTip("Push blocklist filters to Kismet RPi5 config")

        dlg_ref = {}

        def _on_sync():
            sync_btn.setEnabled(False)
            sync_btn.setText("Syncing...")
            try:
                parent = sync_btn.parent()
                while parent and not isinstance(parent, QDialog):
                    parent = parent.parent()
                entries = parent._collect() if parent else []
                self._save_blocklist_entries(entries)
                result = self._sync_blocklist_to_kismet(entries)
                sync_btn.setText(result)
                QTimer.singleShot(3000, lambda: (
                    sync_btn.setText("Sync to Kismet"),
                    sync_btn.setEnabled(True),
                ))
            except Exception as e:
                log.error("Kismet sync failed: %s", e)
                sync_btn.setText("Sync Failed")
                sync_btn.setEnabled(True)
                QTimer.singleShot(3000, lambda: sync_btn.setText("Sync to Kismet"))

        sync_btn.clicked.connect(_on_sync)

        self._show_list_editor(
            title="MAC Blocklist Editor",
            list_path=self._BLOCKLIST_PATH,
            header=(
                "# AirParse MAC Blocklist\n"
                "# One MAC per line (case-insensitive). Lines starting with # are comments.\n"
                '# OUI prefix matching: use partial MACs like "94:83:C4" to match all devices from that vendor.\n'
                "# These MACs are stripped from WiGLE CSVs before upload."
            ),
            save_fn=self._save_blocklist_entries,
            extra_buttons=[sync_btn],
        )

    def _show_watchlist_editor(self):
        self._show_list_editor(
            title="MAC Watchlist Editor",
            list_path=self._WATCHLIST_PATH,
            header=(
                "# AirParse MAC Watchlist\n"
                "# Matching devices are highlighted in orange in device tables.\n"
                '# OUI prefix matching: use partial MACs like "00:30:44" to match all devices from that vendor.'
            ),
            save_fn=lambda entries: self._save_list_entries(
                self._WATCHLIST_PATH,
                "# AirParse MAC Watchlist\n"
                "# Matching devices are highlighted in orange in device tables.\n"
                '# OUI prefix matching: use partial MACs like "00:30:44" to match all devices from that vendor.',
                entries,
            ),
        )

    @staticmethod
    def _generate_kismet_filters(entries: list[tuple[str, str, str]]) -> str:
        """Generate kismet_site.conf filter lines from blocklist entries."""
        lines = [
            "",
            "# --------------------------------------------------",
            "# MAC Blocklist — synced from AirParse",
            "# --------------------------------------------------",
            "",
        ]
        for mac, label, typ in entries:
            if label:
                lines.append(f"# {label}")
            if typ == "OUI Prefix":
                oui = mac.upper()
                mask_addr = f"{oui}:00:00:00/FF:FF:FF:00:00:00"
                lines.append(f"kis_log_device_filter=IEEE802.11,{mask_addr},block")
                lines.append(f"kis_log_packet_filter=IEEE802.11,any,{mask_addr},block")
            else:
                lines.append(f"kis_log_device_filter=IEEE802.11,{mac},block")
                lines.append(f"kis_log_packet_filter=IEEE802.11,any,{mac},block")
        lines.append("")
        return '\n'.join(lines)

    def _sync_blocklist_to_kismet(self, entries: list[tuple[str, str, str]]) -> str:
        """SSH to the Kismet RPi5 and update kismet_site.conf filters."""
        import paramiko
        from sources import SOURCES_FILE, SourceConfig

        sources_data = json.loads(SOURCES_FILE.read_text())
        kismet_cfg = None
        for s in sources_data.get('sources', []):
            if s.get('type') == 'kismet':
                kismet_cfg = SourceConfig.from_dict(s)
                break

        if not kismet_cfg:
            return "No Kismet source configured"

        conf_path = "/etc/kismet/kismet_site.conf"
        filter_marker = "# MAC Blocklist — synced from AirParse"
        old_marker = "# Travel Kit Filter"

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        key_path = Path(kismet_cfg.key_file).expanduser()
        connect_kw = {
            'hostname': kismet_cfg.host,
            'port': kismet_cfg.port,
            'username': kismet_cfg.user,
            'timeout': 5,
        }
        if key_path.exists():
            connect_kw['key_filename'] = str(key_path)

        try:
            ssh.connect(**connect_kw)

            _, stdout, _ = ssh.exec_command(f"cat {conf_path}")
            existing = stdout.read().decode()

            kept_lines = []
            in_filter_block = False
            for line in existing.splitlines():
                if filter_marker in line or old_marker in line:
                    in_filter_block = True
                    continue
                if in_filter_block:
                    if line.startswith('kis_log_device_filter=') or \
                       line.startswith('kis_log_packet_filter=') or \
                       line.startswith('#') or line.strip() == '':
                        continue
                    else:
                        in_filter_block = False
                kept_lines.append(line)

            while kept_lines and kept_lines[-1].strip() == '':
                kept_lines.pop()

            new_filters = self._generate_kismet_filters(entries)
            new_conf = '\n'.join(kept_lines) + '\n' + new_filters

            escaped = new_conf.replace("'", "'\\''")
            cmd = f"echo '{escaped}' | sudo tee {conf_path} > /dev/null"
            _, stdout, stderr = ssh.exec_command(cmd)
            exit_code = stdout.channel.recv_exit_status()

            if exit_code != 0:
                err = stderr.read().decode().strip()
                return f"Write failed: {err}"

            count = len(entries)
            return f"Synced {count} filters to Kismet"
        finally:
            ssh.close()

    # ─── Transactions ────────────────────────────────────────────────

    def _refresh_transactions(self):
        if not WigleApiClient.has_credentials():
            return
        self._refresh_tx_btn.setEnabled(False)
        self._refresh_tx_btn.setText("Loading...")
        worker = _TransactionsWorker()
        worker.result.connect(self._on_transactions)
        worker.result.connect(lambda _: self._workers.remove(worker))
        self._workers.append(worker)
        worker.start()

    def _on_transactions(self, txns: list):
        self._refresh_tx_btn.setEnabled(True)
        self._refresh_tx_btn.setText("Refresh Transactions")
        _populate_tx_tree(self._tx_tree, txns)

        # Queue status banner
        processing = sum(1 for t in txns if 'process' in t.get('status', '').lower())
        trilat = sum(1 for t in txns if 'trilat' in t.get('status', '').lower())
        geo = sum(1 for t in txns if 'geo' in t.get('status', '').lower())
        if processing or trilat or geo:
            parts = []
            if processing:
                parts.append(f"{processing} processing")
            if trilat:
                parts.append(f"{trilat} trilaterating")
            if geo:
                parts.append(f"{geo} geolocating")
            self._queue_status.setText(f"WiGLE queue: {', '.join(parts)}")
            self._queue_status.setVisible(True)
        else:
            self._queue_status.setVisible(False)

    # ─── Downloads ───────────────────────────────────────────────────

    def _find_downloadable(self):
        if not WigleApiClient.has_credentials():
            self._dl_status.setText("Configure API key in Settings first")
            return
        self._dl_find_btn.setEnabled(False)
        self._dl_find_btn.setText("Loading...")
        self._dl_tree.clear()
        worker = _TransactionsWorker()
        worker.result.connect(self._on_download_transactions)
        worker.result.connect(lambda _: self._workers.remove(worker))
        self._workers.append(worker)
        worker.start()

    def _on_download_transactions(self, txns: list):
        self._dl_find_btn.setEnabled(True)
        self._dl_find_btn.setText("Find Transactions")
        self._dl_tree.clear()

        start = self._dl_start.date().toString("yyyyMMdd")
        end = self._dl_end.date().toString("yyyyMMdd")
        only_new = self._dl_only_new.isChecked()
        ignore_failed = self._dl_ignore_failed.isChecked()
        ignored = _load_ignored_transids() if ignore_failed else set()

        filtered = []
        skipped_ignored = 0
        skipped_wigle_failed = 0
        for tx in txns:
            transid = tx.get('transid', '')
            if not transid or len(transid) < 8:
                continue
            date_part = transid[:8]
            if date_part < start or date_part > end:
                continue
            has_local = (_KML_DIR / f"{transid}.kml").exists()
            if only_new and has_local:
                continue
            # Skip transactions we've already tried and failed to download
            if ignore_failed and transid in ignored:
                skipped_ignored += 1
                continue
            # Skip transactions WiGLE marked as failed (empty uploads, etc.)
            wigle_status = tx.get('status', '').lower()
            if ignore_failed and ('fail' in wigle_status or 'error' in wigle_status):
                skipped_wigle_failed += 1
                continue
            # Skip transactions with zero data across the board
            total_data = (
                (tx.get('total', 0) or 0)
                + (tx.get('genTotal', 0) or 0)
                + (tx.get('btTotal', 0) or 0)
            )
            if total_data == 0:
                skipped_wigle_failed += 1
                continue
            tx = dict(tx)
            if has_local:
                tx['status'] = "Downloaded"
            elif tx.get('status', '') not in _STATUS_CODES:
                tx['status'] = "New"
            filtered.append(tx)

        _populate_tx_tree(self._dl_tree, filtered)
        count = len(filtered)
        parts = [f"{count} transaction{'s' if count != 1 else ''} found"]
        if skipped_ignored or skipped_wigle_failed:
            skip_total = skipped_ignored + skipped_wigle_failed
            parts.append(f"{skip_total} ignored")
        self._dl_status.setText(" | ".join(parts))
        self._dl_all_btn.setEnabled(count > 0)

    def _download_all(self):
        transids = []
        for i in range(self._dl_tree.topLevelItemCount()):
            item = self._dl_tree.topLevelItem(i)
            if item.text(1) != "Downloaded":  # Status column
                transids.append(item.data(0, Qt.ItemDataRole.UserRole))
        self._start_download(transids)

    def _start_download(self, transids: list[str]):
        if not transids:
            self._dl_status.setText("Nothing to download")
            return
        if hasattr(self, '_dl_worker') and self._dl_worker and self._dl_worker.isRunning():
            self._dl_status.setText("A download is already in progress")
            return
        self._dl_all_btn.setEnabled(False)
        self._dl_all_btn.setText("Downloading...")
        self._dl_cancel_btn.setVisible(True)
        self._dl_progress.setVisible(True)
        self._dl_progress.setMaximum(len(transids))
        self._dl_progress.setValue(0)
        self._dl_worker = _KmlBatchWorker(transids)
        self._dl_worker.file_done.connect(self._on_kml_file_done)
        self._dl_worker.all_done.connect(self._on_kml_batch_done)
        self._dl_worker.all_done.connect(lambda _: self._workers.remove(self._dl_worker))
        self._workers.append(self._dl_worker)
        self._dl_worker.start()

    def _on_dl_tree_menu(self, pos):
        item_at = self._dl_tree.itemAt(pos)
        selected = self._dl_tree.selectedItems()
        if not item_at and not selected:
            return
        menu = QMenu(self._dl_tree)

        # Scope: if right-click target is part of the selection, actions
        # operate on the whole selection; otherwise just the single row.
        if item_at and item_at in selected and len(selected) > 1:
            scope_items = list(selected)
        elif item_at:
            scope_items = [item_at]
        else:
            scope_items = list(selected)

        scope_transids = [it.data(0, Qt.ItemDataRole.UserRole) for it in scope_items]
        dl_transids = [
            it.data(0, Qt.ItemDataRole.UserRole)
            for it in scope_items
            if it.text(1) != "Downloaded"
        ]
        merge_transids = [
            it.data(0, Qt.ItemDataRole.UserRole)
            for it in scope_items
            if (_KML_DIR / f"{it.data(0, Qt.ItemDataRole.UserRole)}.kml").exists()
        ]
        multi = len(scope_items) > 1

        # Download action
        if multi:
            dl_label = f"Download Selected ({len(dl_transids)})"
        else:
            already = scope_items[0].text(1) == "Downloaded"
            dl_label = "Re-download" if already else "Download"
        act_dl = QAction(dl_label, menu)
        act_dl.setEnabled(bool(dl_transids) or (not multi))
        if multi:
            act_dl.triggered.connect(lambda: self._start_download(dl_transids))
        else:
            act_dl.triggered.connect(lambda: self._start_download(scope_transids))
        menu.addAction(act_dl)

        # Merge to QGIS KML action
        menu.addSeparator()
        if multi:
            merge_label = f"Merge to QGIS KML ({len(merge_transids)})"
        else:
            merge_label = "Merge to QGIS KML"
        act_merge = QAction(merge_label, menu)
        act_merge.setEnabled(bool(merge_transids))
        act_merge.triggered.connect(lambda: self._merge_selected_to_qgis(merge_transids))
        menu.addAction(act_merge)

        menu.exec(self._dl_tree.viewport().mapToGlobal(pos))

    def _merge_selected_to_qgis(self, transids: list[str]):
        from datetime import datetime
        files = [_KML_DIR / f"{t}.kml" for t in transids]
        files = [f for f in files if f.exists()]
        if not files:
            QMessageBox.information(
                self, "Merge to QGIS KML",
                "None of the selected transactions have been downloaded yet.")
            return
        if hasattr(self, '_sel_merge_worker') and self._sel_merge_worker \
                and self._sel_merge_worker.isRunning():
            self._dl_status.setText("A merge is already in progress")
            return
        stamp = datetime.now().strftime('%Y%m%d-%H%M%S')
        out_path = Path.home() / 'Downloads' / f'qgis-export-{stamp}.kml'
        label = f'WiGLE selection ({len(files)} transactions, {stamp})'

        self._dl_status.setText(f"Merging {len(files)} file(s)...")
        self._sel_merge_worker = _KmlSelectedMergeWorker(files, out_path, label)
        self._sel_merge_worker.progress.connect(self._dl_status.setText)
        self._sel_merge_worker.finished_ok.connect(self._on_sel_merge_done)
        self._sel_merge_worker.failed.connect(self._on_sel_merge_failed)
        self._sel_merge_worker.start()

    def _on_sel_merge_done(self, out_path: Path, placemarks: int, files_used: int):
        self._dl_status.setText(
            f"Merged {files_used} file(s), {placemarks:,} placemarks → "
            f"~/Downloads/{out_path.name}"
        )
        log.info("Selected merge complete: %s (%d placemarks)", out_path, placemarks)

    def _on_sel_merge_failed(self, msg: str):
        self._dl_status.setText(f"Merge failed: {msg}")
        QMessageBox.warning(self, "Merge failed", msg)

    def _on_kml_file_done(self, transid: str, status: str):
        self._dl_progress.setValue(self._dl_progress.value() + 1)
        for i in range(self._dl_tree.topLevelItemCount()):
            item = self._dl_tree.topLevelItem(i)
            if item.data(0, Qt.ItemDataRole.UserRole) == transid:
                if status == "ok":
                    item.setText(1, "Downloaded")
                elif status == "empty":
                    # WiGLE reports the transaction has no KML content —
                    # permanent, not worth retrying.
                    item.setText(1, "Empty")
                    item.setForeground(1, QColor('#f39c12'))
                    _add_ignored_transid(transid)
                else:  # "error"
                    # Transient (HTTP 5xx, timeout, etc.) — leave retryable.
                    item.setText(1, "Failed")
                    item.setForeground(1, QColor('#e74c3c'))
                break

    def _cancel_download(self):
        if hasattr(self, '_dl_worker') and self._dl_worker:
            self._dl_worker.cancel()
            self._dl_cancel_btn.setEnabled(False)
            self._dl_cancel_btn.setText("Cancelling...")
            self._dl_status.setText("Cancelling...")

    def _on_kml_batch_done(self, total: int):
        self._dl_all_btn.setEnabled(True)
        self._dl_all_btn.setText("Download All")
        self._dl_cancel_btn.setVisible(False)
        self._dl_cancel_btn.setEnabled(True)
        self._dl_cancel_btn.setText("Cancel")
        self._dl_progress.setVisible(False)
        ignored_count = len(_load_ignored_transids())
        self._dl_clear_ignored_btn.setText(
            f"Clear Ignore List ({ignored_count})" if ignored_count else "Clear Ignore List")
        self._dl_clear_ignored_btn.setEnabled(ignored_count > 0)
        self._dl_status.setText(f"Downloaded {total} KML file{'s' if total != 1 else ''}")
        self._load_kml_to_map()

    def _clear_ignored(self):
        _save_ignored_transids(set())
        self._dl_clear_ignored_btn.setText("Clear Ignore List")
        self._dl_clear_ignored_btn.setEnabled(False)
        self._dl_status.setText("Ignore list cleared")

    def _export_for_qgis(self):
        """Per-year KML merge to ~/Downloads — the original 'QGIS Export'
        behavior, now secondary to the GeoPackage merge but kept for ad-hoc use."""
        year_counts = _scan_years(_KML_DIR)
        if not year_counts:
            QMessageBox.information(
                self, "Per-year KML Export",
                "No KML files found. Use 'Find Transactions' and 'Download All' first.",
            )
            return
        years = self._pick_export_years(year_counts)
        if not years:
            return
        self._qgis_kml_btn.setEnabled(False)
        self._qgis_status.setText("Exporting per-year KMLs to ~/Downloads…")
        self._qgis_worker = _KmlMergeWorker(years=years)
        self._qgis_worker.progress.connect(self._qgis_status.setText)
        self._qgis_worker.finished_ok.connect(self._on_qgis_export_done)
        self._qgis_worker.failed.connect(self._on_qgis_export_failed)
        self._qgis_worker.start()

    def _pick_export_years(self, year_counts: dict[str, int]) -> set[str] | None:
        """Show a checkbox dialog for year selection.
        Returns a set of selected years, or None if cancelled.
        Defaults to current year preselected (or all if current year absent)."""
        from datetime import date
        current = str(date.today().year)

        dlg = QDialog(self)
        dlg.setWindowTitle("QGIS Export — Select Years")
        dlg.setMinimumWidth(340)
        dlg.setStyleSheet("background-color: #1e1e1e; color: #e0e0e0;")

        layout = QVBoxLayout(dlg)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(8)

        header = QLabel("Which years should be merged and written to ~/Downloads?")
        header.setStyleSheet(_LABEL_STYLE)
        header.setWordWrap(True)
        layout.addWidget(header)

        checks: dict[str, QCheckBox] = {}
        any_preselected = current in year_counts
        for year in sorted(year_counts.keys()):
            cb = QCheckBox(f"{year}  ({year_counts[year]} file{'s' if year_counts[year] != 1 else ''})")
            cb.setStyleSheet(_LABEL_STYLE)
            cb.setChecked(year == current if any_preselected else True)
            layout.addWidget(cb)
            checks[year] = cb

        quick_row = QHBoxLayout()
        btn_all = _action_btn("Select All", "#555", "white")
        btn_all.clicked.connect(lambda: [cb.setChecked(True) for cb in checks.values()])
        btn_none = _action_btn("Clear", "#555", "white")
        btn_none.clicked.connect(lambda: [cb.setChecked(False) for cb in checks.values()])
        quick_row.addWidget(btn_all)
        quick_row.addWidget(btn_none)
        quick_row.addStretch()
        layout.addLayout(quick_row)

        bb = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        bb.button(QDialogButtonBox.StandardButton.Ok).setText("Export")
        bb.accepted.connect(dlg.accept)
        bb.rejected.connect(dlg.reject)
        layout.addWidget(bb)

        def _update_ok():
            bb.button(QDialogButtonBox.StandardButton.Ok).setEnabled(
                any(cb.isChecked() for cb in checks.values()))
        for cb in checks.values():
            cb.toggled.connect(_update_ok)
        _update_ok()

        if dlg.exec() != QDialog.DialogCode.Accepted:
            return None
        return {y for y, cb in checks.items() if cb.isChecked()}

    def _on_qgis_export_done(self, results: dict):
        self._qgis_kml_btn.setEnabled(True)
        if not results:
            self._qgis_status.setText("No KMLs to export.")
            return
        years = ", ".join(f"{y}.kml" for y in sorted(results.keys()))
        self._qgis_status.setText(
            f"Exported {len(results)} file{'s' if len(results) != 1 else ''} to ~/Downloads: {years}"
        )
        self._qgis_log.appendPlainText(
            f"Per-year KML export → {years}"
        )
        log.info("QGIS per-year KML export complete: %s", results)

    def _on_qgis_export_failed(self, msg: str):
        self._qgis_kml_btn.setEnabled(True)
        self._qgis_status.setText(f"Export failed: {msg}")
        self._qgis_log.appendPlainText(f"Per-year KML export failed: {msg}")
        QMessageBox.warning(self, "Per-year KML Export failed", msg)
