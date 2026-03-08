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
    QLineEdit, QDateEdit, QCheckBox, QSizePolicy
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QTimer, QDate
from PyQt6.QtGui import QFont, QColor

try:
    from PyQt6.QtWebEngineWidgets import QWebEngineView
    HAS_WEBENGINE = True
except ImportError:
    HAS_WEBENGINE = False

from database.wigle_api import WigleApiClient

log = logging.getLogger(__name__)

_KML_DIR = Path.home() / '.config' / 'airparse' / 'kml'
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
    """Parse all local KML files off the main thread."""
    result = pyqtSignal(list)
    def run(self):
        points = []
        if _KML_DIR.exists():
            for kml_file in _KML_DIR.glob('*.kml'):
                points.extend(_parse_kml_points(kml_file))
        self.result.emit(points)


class _KmlBatchWorker(QThread):
    file_done = pyqtSignal(str, bool)
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
                self.file_done.emit(tid, True)
                downloaded += 1
                continue
            ok, data = client.download_kml(tid)
            if self._cancelled:
                break
            if ok and data:
                _KML_DIR.mkdir(parents=True, exist_ok=True)
                out.write_bytes(data)
                downloaded += 1
            self.file_done.emit(tid, ok and bool(data))
        self.all_done.emit(downloaded)


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



# ─── Main View ──────────────────────────────────────────────────────

class WigleView(QWidget):
    PAGE_DASHBOARD = 0
    PAGE_UPLOAD = 1
    PAGE_DOWNLOADS = 2

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
        layout.addWidget(self._stack)

    def show_page(self, index: int):
        self._stack.setCurrentIndex(index)

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
        if not _KML_DIR.exists():
            return
        self._reload_map_btn.setEnabled(False)
        self._reload_map_btn.setText("Loading KML...")
        worker = _KmlParseWorker()
        worker.result.connect(self._on_kml_parsed)
        worker.result.connect(lambda _: self._workers.remove(worker))
        self._workers.append(worker)
        worker.start()

    def _on_kml_parsed(self, all_points: list):
        self._reload_map_btn.setEnabled(True)
        if all_points:
            if self._map_ready:
                self._send_points_to_map(all_points)
            else:
                self._pending_points = all_points
            self._reload_map_btn.setText(f"Reload KML Data ({len(all_points):,} points)")
        else:
            self._reload_map_btn.setText("No KML files found")
            QTimer.singleShot(3000, lambda: self._reload_map_btn.setText("Reload KML Data"))

    def _send_points_to_map(self, points: list[dict]):
        if not self._map_view:
            return
        data = json.dumps(points)
        self._map_view.page().runJavaScript(f"setPoints({data});")
        if points:
            lats = [p['lat'] for p in points]
            lons = [p['lon'] for p in points]
            self._map_view.page().runJavaScript(
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
        pointData = points;
        buildGrid(points);
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
        if not transids:
            self._dl_status.setText("Nothing to download")
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

    def _on_kml_file_done(self, transid: str, success: bool):
        self._dl_progress.setValue(self._dl_progress.value() + 1)
        for i in range(self._dl_tree.topLevelItemCount()):
            item = self._dl_tree.topLevelItem(i)
            if item.data(0, Qt.ItemDataRole.UserRole) == transid:
                if success:
                    item.setText(1, "Downloaded")
                else:
                    item.setText(1, "Failed")
                    item.setForeground(1, QColor('#e74c3c'))
                    _add_ignored_transid(transid)
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
