"""PCAP-specific analysis views for handshakes, deauths, and probe requests."""

import json
import time as _time
import urllib.request
from pathlib import Path

import numpy as np
import pandas as pd

from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QFrame, QTableWidget, QTableWidgetItem, QHeaderView,
    QSplitter, QScrollArea, QTableView, QAbstractItemView,
    QMenu, QApplication, QDialog, QCheckBox, QPushButton,
    QDialogButtonBox, QFileDialog, QLineEdit,
    QComboBox, QTabWidget, QDateTimeEdit, QSpinBox,
    QProgressBar, QToolTip
)
from PyQt6.QtCore import Qt, QAbstractTableModel, QModelIndex, QSortFilterProxyModel, QDateTime, QThread, pyqtSignal
from PyQt6.QtGui import QColor, QBrush, QAction, QCursor

try:
    import pyqtgraph as pg
    HAS_PYQTGRAPH = True
except ImportError:
    HAS_PYQTGRAPH = False


# --- Fuzzy matching helpers (Feature 1) ---

_FUZZY_SUFFIXES = ['_2g', '_5g', '_2ghz', '_5ghz', '_2.4', '_5', '-guest',
                   '_guest', '_ext', '_repeater', '_upstairs', '-5g', '-2g',
                   '-2.4g', '-5g-1', '-5g-2']


def _strip_ssid_suffixes(ssid: str) -> str:
    """Strip common network suffixes for fuzzy matching."""
    lower = ssid.lower()
    for suffix in sorted(_FUZZY_SUFFIXES, key=len, reverse=True):
        if lower.endswith(suffix):
            return ssid[:len(ssid) - len(suffix)]
    return ssid


# --- Reverse geocoding ---

_geocode_cache: dict[tuple[float, float], str] = {}


def _reverse_geocode(lat: float, lon: float) -> str:
    """Reverse geocode lat/lon to a short address via Nominatim."""
    key = (round(lat, 4), round(lon, 4))
    if key in _geocode_cache:
        return _geocode_cache[key]
    try:
        url = (f"https://nominatim.openstreetmap.org/reverse?"
               f"lat={lat}&lon={lon}&format=json&zoom=18&addressdetails=1")
        req = urllib.request.Request(
            url, headers={'User-Agent': 'AirParse/2.0'})
        with urllib.request.urlopen(req, timeout=3) as resp:
            data = json.loads(resp.read())
            addr = data.get('address', {})
            # Build short address: road + city
            parts = []
            road = addr.get('road', '')
            house = addr.get('house_number', '')
            if road:
                parts.append(f"{house} {road}".strip() if house else road)
            city = (addr.get('city') or addr.get('town')
                    or addr.get('village') or addr.get('hamlet', ''))
            if city:
                parts.append(city)
            state = addr.get('state', '')
            if state:
                parts.append(state)
            result = ', '.join(parts) if parts else data.get('display_name', '')[:60]
            _geocode_cache[key] = result
            return result
    except Exception:
        _geocode_cache[key] = ''
        return ''


class _GeocodeWorker(QThread):
    """Background thread for reverse geocoding AP locations."""
    result_ready = pyqtSignal(int, str)  # row_index, address

    def __init__(self, coords: list, parent=None):
        super().__init__(parent)
        self.coords = coords  # [(row_idx, lat, lon), ...]
        self._stop = False

    def stop(self):
        self._stop = True

    def run(self):
        for i, (row_idx, lat, lon) in enumerate(self.coords):
            if self._stop:
                return
            address = _reverse_geocode(lat, lon)
            if self._stop:
                return
            self.result_ready.emit(row_idx, address)
            if i < len(self.coords) - 1:
                _time.sleep(1.1)  # Respect Nominatim rate limit


# --- Probe table model ---

class _ProbeTableModel(QAbstractTableModel):
    """Table model for probe request DataFrame."""

    _COLUMNS = ['client_mac', 'manufacturer', 'randomized', 'probed_ssids',
                'probe_count', 'strongest_signal', 'first_seen', 'last_seen']
    _HEADERS = ['Client MAC', 'Manufacturer', 'Randomized', 'Probed SSIDs',
                'Probe Count', 'Signal (dBm)', 'First Seen', 'Last Seen']

    def __init__(self, parent=None):
        super().__init__(parent)
        self._df = pd.DataFrame()

    def rowCount(self, parent=QModelIndex()):
        if parent.isValid():
            return 0
        return len(self._df)

    def columnCount(self, parent=QModelIndex()):
        if parent.isValid():
            return 0
        return len(self._COLUMNS)

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid():
            return None
        row, col = index.row(), index.column()
        if row >= len(self._df):
            return None
        col_name = self._COLUMNS[col]
        value = self._df.iloc[row].get(col_name)

        if role == Qt.ItemDataRole.DisplayRole:
            if col_name == 'randomized':
                return "Yes" if value else "No"
            if col_name in ('first_seen', 'last_seen'):
                return str(value)[:19] if pd.notna(value) else ''
            if col_name == 'strongest_signal':
                return str(int(value)) if pd.notna(value) else ''
            if not isinstance(value, bool) and pd.isna(value):
                return ''
            return str(value)

        elif role == Qt.ItemDataRole.ForegroundRole:
            if col_name == 'randomized' and value:
                return QBrush(QColor(230, 126, 34))

        elif role == Qt.ItemDataRole.ToolTipRole:
            if col_name == 'probed_ssids' and value:
                ssids = str(value).split(', ')
                if len(ssids) > 3:
                    return '\n'.join(ssids)
            if isinstance(value, bool) or pd.notna(value):
                return str(value)
            return ''

        elif role == Qt.ItemDataRole.UserRole:
            # Return raw value for sorting
            if col_name == 'strongest_signal':
                return int(value) if pd.notna(value) else -999
            if col_name == 'probe_count':
                return int(value) if pd.notna(value) else 0
            if col_name == 'randomized':
                return 1 if value else 0
            return value

        return None

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if role == Qt.ItemDataRole.DisplayRole:
            if orientation == Qt.Orientation.Horizontal and section < len(self._HEADERS):
                return self._HEADERS[section]
            if orientation == Qt.Orientation.Vertical:
                return str(section + 1)
        return None

    def setDataFrame(self, df: pd.DataFrame):
        self.beginResetModel()
        self._df = df if df is not None else pd.DataFrame()
        self.endResetModel()

    def getDataFrame(self) -> pd.DataFrame:
        return self._df

    def getRowData(self, row: int) -> dict:
        if row < 0 or row >= len(self._df):
            return {}
        return self._df.iloc[row].to_dict()


class _ProbeSortProxy(QSortFilterProxyModel):
    """Sort proxy that uses UserRole for numeric sorting."""

    def lessThan(self, left, right):
        left_val = self.sourceModel().data(left, Qt.ItemDataRole.UserRole)
        right_val = self.sourceModel().data(right, Qt.ItemDataRole.UserRole)
        if left_val is None:
            return True
        if right_val is None:
            return False
        try:
            return left_val < right_val
        except TypeError:
            return str(left_val) < str(right_val)


# --- SSID Filter Dialog (Feature 3) ---

class _ProbeSSIDFilterDialog(QDialog):
    """Dialog to select SSIDs for daisy-chain filtering."""

    def __init__(self, ssids: list[str], parent=None):
        super().__init__(parent)
        self.setWindowTitle("Filter by Probed SSIDs")
        self.setMinimumSize(350, 400)
        self.setStyleSheet("""
            QDialog { background-color: #2b2b2b; color: #e0e0e0; }
            QLabel { color: #e0e0e0; }
            QCheckBox { color: #e0e0e0; spacing: 6px; }
            QCheckBox::indicator { width: 16px; height: 16px; }
            QPushButton { background-color: #3c3f41; color: #e0e0e0;
                          border: 1px solid #555; border-radius: 4px;
                          padding: 4px 12px; }
            QPushButton:hover { background-color: #4c5052; }
            QScrollArea { border: 1px solid #555; }
        """)

        layout = QVBoxLayout(self)

        info = QLabel("Select SSIDs to filter — shows only devices that probed for ALL selected:")
        info.setWordWrap(True)
        layout.addWidget(info)

        # Select all / none buttons
        btn_row = QHBoxLayout()
        select_all_btn = QPushButton("Select All")
        select_all_btn.clicked.connect(self._select_all)
        select_none_btn = QPushButton("Select None")
        select_none_btn.clicked.connect(self._select_none)
        btn_row.addWidget(select_all_btn)
        btn_row.addWidget(select_none_btn)
        btn_row.addStretch()
        layout.addLayout(btn_row)

        # Scrollable checkbox area
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll_widget = QWidget()
        self._check_layout = QVBoxLayout(scroll_widget)

        self._checkboxes = []
        for ssid in sorted(ssids):
            cb = QCheckBox(ssid)
            cb.setChecked(True)
            self._checkboxes.append(cb)
            self._check_layout.addWidget(cb)
        self._check_layout.addStretch()

        scroll.setWidget(scroll_widget)
        layout.addWidget(scroll)

        # Dialog buttons
        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _select_all(self):
        for cb in self._checkboxes:
            cb.setChecked(True)

    def _select_none(self):
        for cb in self._checkboxes:
            cb.setChecked(False)

    def selected_ssids(self) -> list[str]:
        return [cb.text() for cb in self._checkboxes if cb.isChecked()]


def _make_help_button(text: str, parent=None) -> QPushButton:
    """Create a small '?' help button that shows an info dialog on click."""
    btn = QPushButton("?")
    btn.setFixedSize(24, 24)
    btn.setStyleSheet(
        "QPushButton { border: 1px solid #555; border-radius: 12px; "
        "font-weight: bold; font-size: 13px; color: #ccc; background: #3c3f41; }"
        "QPushButton:hover { background: #4c5052; }")
    btn.setToolTip("Click for help on this view")

    from PyQt6.QtWidgets import QMessageBox

    def _show():
        QMessageBox.information(parent or btn, "Help", text)

    btn.clicked.connect(_show)
    return btn


class _SummaryCard(QFrame):
    """Small summary stat card for PCAP views."""

    def __init__(self, title: str, value: str = "-", parent=None):
        super().__init__(parent)
        self.setFrameStyle(QFrame.Shape.Box | QFrame.Shadow.Raised)
        self.setStyleSheet("""
            _SummaryCard {
                background-color: #2b2b2b;
                border: 1px solid #444;
                border-radius: 6px;
                padding: 8px;
            }
        """)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 6, 10, 6)

        self.title_label = QLabel(title)
        self.title_label.setStyleSheet("color: #999; font-size: 11px;")
        layout.addWidget(self.title_label)

        self.value_label = QLabel(value)
        self.value_label.setStyleSheet("font-size: 18px; font-weight: bold; color: #e0e0e0;")
        layout.addWidget(self.value_label)

    def set_value(self, value: str):
        self.value_label.setText(value)


class HandshakeView(QWidget):
    """View showing detected WPA handshakes with progress bars and cracking."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._cracked: dict[str, str] = {}   # "bssid:client_mac" -> password
        self._pcap_path: str = ''
        self._active_worker = None
        self._progress_dialog = None
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)

        # Summary cards
        cards_layout = QHBoxLayout()
        self.complete_card = _SummaryCard("Complete (4-way)")
        self.complete_card.setToolTip(
            "Networks where all 4 EAPOL messages were captured.\n"
            "A complete handshake can be used to test password strength.")
        self.partial_card = _SummaryCard("Partial")
        self.partial_card.setToolTip(
            "Networks where some but not all 4 EAPOL\n"
            "messages were captured (e.g. messages 1,2,3).")
        self.total_card = _SummaryCard("Total Handshakes")
        self.total_card.setToolTip("Total number of unique BSSID/client handshake pairs detected.")
        self.cracked_card = _SummaryCard("Cracked")
        self.cracked_card.setToolTip("Passwords successfully cracked with hashcat.")
        cards_layout.addWidget(self.complete_card)
        cards_layout.addWidget(self.partial_card)
        cards_layout.addWidget(self.total_card)
        cards_layout.addWidget(self.cracked_card)
        cards_layout.addStretch()
        cards_layout.addWidget(_make_help_button(
            "WPA Handshakes\n\n"
            "WPA/WPA2 networks use a 4-way EAPOL handshake when a client "
            "connects. Capturing all 4 messages allows offline password "
            "testing (e.g. with hashcat or aircrack-ng).\n\n"
            "The progress bar shows how many of the 4 messages were captured:\n"
            "  25% = Message 1 only\n"
            "  50% = Messages 1 & 2\n"
            "  75% = Messages 1, 2 & 3\n"
            "  100% = Complete handshake\n\n"
            "Right-click a handshake to crack the password with hashcat.\n"
            "Requires: hashcat, hcxtools, and a wordlist (e.g. rockyou.txt).",
            self))
        layout.addLayout(cards_layout)

        # Table: SSID, Capture, Timestamp, Client MAC, Status
        self.table = QTableWidget()
        self.table.setColumnCount(5)
        self.table.setHorizontalHeaderLabels([
            'SSID', 'Capture', 'Timestamp', 'Client MAC', 'Status'
        ])
        self.table.horizontalHeader().setSectionResizeMode(
            QHeaderView.ResizeMode.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(
            0, QHeaderView.ResizeMode.Stretch)
        self.table.setAlternatingRowColors(True)
        self.table.setSelectionBehavior(
            QTableWidget.SelectionBehavior.SelectRows)
        self.table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.table.setMouseTracking(True)
        self.table.cellEntered.connect(self._show_tooltip)

        # Context menu
        self.table.setContextMenuPolicy(
            Qt.ContextMenuPolicy.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self._show_context_menu)

        layout.addWidget(self.table)

    def set_pcap_path(self, path: str):
        """Set the PCAP file path (called by main_window after loading)."""
        self._pcap_path = path

    def _show_tooltip(self, row, col):
        """Show BSSID and manufacturer info on hover."""
        data = self.table.property(f'row_data_{row}')
        if data:
            tip = (f"BSSID: {data.get('bssid', 'N/A')}\n"
                   f"Manufacturer: {data.get('client_manufacturer', 'N/A')}\n"
                   f"Messages: {data.get('messages', 'N/A')}")
            QToolTip.showText(QCursor.pos(), tip, self.table)

    def _show_context_menu(self, position):
        """Show right-click context menu."""
        row = self.table.rowAt(position.y())
        if row < 0:
            return
        data = self.table.property(f'row_data_{row}')
        if not data:
            return

        menu = QMenu(self)

        # Copy actions
        copy_bssid = QAction("Copy BSSID", self)
        copy_bssid.triggered.connect(
            lambda: QApplication.clipboard().setText(str(data.get('bssid', ''))))
        menu.addAction(copy_bssid)

        copy_mac = QAction("Copy Client MAC", self)
        copy_mac.triggered.connect(
            lambda: QApplication.clipboard().setText(str(data.get('client_mac', ''))))
        menu.addAction(copy_mac)

        menu.addSeparator()

        # Crack action
        key = f"{data.get('bssid', '')}:{data.get('client_mac', '')}"
        messages_str = str(data.get('messages', ''))
        msg_count = len([m for m in messages_str.split(',') if m.strip()]) if messages_str else 0
        pct = min(msg_count * 25, 100)

        crack_action = QAction("Crack Password...", self)

        if key in self._cracked and self._cracked[key]:
            crack_action.setText(f"Cracked: {self._cracked[key]}")
            crack_action.setEnabled(False)
        elif key in self._cracked:
            crack_action.setText("Crack Attempted (Not Found)")
            crack_action.setEnabled(False)
        elif self._active_worker is not None:
            crack_action.setText("Crack in progress...")
            crack_action.setEnabled(False)
        elif pct >= 100:
            crack_action.triggered.connect(lambda: self._start_crack(data))
        elif pct >= 75:
            crack_action.triggered.connect(
                lambda: self._start_crack(data, warn=True))
        else:
            crack_action.setText("Crack Password (insufficient capture)")
            crack_action.setEnabled(False)
            crack_action.setToolTip(
                "Need at least 75% capture (3 of 4 EAPOL messages).")

        menu.addAction(crack_action)
        menu.exec(self.table.viewport().mapToGlobal(position))

    def _start_crack(self, row_data: dict, warn: bool = False):
        """Initiate a crack operation."""
        from ui.crack_dialog import (
            check_dependencies, find_wordlist,
            CrackProgressDialog, CrackResultDialog)
        from database.hashcat_worker import HashcatWorker
        from config import DEFAULT_CONFIG
        from PyQt6.QtWidgets import QMessageBox

        # Dependency check
        ok, msg = check_dependencies()
        if not ok:
            QMessageBox.warning(self, "Missing Dependencies", msg)
            return

        # Wordlist
        wordlist = DEFAULT_CONFIG['hashcat']['wordlist_path']
        if not Path(wordlist).exists():
            found = find_wordlist()
            if found:
                wordlist = found
            else:
                # Offer to download rockyou.txt
                reply = QMessageBox.question(
                    self, "Wordlist Not Found",
                    "rockyou.txt wordlist not found.\n\n"
                    "Download it now? (~134 MB)",
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
                if reply == QMessageBox.StandardButton.Yes:
                    self._download_and_crack(row_data, warn)
                return

        # Check for .gz
        if wordlist.endswith('.gz'):
            QMessageBox.warning(
                self, "Compressed Wordlist",
                f"Wordlist is gzipped:\n{wordlist}\n\n"
                "Decompress with: gunzip " + wordlist)
            return

        # PCAP path
        if not self._pcap_path:
            QMessageBox.warning(self, "No PCAP", "No PCAP file loaded.")
            return

        # Warning for incomplete handshakes
        if warn:
            reply = QMessageBox.question(
                self, "Incomplete Handshake",
                "This handshake is incomplete (75%). Cracking may not "
                "succeed depending on which frames were captured.\n\n"
                "Try anyway?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
            if reply != QMessageBox.StandardButton.Yes:
                return

        ssid = str(row_data.get('ssid', 'Unknown'))
        bssid = str(row_data.get('bssid', ''))
        client_mac = str(row_data.get('client_mac', ''))
        use_gpu = DEFAULT_CONFIG['hashcat']['use_gpu']

        # Create worker
        self._active_worker = HashcatWorker(
            self._pcap_path, bssid, client_mac, ssid, wordlist, use_gpu)

        # Create progress dialog
        self._progress_dialog = CrackProgressDialog(ssid, bssid, self)
        self._progress_dialog.cancelled.connect(self._on_crack_cancel)

        # Wire signals
        self._active_worker.status.connect(self._progress_dialog.update_status)
        self._active_worker.progress.connect(self._progress_dialog.update_progress)
        self._active_worker.finished.connect(self._on_crack_finished)

        # Start
        self._active_worker.start()
        self._progress_dialog.show()

    def _download_and_crack(self, row_data: dict, warn: bool):
        """Download rockyou.txt then start cracking."""
        from ui.crack_dialog import WordlistDownloadDialog
        from PyQt6.QtWidgets import QMessageBox

        dlg = WordlistDownloadDialog(self)

        def on_download_done(success, result):
            if success:
                # Wordlist downloaded, now start the crack
                self._start_crack(row_data, warn)
            else:
                if result != 'Cancelled':
                    QMessageBox.warning(
                        self, "Download Failed", result)

        dlg.download_finished.connect(on_download_done)
        dlg.start_download()
        dlg.exec()

    def _on_crack_cancel(self):
        """Handle cancel from progress dialog."""
        if self._active_worker:
            self._active_worker.cancel()

    def _on_crack_finished(self, success: bool, result: str, bssid_key: str):
        """Handle crack completion."""
        from ui.crack_dialog import CrackResultDialog

        elapsed = 0
        if self._progress_dialog:
            elapsed = self._progress_dialog.elapsed_seconds()
            self._progress_dialog.finish()
            self._progress_dialog = None

        # Store result
        password = result if success else ''
        self._cracked[bssid_key] = password

        # Update table
        self._update_cracked_status()

        # Show result dialog
        ssid = bssid_key.split(':')[0] if ':' in bssid_key else ''
        # Look up SSID from table data
        for row_idx in range(self.table.rowCount()):
            data = self.table.property(f'row_data_{row_idx}')
            if data and f"{data.get('bssid', '')}:{data.get('client_mac', '')}" == bssid_key:
                ssid = str(data.get('ssid', ''))
                break

        bssid = bssid_key.split(':')[0] if ':' in bssid_key else bssid_key

        if success:
            dlg = CrackResultDialog(ssid, bssid, password, elapsed, parent=self)
        else:
            dlg = CrackResultDialog(ssid, bssid, None, elapsed,
                                    error=result if result != "Password not found in wordlist" else None,
                                    parent=self)
        dlg.exec()

        self._active_worker = None

    def _update_cracked_status(self):
        """Update the Status column for all rows based on _cracked dict."""
        cracked_count = sum(1 for v in self._cracked.values() if v)
        self.cracked_card.set_value(str(cracked_count))
        if cracked_count > 0:
            self.cracked_card.value_label.setStyleSheet(
                "font-size: 18px; font-weight: bold; color: #2ecc71;")

        for row_idx in range(self.table.rowCount()):
            data = self.table.property(f'row_data_{row_idx}')
            if not data:
                continue
            key = f"{data.get('bssid', '')}:{data.get('client_mac', '')}"
            if key in self._cracked:
                pw = self._cracked[key]
                if pw:
                    item = QTableWidgetItem(f"\U0001F513 {pw}")
                    item.setForeground(QBrush(QColor(46, 204, 113)))
                else:
                    item = QTableWidgetItem("Not Found")
                    item.setForeground(QBrush(QColor(243, 156, 18)))
                self.table.setItem(row_idx, 4, item)

    @staticmethod
    def _progress_color(pct: int) -> str:
        """Return stylesheet chunk color for given percentage."""
        if pct <= 25:
            return '#e74c3c'   # red
        elif pct <= 50:
            return '#f39c12'   # amber
        elif pct <= 75:
            return '#a8d65c'   # yellow-green
        else:
            return '#2ecc71'   # green

    def load_data(self, df: pd.DataFrame):
        """Load handshake data into the view with progress bars."""
        if df.empty:
            self.table.setRowCount(0)
            self.complete_card.set_value("0")
            self.partial_card.set_value("0")
            self.total_card.set_value("0")
            self.cracked_card.set_value("0")
            return

        complete = df[df['complete'] == True] if 'complete' in df.columns else pd.DataFrame()
        partial = df[df['complete'] == False] if 'complete' in df.columns else pd.DataFrame()

        self.complete_card.set_value(str(len(complete)))
        self.partial_card.set_value(str(len(partial)))
        self.total_card.set_value(str(len(df)))

        # Sort: cracked first, then complete, then by message count desc, then timestamp
        if 'complete' in df.columns:
            df = df.copy()
            df['_cracked'] = df.apply(
                lambda r: 1 if f"{r.get('bssid', '')}:{r.get('client_mac', '')}" in self._cracked
                and self._cracked[f"{r.get('bssid', '')}:{r.get('client_mac', '')}"] else 0,
                axis=1)
            df['_msg_count'] = df['messages'].apply(
                lambda m: len([x for x in str(m).split(',') if x.strip()]) if pd.notna(m) else 0)
            df = df.sort_values(
                ['_cracked', 'complete', '_msg_count', 'timestamp'],
                ascending=[False, False, False, False])

        self.table.setRowCount(len(df))
        for row_idx, (_, row) in enumerate(df.iterrows()):
            # Store full row data for tooltip
            self.table.setProperty(f'row_data_{row_idx}', row.to_dict())

            # SSID
            self.table.setItem(row_idx, 0, QTableWidgetItem(str(row.get('ssid', ''))))

            # Capture progress bar
            messages_str = str(row.get('messages', ''))
            msg_count = len([m for m in messages_str.split(',') if m.strip()]) if messages_str else 0
            pct = min(msg_count * 25, 100)
            color = self._progress_color(pct)

            progress = QProgressBar()
            progress.setRange(0, 100)
            progress.setValue(pct)
            progress.setFormat(f"{pct}%")
            progress.setTextVisible(True)
            progress.setFixedHeight(22)
            progress.setStyleSheet(f"""
                QProgressBar {{
                    border: 1px solid #555;
                    border-radius: 3px;
                    text-align: center;
                    background-color: #2b2b2b;
                    color: #e0e0e0;
                }}
                QProgressBar::chunk {{
                    background-color: {color};
                    border-radius: 2px;
                }}
            """)
            self.table.setCellWidget(row_idx, 1, progress)

            # Timestamp
            ts = row.get('timestamp', '')
            ts_str = str(ts)[:19] if pd.notna(ts) else ''
            self.table.setItem(row_idx, 2, QTableWidgetItem(ts_str))

            # Client MAC
            self.table.setItem(row_idx, 3, QTableWidgetItem(str(row.get('client_mac', ''))))

            # Status (cracked indicator)
            key = f"{row.get('bssid', '')}:{row.get('client_mac', '')}"
            if key in self._cracked:
                pw = self._cracked[key]
                if pw:
                    item = QTableWidgetItem(f"\U0001F513 {pw}")
                    item.setForeground(QBrush(QColor(46, 204, 113)))
                else:
                    item = QTableWidgetItem("Not Found")
                    item.setForeground(QBrush(QColor(243, 156, 18)))
                self.table.setItem(row_idx, 4, item)

        self._update_cracked_status()


class DeauthView(QWidget):
    """View showing deauthentication frame analysis with smart categorization."""

    # Reason codes that indicate normal client departure
    _CLIENT_DEPARTURE_REASONS = {3, 8}  # 3=STA deauth leaving, 8=STA disassoc leaving

    def __init__(self, parent=None):
        super().__init__(parent)
        self._df = pd.DataFrame()
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)

        # Summary cards: 3 categories
        cards_layout = QHBoxLayout()
        self.steering_card = _SummaryCard("Band Steering")
        self.steering_card.setToolTip(
            "Deauth frames sent by known access points.\n"
            "Usually part of band steering (pushing clients to 5GHz)\n"
            "or AP load balancing — completely normal.")
        self.departure_card = _SummaryCard("Client Departures")
        self.departure_card.setToolTip(
            "Deauth frames from known clients voluntarily\n"
            "leaving the network (reason codes 3 or 8).")
        self.attack_card = _SummaryCard("Deauth Attack")
        self.attack_card.setToolTip(
            "Deauth frames from unknown sources.\n"
            "High counts may indicate a deauthentication attack\n"
            "or rogue device (e.g. WiFi Pineapple, MDK3).")
        cards_layout.addWidget(self.steering_card)
        cards_layout.addWidget(self.departure_card)
        cards_layout.addWidget(self.attack_card)
        cards_layout.addStretch()
        cards_layout.addWidget(_make_help_button(
            "Deauthentication Analysis\n\n"
            "Deauth/disassociation frames are categorized into 3 groups:\n\n"
            "Band Steering: Sent by known access points — usually pushing "
            "clients from 2.4GHz to 5GHz. Completely normal.\n\n"
            "Client Departures: Sent by known clients voluntarily "
            "disconnecting from the network.\n\n"
            "Deauth Attack: Frames from unknown sources. High counts may "
            "indicate an active attack (e.g. WiFi Pineapple, MDK3, aireplay-ng). "
            "The timeline chart shows when attacks occurred.\n\n"
            "Click 'Show Raw Data' to see individual frames with reason codes.",
            self))
        layout.addLayout(cards_layout)

        # Splitter: chart on top, table toggle on bottom
        splitter = QSplitter(Qt.Orientation.Vertical)

        # Deauth timeline chart (with DateAxisItem for readable timestamps)
        if HAS_PYQTGRAPH:
            chart_frame = QFrame()
            chart_frame.setFrameStyle(QFrame.Shape.Box | QFrame.Shadow.Raised)
            chart_layout = QVBoxLayout(chart_frame)
            chart_label = QLabel("Deauth Frequency Over Time")
            chart_label.setStyleSheet("font-weight: bold;")
            chart_layout.addWidget(chart_label)

            date_axis = pg.DateAxisItem(orientation='bottom')
            self.timeline_plot = pg.PlotWidget(axisItems={'bottom': date_axis})
            self.timeline_plot.setBackground('#2b2b2b')
            self.timeline_plot.setLabel('left', 'Count')
            self.timeline_plot.showGrid(x=True, y=True, alpha=0.3)
            self.timeline_plot.addLegend(offset=(10, 10))
            chart_layout.addWidget(self.timeline_plot)
            splitter.addWidget(chart_frame)
        else:
            self.timeline_plot = None

        # Table container with toggle button
        table_container = QWidget()
        table_layout = QVBoxLayout(table_container)
        table_layout.setContentsMargins(0, 0, 0, 0)

        self._toggle_btn = QPushButton("Show Raw Data")
        self._toggle_btn.setCheckable(True)
        self._toggle_btn.clicked.connect(self._toggle_table)
        self._toggle_btn.setMaximumWidth(150)
        table_layout.addWidget(self._toggle_btn)

        # Table
        self.table = QTableWidget()
        self.table.setColumnCount(8)
        self.table.setHorizontalHeaderLabels([
            'Timestamp', 'Source MAC', 'Dest MAC', 'BSSID',
            'Reason Code', 'Reason', 'Type', 'Category'
        ])
        self.table.horizontalHeader().setSectionResizeMode(
            QHeaderView.ResizeMode.ResizeToContents
        )
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setAlternatingRowColors(True)
        self.table.setSelectionBehavior(
            QTableWidget.SelectionBehavior.SelectRows
        )
        self.table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.table.setVisible(False)
        table_layout.addWidget(self.table)

        splitter.addWidget(table_container)
        splitter.setSizes([500, 200])
        layout.addWidget(splitter)

    def _toggle_table(self):
        """Toggle raw data table visibility."""
        visible = self._toggle_btn.isChecked()
        self.table.setVisible(visible)
        self._toggle_btn.setText("Hide Raw Data" if visible else "Show Raw Data")

    def load_data(self, df: pd.DataFrame, ap_df: pd.DataFrame = None,
                  client_df: pd.DataFrame = None):
        """Load deauth data with smart 3-category classification."""
        total_count = df.attrs.get('total_deauth_count', len(df)) if not df.empty else 0

        if df.empty:
            self.table.setRowCount(0)
            self.steering_card.set_value("0")
            self.departure_card.set_value("0")
            self.attack_card.set_value("0")
            self.attack_card.value_label.setStyleSheet(
                "font-size: 18px; font-weight: bold; color: #2ecc71;")
            if self.timeline_plot:
                self.timeline_plot.clear()
            return

        # Build known MAC sets
        ap_bssids = set()
        if ap_df is not None and not ap_df.empty and 'devmac' in ap_df.columns:
            ap_bssids = set(ap_df['devmac'].str.lower())

        client_macs = set()
        if client_df is not None and not client_df.empty and 'client_mac' in client_df.columns:
            client_macs = set(client_df['client_mac'].str.lower())

        # --- 3-category classification ---
        categories = []
        for _, row in df.iterrows():
            src = str(row.get('source_mac', '')).lower()
            reason = int(row.get('reason_code', 0)) if pd.notna(row.get('reason_code')) else 0

            if src in ap_bssids:
                categories.append('Band Steering')
            elif src in client_macs and reason in self._CLIENT_DEPARTURE_REASONS:
                categories.append('Client Departure')
            elif src in client_macs:
                # Known client but unusual reason — still likely benign
                categories.append('Client Departure')
            else:
                categories.append('External Source')

        df = df.copy()
        df['category'] = categories

        # Flood detection: any source sending 100+ deauths/sec
        if 'timestamp' in df.columns and not df.empty:
            ts_unix = df['timestamp'].astype('int64') // 10**9
            df['_ts_sec'] = ts_unix
            flood_groups = df.groupby(['source_mac', '_ts_sec']).size()
            flood_sources = set()
            for (src, _sec), count in flood_groups.items():
                if count >= 100:
                    flood_sources.add(src)
            if flood_sources:
                df.loc[df['source_mac'].str.lower().isin(flood_sources), 'category'] = 'Flood'
            df.drop(columns=['_ts_sec'], inplace=True)

        self._df = df

        # --- Update summary cards ---
        steering_count = len(df[df['category'] == 'Band Steering'])
        departure_count = len(df[df['category'] == 'Client Departure'])
        attack_count = len(df[df['category'].isin(['External Source', 'Flood'])])

        self.steering_card.set_value(f"{steering_count:,}")
        self.steering_card.value_label.setStyleSheet(
            "font-size: 18px; font-weight: bold; color: #3498db;")

        self.departure_card.set_value(f"{departure_count:,}")
        self.departure_card.value_label.setStyleSheet(
            "font-size: 18px; font-weight: bold; color: #95a5a6;")

        self.attack_card.set_value(f"{attack_count:,}")
        if attack_count > 0:
            self.attack_card.value_label.setStyleSheet(
                "font-size: 18px; font-weight: bold; color: #e74c3c;")
        else:
            self.attack_card.value_label.setStyleSheet(
                "font-size: 18px; font-weight: bold; color: #2ecc71;")

        # --- Timeline: color-coded, uses DateAxisItem for readable timestamps ---
        if self.timeline_plot is not None and 'timestamp' in df.columns:
            self.timeline_plot.clear()
            df_sorted = df.sort_values('timestamp')
            ts_series = df_sorted['timestamp'].astype('int64') // 10**9

            if not ts_series.empty:
                min_ts = float(ts_series.min())
                max_ts = float(ts_series.max())
                duration = max(max_ts - min_ts, 1)

                benign_mask = df_sorted['category'].isin(['Band Steering', 'Client Departure'])
                attack_mask = ~benign_mask

                if len(df_sorted) <= 50:
                    # Sparse data: scatter plot — individual event markers
                    benign_ts = ts_series[benign_mask].values.astype(float)
                    attack_ts = ts_series[attack_mask].values.astype(float)

                    if len(benign_ts) > 0:
                        self.timeline_plot.plot(
                            benign_ts, np.ones(len(benign_ts)),
                            pen=None, symbol='o', symbolSize=10,
                            symbolBrush='#3498db', name='Benign')
                    if len(attack_ts) > 0:
                        self.timeline_plot.plot(
                            attack_ts, np.ones(len(attack_ts)) * 2,
                            pen=None, symbol='o', symbolSize=10,
                            symbolBrush='#e74c3c', name='Attack')

                    padding = max(duration * 0.05, 60)
                    self.timeline_plot.setXRange(min_ts - padding, max_ts + padding, padding=0)
                    self.timeline_plot.setYRange(0, 3, padding=0)
                    self.timeline_plot.setLabel('left', 'Events')
                else:
                    # Dense data: binned bar chart
                    if duration < 300:
                        bin_size = 1
                    elif duration < 3600:
                        bin_size = 10
                    else:
                        bin_size = 60

                    benign_bins = (ts_series[benign_mask] // bin_size).value_counts().sort_index()
                    attack_bins = (ts_series[attack_mask] // bin_size).value_counts().sort_index()

                    all_keys = sorted(set(benign_bins.index) | set(attack_bins.index))
                    benign_y = np.array([benign_bins.get(k, 0) for k in all_keys])
                    attack_y = np.array([attack_bins.get(k, 0) for k in all_keys])
                    x = np.array([k * bin_size for k in all_keys], dtype=float)

                    if len(x) > 0:
                        bg_benign = pg.BarGraphItem(
                            x=x, height=benign_y, width=bin_size * 0.8,
                            brush='#3498db', name='Benign')
                        self.timeline_plot.addItem(bg_benign)

                        bg_attack = pg.BarGraphItem(
                            x=x, height=attack_y, width=bin_size * 0.8,
                            y0=benign_y, brush='#e74c3c', name='Attack')
                        self.timeline_plot.addItem(bg_attack)

                        padding = bin_size * 2
                        self.timeline_plot.setXRange(
                            float(x.min()) - padding, float(x.max()) + padding, padding=0)
                        y_max = float(max((benign_y + attack_y).max(), 1)) * 1.1
                        self.timeline_plot.setYRange(0, y_max, padding=0)
                    self.timeline_plot.setLabel('left', 'Count')

        # --- Populate table (cap display at 5000 rows) ---
        display_df = df.head(5000)
        self.table.setRowCount(len(display_df))
        for row_idx, (_, row) in enumerate(display_df.iterrows()):
            ts = row.get('timestamp', '')
            ts_str = str(ts)[:19] if pd.notna(ts) else ''
            self.table.setItem(row_idx, 0, QTableWidgetItem(ts_str))
            self.table.setItem(row_idx, 1, QTableWidgetItem(str(row.get('source_mac', ''))))
            self.table.setItem(row_idx, 2, QTableWidgetItem(str(row.get('dest_mac', ''))))
            self.table.setItem(row_idx, 3, QTableWidgetItem(str(row.get('bssid', ''))))
            self.table.setItem(row_idx, 4, QTableWidgetItem(str(row.get('reason_code', ''))))
            self.table.setItem(row_idx, 5, QTableWidgetItem(str(row.get('reason_text', ''))))
            self.table.setItem(row_idx, 6, QTableWidgetItem(str(row.get('subtype', ''))))

            cat = str(row.get('category', ''))
            cat_item = QTableWidgetItem(cat)
            if cat in ('External Source', 'Flood'):
                cat_item.setForeground(QBrush(QColor(231, 76, 60)))
            else:
                cat_item.setForeground(QBrush(QColor(46, 204, 113)))
            self.table.setItem(row_idx, 7, cat_item)

        if len(df) > 5000:
            self.table.setRowCount(len(display_df) + 1)
            note = QTableWidgetItem(f"... {len(df) - 5000:,} more rows (capped for display)")
            note.setForeground(QBrush(QColor(150, 150, 150)))
            self.table.setItem(5000, 0, note)


class ProbeMapView(QWidget):
    """View showing probe request analysis with search, filters, and context menus."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._full_df = pd.DataFrame()
        self._filtered_df = pd.DataFrame()
        self._ssid_filter_active = []  # Active SSID daisy-chain filter
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)

        # Summary cards
        cards_layout = QHBoxLayout()
        self.clients_card = _SummaryCard("Probing Clients")
        self.clients_card.setToolTip("Number of unique devices that sent probe requests.")
        self.ssids_card = _SummaryCard("Unique SSIDs")
        self.ssids_card.setToolTip("Number of unique network names probed for (excluding broadcast).")
        self.random_card = _SummaryCard("Randomized MACs")
        self.random_card.setToolTip(
            "Percentage of devices using randomized MAC addresses.\n"
            "Modern devices randomize MACs for privacy when probing.")
        self.total_card = _SummaryCard("Devices w/ 2+ SSIDs")
        self.total_card.setToolTip(
            "Devices that probed for multiple named networks.\n"
            "Useful for device fingerprinting and tracking.")
        cards_layout.addWidget(self.clients_card)
        cards_layout.addWidget(self.ssids_card)
        cards_layout.addWidget(self.random_card)
        cards_layout.addWidget(self.total_card)
        cards_layout.addStretch()
        cards_layout.addWidget(_make_help_button(
            "Probe Request Analysis\n\n"
            "When a Wi-Fi device searches for networks, it sends probe "
            "requests that reveal the network names it's looking for. "
            "This exposes a device's connection history.\n\n"
            "Investigation tips:\n"
            "- Use 'Min SSIDs' to find devices probing for many networks\n"
            "- Right-click > 'Filter by Probed SSIDs' to find devices "
            "with overlapping networks (daisy-chain analysis)\n"
            "- Fuzzy mode matches variants like MyNetwork_5G and MyNetwork_2G\n"
            "- Randomized MACs hide the real device — but probed SSIDs "
            "can still fingerprint it\n\n"
            "Switch to 'Advanced' mode for signal, manufacturer, "
            "and randomized MAC filters.",
            self))
        layout.addLayout(cards_layout)

        # --- Filter bar (single row, Simple/Advanced toggle inline) ---
        filter_layout = QHBoxLayout()
        filter_layout.setContentsMargins(0, 2, 0, 2)

        # Mode toggle
        self._filter_mode = QComboBox()
        self._filter_mode.addItems(["Simple", "Advanced"])
        self._filter_mode.currentIndexChanged.connect(self._on_filter_mode_changed)
        self._filter_mode.setMaximumWidth(90)
        filter_layout.addWidget(self._filter_mode)

        # SSID search (shared)
        filter_layout.addWidget(QLabel("SSID:"))
        self._ssid_search = QLineEdit()
        self._ssid_search.setPlaceholderText("Search SSIDs...")
        self._ssid_search.returnPressed.connect(self._apply_filters)
        filter_layout.addWidget(self._ssid_search, 2)

        self._match_mode = QComboBox()
        self._match_mode.addItems(["Exact", "Fuzzy"])
        self._match_mode.setMaximumWidth(70)
        self._match_mode.setToolTip(
            "Exact: case-insensitive substring match.\n"
            "Fuzzy: ignores common suffixes like _5G, _2G, -guest, _ext, etc.")
        filter_layout.addWidget(self._match_mode)

        # Min named SSIDs (Feature 7)
        filter_layout.addWidget(QLabel("Min SSIDs:"))
        self._min_ssids = QSpinBox()
        self._min_ssids.setRange(0, 99)
        self._min_ssids.setValue(0)
        self._min_ssids.setMaximumWidth(55)
        self._min_ssids.setToolTip(
            "Only show devices that probed for at least this\n"
            "many named SSIDs (excludes broadcast probes).")
        filter_layout.addWidget(self._min_ssids)

        # Time range
        self._time_label_from = QLabel("From:")
        filter_layout.addWidget(self._time_label_from)
        self._time_start = QDateTimeEdit()
        self._time_start.setCalendarPopup(True)
        self._time_start.setDisplayFormat("yyyy-MM-dd HH:mm")
        filter_layout.addWidget(self._time_start)

        self._time_label_to = QLabel("To:")
        filter_layout.addWidget(self._time_label_to)
        self._time_end = QDateTimeEdit()
        self._time_end.setCalendarPopup(True)
        self._time_end.setDisplayFormat("yyyy-MM-dd HH:mm")
        filter_layout.addWidget(self._time_end)

        # Advanced-only widgets (hidden in Simple mode)
        self._adv_widgets = []

        sig_label = QLabel("Sig:")
        filter_layout.addWidget(sig_label)
        self._adv_widgets.append(sig_label)

        self._sig_min = QSpinBox()
        self._sig_min.setRange(-100, 0)
        self._sig_min.setValue(-100)
        self._sig_min.setSuffix("dBm")
        self._sig_min.setMaximumWidth(75)
        filter_layout.addWidget(self._sig_min)
        self._adv_widgets.append(self._sig_min)

        self._sig_max = QSpinBox()
        self._sig_max.setRange(-100, 0)
        self._sig_max.setValue(0)
        self._sig_max.setSuffix("dBm")
        self._sig_max.setMaximumWidth(75)
        filter_layout.addWidget(self._sig_max)
        self._adv_widgets.append(self._sig_max)

        mfr_label = QLabel("Mfr:")
        filter_layout.addWidget(mfr_label)
        self._adv_widgets.append(mfr_label)

        self._adv_manufacturer = QLineEdit()
        self._adv_manufacturer.setPlaceholderText("Manufacturer...")
        self._adv_manufacturer.setMaximumWidth(100)
        filter_layout.addWidget(self._adv_manufacturer)
        self._adv_widgets.append(self._adv_manufacturer)

        rand_label = QLabel("Rand:")
        filter_layout.addWidget(rand_label)
        self._adv_widgets.append(rand_label)

        self._adv_randomized = QComboBox()
        self._adv_randomized.addItems(["All", "Yes", "No"])
        self._adv_randomized.setMaximumWidth(60)
        filter_layout.addWidget(self._adv_randomized)
        self._adv_widgets.append(self._adv_randomized)

        # Hide advanced widgets by default
        for w in self._adv_widgets:
            w.setVisible(False)

        # Buttons
        apply_btn = QPushButton("Apply")
        apply_btn.clicked.connect(self._apply_filters)
        apply_btn.setMaximumWidth(55)
        filter_layout.addWidget(apply_btn)

        clear_btn = QPushButton("Clear")
        clear_btn.clicked.connect(self._clear_filters)
        clear_btn.setMaximumWidth(50)
        filter_layout.addWidget(clear_btn)

        layout.addLayout(filter_layout)

        # Row count / active filter label
        info_layout = QHBoxLayout()
        self._row_count_label = QLabel("")
        info_layout.addWidget(self._row_count_label)
        self._filter_label = QLabel("")
        self._filter_label.setStyleSheet("color: #f39c12; font-weight: bold;")
        info_layout.addWidget(self._filter_label)
        info_layout.addStretch()
        layout.addLayout(info_layout)

        # Table (QTableView)
        self.table = QTableView()
        self.table.setAlternatingRowColors(True)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.table.setSortingEnabled(True)
        self.table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self._show_context_menu)

        header = self.table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        header.setStretchLastSection(True)
        header.setSortIndicatorShown(True)

        self._source_model = _ProbeTableModel()
        self._proxy_model = _ProbeSortProxy()
        self._proxy_model.setSourceModel(self._source_model)
        self.table.setModel(self._proxy_model)
        layout.addWidget(self.table)

    def _on_filter_mode_changed(self, index):
        """Show/hide advanced filter widgets."""
        advanced = index == 1
        for w in self._adv_widgets:
            w.setVisible(advanced)

    # --- Data loading ---

    def load_data(self, df: pd.DataFrame):
        """Load probe request data into the view."""
        self._full_df = df if df is not None else pd.DataFrame()
        self._ssid_filter_active = []
        self._filter_label.setText("")

        if df is None or df.empty:
            self._source_model.setDataFrame(pd.DataFrame())
            self._update_cards(pd.DataFrame())
            self._row_count_label.setText("0 rows")
            return

        # Auto-populate time range bounds (use epoch seconds for consistency)
        if 'first_seen' in df.columns:
            min_time = df['first_seen'].min()
            max_time = df['last_seen'].max() if 'last_seen' in df.columns else min_time
            if pd.notna(min_time):
                epoch = int(min_time.value // 10**9)  # nanoseconds to seconds
                self._time_start.setDateTime(QDateTime.fromSecsSinceEpoch(epoch))
            if pd.notna(max_time):
                epoch = int(max_time.value // 10**9)
                self._time_end.setDateTime(QDateTime.fromSecsSinceEpoch(epoch))

        self._apply_and_display(df)

    def _apply_and_display(self, df: pd.DataFrame):
        """Update table and cards with given DataFrame."""
        self._filtered_df = df
        self._source_model.setDataFrame(df)
        self._update_cards(df)
        self.table.resizeColumnsToContents()
        # Stretch the Probed SSIDs column
        header = self.table.horizontalHeader()
        if header.count() > 3:
            header.setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        self._row_count_label.setText(
            f"{len(df):,} rows" if len(df) == len(self._full_df)
            else f"{len(df):,} of {len(self._full_df):,} rows"
        )

    def _update_cards(self, df: pd.DataFrame):
        """Update summary cards from DataFrame."""
        if df.empty:
            self.clients_card.set_value("0")
            self.ssids_card.set_value("0")
            self.random_card.set_value("0%")
            self.total_card.set_value("0")
            return

        total_clients = len(df)
        self.clients_card.set_value(f"{total_clients:,}")

        all_ssids = set()
        devices_with_2plus = 0
        for ssids_str in df['probed_ssids'].dropna():
            if ssids_str:
                named = [s for s in ssids_str.split(', ') if s and s != '<broadcast>']
                all_ssids.update(named)
                if len(named) >= 2:
                    devices_with_2plus += 1
        self.ssids_card.set_value(f"{len(all_ssids):,}")

        # Randomized MACs as percentage
        random_count = int(df['randomized'].sum()) if 'randomized' in df.columns else 0
        if total_clients > 0:
            pct = round(random_count / total_clients * 100)
            self.random_card.set_value(f"{pct}%")
        else:
            self.random_card.set_value("0%")

        # Devices with 2+ named SSIDs (investigation-worthy)
        self.total_card.set_value(f"{devices_with_2plus:,}")

    # --- Filtering (Features 1 & 2) ---

    def _apply_filters(self):
        """Apply filters to the full DataFrame."""
        if self._full_df.empty:
            return

        df = self._full_df.copy()
        is_advanced = self._filter_mode.currentIndex() == 1

        # SSID search
        ssid_text = self._ssid_search.text().strip()
        fuzzy = self._match_mode.currentIndex() == 1

        if ssid_text:
            df = self._filter_by_ssid(df, ssid_text, fuzzy)

        # Minimum named SSIDs filter (Feature 7)
        min_ssids = self._min_ssids.value()
        if min_ssids > 0 and 'probed_ssids' in df.columns:
            def _count_named(ssids_str):
                if not ssids_str or pd.isna(ssids_str):
                    return 0
                return len([s for s in str(ssids_str).split(',')
                           if s.strip() and s.strip() != '<broadcast>'])
            df = df[df['probed_ssids'].apply(_count_named) >= min_ssids]

        # Time range — use epoch seconds to avoid tz-aware/naive mismatch
        if 'first_seen' in df.columns:
            try:
                start_ts = pd.Timestamp(
                    self._time_start.dateTime().toSecsSinceEpoch(), unit='s')
                end_ts = pd.Timestamp(
                    self._time_end.dateTime().toSecsSinceEpoch(), unit='s')
                df = df[(df['first_seen'] >= start_ts) | (df['last_seen'] >= start_ts)]
                df = df[(df['last_seen'] <= end_ts) | (df['first_seen'] <= end_ts)]
            except (TypeError, ValueError):
                pass  # Skip time filter on comparison errors

        # Advanced-only filters
        if is_advanced:
            sig_min = self._sig_min.value()
            sig_max = self._sig_max.value()
            if 'strongest_signal' in df.columns:
                df = df[df['strongest_signal'].fillna(-100).between(sig_min, sig_max)]

            mfr = self._adv_manufacturer.text().strip()
            if mfr and 'manufacturer' in df.columns:
                df = df[df['manufacturer'].str.contains(mfr, case=False, na=False)]

            rand_filter = self._adv_randomized.currentText()
            if rand_filter != "All" and 'randomized' in df.columns:
                df = df[df['randomized'] == (rand_filter == "Yes")]

        # Also apply active SSID daisy-chain filter
        if self._ssid_filter_active:
            df = self._apply_ssid_daisy_chain(df, self._ssid_filter_active)

        self._apply_and_display(df.reset_index(drop=True))

    def _filter_by_ssid(self, df: pd.DataFrame, search: str, fuzzy: bool) -> pd.DataFrame:
        """Filter DataFrame rows by SSID search term."""
        search_lower = search.lower()
        search_base = _strip_ssid_suffixes(search).lower() if fuzzy else None
        matches = []

        for idx in df.index:
            ssids_str = df.at[idx, 'probed_ssids']
            if pd.isna(ssids_str) or not ssids_str:
                continue
            ssids = [s.strip() for s in str(ssids_str).split(',')]
            for ssid in ssids:
                if fuzzy:
                    if _strip_ssid_suffixes(ssid).lower() == search_base:
                        matches.append(idx)
                        break
                else:
                    if search_lower in ssid.lower():
                        matches.append(idx)
                        break
        return df.loc[matches] if matches else df.iloc[0:0]

    def _apply_ssid_daisy_chain(self, df: pd.DataFrame, required_ssids: list[str]) -> pd.DataFrame:
        """Filter to rows that probed for ALL required SSIDs."""
        required = set(s.lower() for s in required_ssids)
        matches = []
        for idx in df.index:
            ssids_str = df.at[idx, 'probed_ssids']
            if pd.isna(ssids_str) or not ssids_str:
                continue
            row_ssids = set(s.strip().lower() for s in str(ssids_str).split(','))
            if required.issubset(row_ssids):
                matches.append(idx)
        return df.loc[matches] if matches else df.iloc[0:0]

    def _clear_filters(self):
        """Reset all filters and show full data."""
        self._ssid_search.clear()
        self._adv_manufacturer.clear()
        self._sig_min.setValue(-100)
        self._sig_max.setValue(0)
        self._adv_randomized.setCurrentIndex(0)
        self._match_mode.setCurrentIndex(0)
        self._min_ssids.setValue(0)
        self._ssid_filter_active = []
        self._filter_label.setText("")

        if not self._full_df.empty:
            if 'first_seen' in self._full_df.columns:
                min_time = self._full_df['first_seen'].min()
                max_time = self._full_df['last_seen'].max()
                if pd.notna(min_time):
                    epoch = int(min_time.value // 10**9)
                    self._time_start.setDateTime(QDateTime.fromSecsSinceEpoch(epoch))
                if pd.notna(max_time):
                    epoch = int(max_time.value // 10**9)
                    self._time_end.setDateTime(QDateTime.fromSecsSinceEpoch(epoch))

        self._apply_and_display(self._full_df)

    # --- Context menu (Feature 3) ---

    def _show_context_menu(self, position):
        """Show right-click context menu."""
        index = self.table.indexAt(position)
        if not index.isValid():
            return

        menu = QMenu(self)

        copy_mac = QAction("Copy MAC Address", self)
        copy_mac.triggered.connect(lambda: self._copy_mac(index))
        menu.addAction(copy_mac)

        copy_cell = QAction("Copy Cell Value", self)
        copy_cell.triggered.connect(lambda: self._copy_cell(index))
        menu.addAction(copy_cell)

        copy_row = QAction("Copy Row", self)
        copy_row.triggered.connect(lambda: self._copy_row(index))
        menu.addAction(copy_row)

        menu.addSeparator()

        filter_ssids = QAction("Filter by Probed SSIDs...", self)
        filter_ssids.triggered.connect(lambda: self._open_ssid_filter_dialog(index))
        menu.addAction(filter_ssids)

        menu.addSeparator()

        export_sel = QAction("Export Selected to CSV...", self)
        export_sel.triggered.connect(self._export_selected_csv)
        menu.addAction(export_sel)

        menu.exec(self.table.viewport().mapToGlobal(position))

    def _get_source_row(self, proxy_index) -> dict:
        """Map proxy index to source row data."""
        source_index = self._proxy_model.mapToSource(proxy_index)
        return self._source_model.getRowData(source_index.row())

    def _copy_mac(self, index):
        row_data = self._get_source_row(index)
        mac = row_data.get('client_mac', '')
        if mac:
            QApplication.clipboard().setText(str(mac))

    def _copy_cell(self, index):
        value = self._proxy_model.data(index, Qt.ItemDataRole.DisplayRole)
        if value:
            QApplication.clipboard().setText(str(value))

    def _copy_row(self, index):
        row_data = self._get_source_row(index)
        if row_data:
            values = [str(v) for v in row_data.values()]
            QApplication.clipboard().setText('\t'.join(values))

    def _open_ssid_filter_dialog(self, index):
        """Open the SSID selection dialog for daisy-chain filtering."""
        row_data = self._get_source_row(index)
        ssids_str = row_data.get('probed_ssids', '')
        if not ssids_str:
            return
        ssids = [s.strip() for s in str(ssids_str).split(',') if s.strip() and s.strip() != '<broadcast>']
        if not ssids:
            return

        dialog = _ProbeSSIDFilterDialog(ssids, self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            selected = dialog.selected_ssids()
            if selected:
                self._ssid_filter_active = selected
                self._filter_label.setText(f"SSID filter: {', '.join(selected[:3])}{'...' if len(selected) > 3 else ''}")
                self._apply_filters()

    def _export_selected_csv(self):
        """Export selected rows to CSV."""
        selection = self.table.selectionModel().selectedRows()
        if not selection:
            return

        rows = []
        for proxy_index in selection:
            source_index = self._proxy_model.mapToSource(proxy_index)
            rows.append(self._source_model.getRowData(source_index.row()))

        if not rows:
            return

        path, _ = QFileDialog.getSaveFileName(self, "Export Selected Probes", "", "CSV Files (*.csv)")
        if path:
            export_df = pd.DataFrame(rows)
            export_df.to_csv(path, index=False)
            QApplication.instance().activeWindow().statusBar().showMessage(
                f"Exported {len(rows)} rows to {path}", 5000
            )


class FrameTypeView(QWidget):
    """View showing frame type distribution from PCAP."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)

        # Splitter: charts on top, table on bottom
        splitter = QSplitter(Qt.Orientation.Vertical)

        # Charts row
        charts_widget = QWidget()
        charts_layout = QHBoxLayout(charts_widget)

        # High-level type chart
        if HAS_PYQTGRAPH:
            type_frame = QFrame()
            type_frame.setFrameStyle(QFrame.Shape.Box | QFrame.Shadow.Raised)
            type_layout = QVBoxLayout(type_frame)
            type_label = QLabel("Frame Types")
            type_label.setStyleSheet("font-weight: bold;")
            type_layout.addWidget(type_label)

            self.type_plot = pg.PlotWidget()
            self.type_plot.setBackground('#2b2b2b')
            self.type_plot.setLabel('left', 'Count')
            type_layout.addWidget(self.type_plot)
            charts_layout.addWidget(type_frame)

            # Subtype chart
            subtype_frame = QFrame()
            subtype_frame.setFrameStyle(QFrame.Shape.Box | QFrame.Shadow.Raised)
            subtype_layout = QVBoxLayout(subtype_frame)
            subtype_label = QLabel("Top Frame Subtypes")
            subtype_label.setStyleSheet("font-weight: bold;")
            subtype_layout.addWidget(subtype_label)

            self.subtype_plot = pg.PlotWidget()
            self.subtype_plot.setBackground('#2b2b2b')
            self.subtype_plot.setLabel('bottom', 'Count')
            subtype_layout.addWidget(self.subtype_plot)
            charts_layout.addWidget(subtype_frame)
        else:
            self.type_plot = None
            self.subtype_plot = None

        splitter.addWidget(charts_widget)

        # Table
        self.table = QTableWidget()
        self.table.setColumnCount(3)
        self.table.setHorizontalHeaderLabels(['Category', 'Name', 'Count'])
        self.table.horizontalHeader().setSectionResizeMode(
            QHeaderView.ResizeMode.Stretch
        )
        self.table.setAlternatingRowColors(True)
        self.table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        splitter.addWidget(self.table)

        splitter.setSizes([350, 300])
        layout.addWidget(splitter)

    def load_data(self, df: pd.DataFrame):
        """Load frame type distribution data."""
        if df.empty:
            self.table.setRowCount(0)
            return

        # Populate table
        self.table.setRowCount(len(df))
        for row_idx, (_, row) in enumerate(df.iterrows()):
            self.table.setItem(row_idx, 0, QTableWidgetItem(str(row.get('category', ''))))
            self.table.setItem(row_idx, 1, QTableWidgetItem(str(row.get('name', ''))))

            count = row.get('count', 0)
            count_item = QTableWidgetItem(f"{count:,}")
            count_item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            self.table.setItem(row_idx, 2, count_item)

        # Update charts
        if self.type_plot is not None:
            type_df = df[df['category'] == 'type']
            if not type_df.empty:
                self.type_plot.clear()
                colors = {
                    'Management': '#3498db',
                    'Control': '#e74c3c',
                    'Data': '#2ecc71',
                    'Non-802.11': '#95a5a6',
                }
                x = list(range(len(type_df)))
                y = type_df['count'].values
                names = type_df['name'].tolist()
                brushes = [colors.get(n, '#95a5a6') for n in names]
                bargraph = pg.BarGraphItem(x=x, height=y, width=0.6, brushes=brushes)
                self.type_plot.addItem(bargraph)
                ax = self.type_plot.getAxis('bottom')
                ax.setTicks([[(i, names[i]) for i in range(len(names))]])

        if self.subtype_plot is not None:
            subtype_df = df[df['category'] == 'subtype'].head(15)
            if not subtype_df.empty:
                self.subtype_plot.clear()
                y = list(range(len(subtype_df)))
                x = subtype_df['count'].values
                names = subtype_df['name'].tolist()
                bargraph = pg.BarGraphItem(x0=0, y=y, height=0.6, width=x,
                                          brush='#3498db')
                self.subtype_plot.addItem(bargraph)
                ax = self.subtype_plot.getAxis('left')
                ax.setTicks([[(i, names[i][:25]) for i in range(len(names))]])


class NetworksView(QWidget):
    """Networks (SSIDs) investigation hub with left/right split panel."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._reader = None
        self._networks_df = pd.DataFrame()
        self._all_ssids = []
        self._ssid_locations = {}
        self._geocode_worker = None
        self._ssid_geocode_worker = None
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        splitter = QSplitter(Qt.Orientation.Horizontal)

        # --- LEFT: SSID list with search ---
        left = QWidget()
        left_layout = QVBoxLayout(left)
        left_layout.setContentsMargins(4, 4, 4, 4)

        search_row = QHBoxLayout()
        self._search = QLineEdit()
        self._search.setPlaceholderText("Search SSIDs...")
        self._search.textChanged.connect(self._filter_list)
        search_row.addWidget(self._search)
        search_row.addWidget(_make_help_button(
            "Networks Investigation Hub\n\n"
            "Click any SSID on the left to see detailed information:\n\n"
            "- Access Points broadcasting that network name\n"
            "- Clients that probed for it (PCAP only)\n"
            "- Captured WPA handshakes (PCAP only)\n"
            "- Similar network names (fuzzy match)\n\n"
            "Multiple APs with the same SSID may indicate a mesh network, "
            "enterprise setup, or an evil twin attack.\n\n"
            "The 'Similar Networks' section helps identify related "
            "infrastructure (e.g. MyNetwork, MyNetwork_5G, MyNetwork-guest).",
            self))
        left_layout.addLayout(search_row)

        self._ssid_table = QTableWidget()
        self._ssid_table.setColumnCount(3)
        self._ssid_table.setHorizontalHeaderLabels(['SSID', 'APs', 'Location'])
        self._ssid_table.horizontalHeader().setSectionResizeMode(
            0, QHeaderView.ResizeMode.Interactive)
        self._ssid_table.horizontalHeader().setSectionResizeMode(
            1, QHeaderView.ResizeMode.ResizeToContents)
        self._ssid_table.horizontalHeader().setSectionResizeMode(
            2, QHeaderView.ResizeMode.Stretch)
        self._ssid_table.setColumnWidth(0, 120)
        self._ssid_table.setSelectionBehavior(
            QTableWidget.SelectionBehavior.SelectRows)
        self._ssid_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._ssid_table.setAlternatingRowColors(True)
        self._ssid_table.setSortingEnabled(True)
        self._ssid_table.currentCellChanged.connect(self._on_ssid_selected)
        left_layout.addWidget(self._ssid_table)

        self._count_label = QLabel("0 networks")
        self._count_label.setStyleSheet("color: #999; font-size: 11px;")
        left_layout.addWidget(self._count_label)

        splitter.addWidget(left)

        # --- RIGHT: Detail panel (scrollable) ---
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        self._detail_widget = QWidget()
        detail = QVBoxLayout(self._detail_widget)
        detail.setContentsMargins(8, 8, 8, 8)

        # Header
        self._header = QLabel("Select a network")
        self._header.setStyleSheet("font-size: 18px; font-weight: bold;")
        self._header.setWordWrap(True)
        detail.addWidget(self._header)

        self._meta_label = QLabel("")
        self._meta_label.setStyleSheet("color: #999; font-size: 12px; margin-bottom: 8px;")
        detail.addWidget(self._meta_label)

        # --- Access Points section ---
        detail.addWidget(self._section_label("Access Points Broadcasting"))
        self._ap_table = QTableWidget()
        self._ap_table.setColumnCount(6)
        self._ap_table.setHorizontalHeaderLabels([
            'BSSID', 'Manufacturer', 'Encryption', 'Channel', 'Signal', 'Location'])
        self._ap_table.horizontalHeader().setSectionResizeMode(
            QHeaderView.ResizeMode.ResizeToContents)
        self._ap_table.horizontalHeader().setSectionResizeMode(
            5, QHeaderView.ResizeMode.Stretch)
        self._ap_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._ap_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._ap_table.setAlternatingRowColors(True)
        self._ap_table.setMaximumHeight(200)
        detail.addWidget(self._ap_table)

        # --- Probing Clients section (PCAP only) ---
        self._probe_section = QLabel()
        self._probe_section.setStyleSheet(
            "font-weight: bold; color: #e0e0e0; margin-top: 12px; "
            "border-bottom: 1px solid #555; padding-bottom: 3px;")
        self._probe_section.setText("Clients Probing for This SSID")
        self._probe_section.setToolTip(
            "Devices that sent probe requests for this network name.\n"
            "'Other SSIDs' shows what else the device is looking for —\n"
            "useful for fingerprinting and tracking.")
        detail.addWidget(self._probe_section)

        self._probe_table = QTableWidget()
        self._probe_table.setColumnCount(4)
        self._probe_table.setHorizontalHeaderLabels([
            'Client MAC', 'Manufacturer', 'Other SSIDs', 'Signal'])
        self._probe_table.horizontalHeader().setSectionResizeMode(
            QHeaderView.ResizeMode.ResizeToContents)
        self._probe_table.horizontalHeader().setSectionResizeMode(
            2, QHeaderView.ResizeMode.Stretch)
        self._probe_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._probe_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._probe_table.setAlternatingRowColors(True)
        self._probe_table.setMaximumHeight(200)
        detail.addWidget(self._probe_table)

        # --- Handshakes section (PCAP only) ---
        self._hs_section = QLabel()
        self._hs_section.setStyleSheet(
            "font-weight: bold; color: #e0e0e0; margin-top: 12px; "
            "border-bottom: 1px solid #555; padding-bottom: 3px;")
        self._hs_section.setText("Captured Handshakes")
        detail.addWidget(self._hs_section)

        self._hs_table = QTableWidget()
        self._hs_table.setColumnCount(4)
        self._hs_table.setHorizontalHeaderLabels([
            'BSSID', 'Client MAC', 'Messages', 'Status'])
        self._hs_table.horizontalHeader().setSectionResizeMode(
            QHeaderView.ResizeMode.ResizeToContents)
        self._hs_table.horizontalHeader().setSectionResizeMode(
            0, QHeaderView.ResizeMode.Stretch)
        self._hs_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._hs_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._hs_table.setAlternatingRowColors(True)
        self._hs_table.setMaximumHeight(150)
        detail.addWidget(self._hs_table)

        # --- Similar Networks section ---
        self._similar_section = QLabel()
        self._similar_section.setStyleSheet(
            "font-weight: bold; color: #e0e0e0; margin-top: 12px; "
            "border-bottom: 1px solid #555; padding-bottom: 3px;")
        self._similar_section.setText("Similar Networks (Fuzzy Match)")
        self._similar_section.setToolTip(
            "Networks that match after removing common suffixes\n"
            "like _5G, _2G, -guest, _ext, _repeater, etc.\n"
            "Helps identify related network infrastructure.")
        detail.addWidget(self._similar_section)

        self._similar_list = QLabel("")
        self._similar_list.setWordWrap(True)
        self._similar_list.setStyleSheet("color: #bbb; padding: 4px;")
        detail.addWidget(self._similar_list)

        detail.addStretch()
        scroll.setWidget(self._detail_widget)
        splitter.addWidget(scroll)

        splitter.setSizes([300, 700])
        layout.addWidget(splitter)

    @staticmethod
    def _section_label(text: str) -> QLabel:
        lbl = QLabel(text)
        lbl.setStyleSheet(
            "font-weight: bold; color: #e0e0e0; margin-top: 12px; "
            "border-bottom: 1px solid #555; padding-bottom: 3px;")
        return lbl

    # --- Data loading ---

    def load_data(self, reader):
        """Load network data from the capture reader."""
        self._reader = reader
        self._networks_df = reader.get_networks()

        if self._networks_df.empty:
            self._ssid_table.setRowCount(0)
            self._count_label.setText("0 networks")
            self._all_ssids = []
            return

        # Build SSID→location lookup from AP data (strongest AP per SSID)
        self._ssid_locations = {}
        ap_df = reader.get_access_points()
        if not ap_df.empty and 'name' in ap_df.columns:
            lat_col = 'min_lat' if 'min_lat' in ap_df.columns else 'lat'
            lon_col = 'min_lon' if 'min_lon' in ap_df.columns else 'lon'
            sig_col = 'strongest_signal' if 'strongest_signal' in ap_df.columns else None
            if lat_col in ap_df.columns:
                for ssid in self._networks_df['ssid']:
                    ssid_aps = ap_df[ap_df['name'] == ssid]
                    if ssid_aps.empty:
                        continue
                    # Pick strongest AP with GPS
                    valid = ssid_aps[
                        (ssid_aps[lat_col].notna()) &
                        (ssid_aps[lat_col] != 0) &
                        (ssid_aps[lon_col].notna()) &
                        (ssid_aps[lon_col] != 0)
                    ]
                    if valid.empty:
                        continue
                    if sig_col and sig_col in valid.columns:
                        best = valid.loc[valid[sig_col].idxmax()]
                    else:
                        best = valid.iloc[0]
                    lat, lon = float(best[lat_col]), float(best[lon_col])
                    cached = _geocode_cache.get((round(lat, 4), round(lon, 4)))
                    self._ssid_locations[ssid] = cached if cached else f"{lat:.4f}, {lon:.4f}"

        self._all_ssids = self._networks_df['ssid'].tolist()
        self._populate_ssid_table(self._networks_df)

        # Start background geocoding for SSIDs that only have coordinates
        geocode_requests = []
        for row_idx, (_, row) in enumerate(self._networks_df.iterrows()):
            ssid = str(row.get('ssid', ''))
            loc = self._ssid_locations.get(ssid, '')
            if loc and ',' in loc and not any(c.isalpha() for c in loc):
                # Looks like raw coordinates — queue for geocoding
                parts = loc.split(',')
                try:
                    lat, lon = float(parts[0].strip()), float(parts[1].strip())
                    geocode_requests.append((row_idx, lat, lon))
                except (ValueError, IndexError):
                    pass

        if geocode_requests:
            if self._ssid_geocode_worker and self._ssid_geocode_worker.isRunning():
                self._ssid_geocode_worker.stop()
                self._ssid_geocode_worker.result_ready.disconnect()
                self._ssid_geocode_worker.wait(2000)
            self._ssid_geocode_worker = _GeocodeWorker(geocode_requests, self)
            self._ssid_geocode_worker.result_ready.connect(self._on_ssid_geocode_result)
            self._ssid_geocode_worker.start()

        # Hide PCAP-only sections when not available
        has_pcap = reader.has_pcap_features()
        for w in (self._probe_section, self._probe_table,
                  self._hs_section, self._hs_table):
            w.setVisible(has_pcap)

    def _populate_ssid_table(self, df: pd.DataFrame):
        """Fill the left SSID list."""
        self._ssid_table.setSortingEnabled(False)
        self._ssid_table.setRowCount(len(df))

        for row_idx, (_, row) in enumerate(df.iterrows()):
            ssid = str(row.get('ssid', ''))
            ssid_item = QTableWidgetItem(ssid)
            self._ssid_table.setItem(row_idx, 0, ssid_item)

            count = int(row.get('ap_count', 0))
            count_item = QTableWidgetItem()
            count_item.setData(Qt.ItemDataRole.DisplayRole, count)
            count_item.setTextAlignment(
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            self._ssid_table.setItem(row_idx, 1, count_item)

            location = self._ssid_locations.get(ssid, '')
            self._ssid_table.setItem(row_idx, 2, QTableWidgetItem(location))

        self._ssid_table.setSortingEnabled(True)
        self._count_label.setText(f"{len(df)} networks")

    def _filter_list(self, text: str):
        """Filter the SSID list by search text."""
        if not text:
            self._populate_ssid_table(self._networks_df)
            return

        lower = text.lower()
        mask = self._networks_df['ssid'].str.lower().str.contains(lower, na=False)
        filtered = self._networks_df[mask]
        self._populate_ssid_table(filtered)

    def _on_ssid_selected(self, row, col, prev_row, prev_col):
        """Populate right panel when an SSID is selected."""
        if row < 0:
            return
        item = self._ssid_table.item(row, 0)
        if not item:
            return
        ssid = item.text()
        self._show_details(ssid)

    # --- Detail panel ---

    def _show_details(self, ssid: str):
        """Populate the right-side detail panel for a selected SSID."""
        if not self._reader:
            return

        self._header.setText(ssid)

        # --- Access Points ---
        ap_df = self._reader.get_access_points()
        ssid_aps = pd.DataFrame()
        if not ap_df.empty and 'name' in ap_df.columns:
            ssid_aps = ap_df[ap_df['name'] == ssid]

        encryptions = set()
        channels = set()

        self._ap_table.setRowCount(len(ssid_aps))
        geocode_requests = []
        for i, (_, ap) in enumerate(ssid_aps.iterrows()):
            self._ap_table.setItem(i, 0, QTableWidgetItem(
                str(ap.get('devmac', ''))))
            self._ap_table.setItem(i, 1, QTableWidgetItem(
                str(ap.get('manufacturer', ''))))

            enc = str(ap.get('encryption', ''))
            enc_item = QTableWidgetItem(enc)
            if 'WPA3' in enc:
                enc_item.setForeground(QBrush(QColor(46, 204, 113)))
            elif 'WPA2' in enc:
                enc_item.setForeground(QBrush(QColor(52, 152, 219)))
            elif 'WEP' in enc:
                enc_item.setForeground(QBrush(QColor(231, 76, 60)))
            elif 'Open' in enc:
                enc_item.setForeground(QBrush(QColor(230, 126, 34)))
            self._ap_table.setItem(i, 2, enc_item)

            self._ap_table.setItem(i, 3, QTableWidgetItem(
                str(ap.get('channel', ''))))

            sig = ap.get('strongest_signal')
            sig_str = str(int(sig)) + ' dBm' if pd.notna(sig) else ''
            self._ap_table.setItem(i, 4, QTableWidgetItem(sig_str))

            # Location column — show coords, queue geocoding
            lat = ap.get('min_lat', 0)
            lon = ap.get('min_lon', 0)
            if lat and lon and lat != 0 and lon != 0:
                cached = _geocode_cache.get((round(lat, 4), round(lon, 4)))
                if cached:
                    self._ap_table.setItem(i, 5, QTableWidgetItem(cached))
                else:
                    self._ap_table.setItem(i, 5, QTableWidgetItem(
                        f"{lat:.4f}, {lon:.4f}"))
                    geocode_requests.append((i, lat, lon))
            else:
                self._ap_table.setItem(i, 5, QTableWidgetItem(''))

            if enc:
                encryptions.add(enc)
            ch = ap.get('channel', '')
            if ch:
                channels.add(str(ch))

        # Start background geocoding for APs with GPS
        if geocode_requests:
            if self._geocode_worker and self._geocode_worker.isRunning():
                self._geocode_worker.stop()
                self._geocode_worker.result_ready.disconnect()
                self._geocode_worker.wait(2000)
            self._geocode_worker = _GeocodeWorker(geocode_requests, self)
            self._geocode_worker.result_ready.connect(self._on_geocode_result)
            self._geocode_worker.start()

        # Meta line
        parts = []
        if encryptions:
            parts.append(f"Encryption: {', '.join(sorted(encryptions))}")
        if channels:
            parts.append(f"Channel{'s' if len(channels) > 1 else ''}: {', '.join(sorted(channels))}")
        parts.append(f"{len(ssid_aps)} AP{'s' if len(ssid_aps) != 1 else ''}")
        self._meta_label.setText("  |  ".join(parts))

        # --- Probing Clients (PCAP only) ---
        if self._reader.has_pcap_features():
            probe_df = self._reader.get_probe_requests()
            ssid_probes = pd.DataFrame()
            if not probe_df.empty and 'probed_ssids' in probe_df.columns:
                mask = probe_df['probed_ssids'].apply(
                    lambda s: ssid in [x.strip() for x in str(s).split(',')]
                    if pd.notna(s) else False)
                ssid_probes = probe_df[mask]

            self._probe_section.setText(
                f"Clients Probing for This SSID ({len(ssid_probes)})")
            self._probe_table.setRowCount(len(ssid_probes))
            for i, (_, pr) in enumerate(ssid_probes.iterrows()):
                self._probe_table.setItem(i, 0, QTableWidgetItem(
                    str(pr.get('client_mac', ''))))
                self._probe_table.setItem(i, 1, QTableWidgetItem(
                    str(pr.get('manufacturer', ''))))
                # Show other SSIDs this client probed (excluding the selected one)
                all_ssids = str(pr.get('probed_ssids', ''))
                other = [s.strip() for s in all_ssids.split(',')
                         if s.strip() and s.strip() != ssid
                         and s.strip() != '<broadcast>']
                self._probe_table.setItem(i, 2, QTableWidgetItem(
                    ', '.join(other) if other else ''))
                sig = pr.get('strongest_signal')
                sig_str = str(int(sig)) + ' dBm' if pd.notna(sig) else ''
                self._probe_table.setItem(i, 3, QTableWidgetItem(sig_str))

            # --- Handshakes ---
            hs_df = self._reader.get_handshakes()
            ssid_hs = pd.DataFrame()
            if not hs_df.empty and 'ssid' in hs_df.columns:
                ssid_hs = hs_df[hs_df['ssid'] == ssid]

            self._hs_section.setText(
                f"Captured Handshakes ({len(ssid_hs)})")
            self._hs_table.setRowCount(len(ssid_hs))
            for i, (_, hs) in enumerate(ssid_hs.iterrows()):
                self._hs_table.setItem(i, 0, QTableWidgetItem(
                    str(hs.get('bssid', ''))))
                self._hs_table.setItem(i, 1, QTableWidgetItem(
                    str(hs.get('client_mac', ''))))
                self._hs_table.setItem(i, 2, QTableWidgetItem(
                    str(hs.get('messages', ''))))
                complete = hs.get('complete', False)
                status_item = QTableWidgetItem(
                    "Complete" if complete else "Partial")
                status_item.setForeground(QBrush(QColor(
                    46, 204, 113) if complete else QColor(230, 126, 34)))
                self._hs_table.setItem(i, 3, status_item)

        # --- Similar Networks (fuzzy match) ---
        base = _strip_ssid_suffixes(ssid).lower()
        similar = []
        for other_ssid in self._all_ssids:
            if other_ssid == ssid:
                continue
            if _strip_ssid_suffixes(other_ssid).lower() == base:
                similar.append(other_ssid)

        if similar:
            self._similar_section.setText(
                f"Similar Networks ({len(similar)})")
            self._similar_list.setText('\n'.join(sorted(similar)))
            self._similar_section.setVisible(True)
            self._similar_list.setVisible(True)
        else:
            self._similar_section.setVisible(False)
            self._similar_list.setVisible(False)

    def _on_geocode_result(self, row_idx: int, address: str):
        """Update AP table with geocoded address from background thread."""
        try:
            if address and row_idx < self._ap_table.rowCount():
                self._ap_table.setItem(row_idx, 5, QTableWidgetItem(address))
        except RuntimeError:
            pass  # Widget was destroyed between signal emit and delivery

    def _on_ssid_geocode_result(self, row_idx: int, address: str):
        """Update SSID list Location column with geocoded address."""
        try:
            if address and row_idx < self._ssid_table.rowCount():
                self._ssid_table.setItem(row_idx, 2, QTableWidgetItem(address))
                # Update the lookup so future repopulations use the address
                ssid_item = self._ssid_table.item(row_idx, 0)
                if ssid_item:
                    self._ssid_locations[ssid_item.text()] = address
        except RuntimeError:
            pass
