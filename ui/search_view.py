"""Network Search — local-first with WiGLE API fallback."""

import csv
import gzip
import json
import logging
import xml.etree.ElementTree as ET
from pathlib import Path

from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QGroupBox, QTreeWidget, QTreeWidgetItem, QHeaderView,
    QLineEdit, QFormLayout, QComboBox
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QFont, QColor

from database.wigle_api import WigleApiClient

log = logging.getLogger(__name__)

_KML_DIR = Path.home() / '.config' / 'airparse' / 'kml'
_STAGE_DIR = Path.home() / '.config' / 'airparse' / 'wigle_uploads'
_PULL_DIR = Path.home() / '.local' / 'share' / 'airparse' / 'pulls'

_LABEL_STYLE = "color: #e0e0e0; border: none; background: transparent;"
_DIM_STYLE = "color: #999; border: none; background: transparent; font-size: 11px;"

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
    QLineEdit {
        background-color: #3c3f41; color: #e0e0e0;
        border: 1px solid #555; border-radius: 3px; padding: 4px;
    }
"""


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


# ─── Workers ────────────────────────────────────────────────────────

class _SearchWorker(QThread):
    result = pyqtSignal(list)
    def __init__(self, kwargs: dict):
        super().__init__()
        self._kwargs = kwargs
    def run(self):
        self.result.emit(WigleApiClient().search_networks(**self._kwargs))


class _GeocodeWorker(QThread):
    result = pyqtSignal(bool, float, float, str)
    def __init__(self, address: str):
        super().__init__()
        self._address = address
    def run(self):
        import urllib.request, urllib.parse, json as _json
        try:
            q = urllib.parse.quote(self._address)
            url = f'https://nominatim.openstreetmap.org/search?q={q}&format=json&limit=1'
            req = urllib.request.Request(url, headers={'User-Agent': 'AirParse/2.0'})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = _json.loads(resp.read())
            if data:
                lat = float(data[0]['lat'])
                lon = float(data[0]['lon'])
                name = data[0].get('display_name', '')
                self.result.emit(True, lat, lon, name)
            else:
                self.result.emit(False, 0, 0, 'Address not found')
        except Exception as e:
            self.result.emit(False, 0, 0, str(e))


class _LocalSearchWorker(QThread):
    result = pyqtSignal(list)
    def __init__(self, criteria: dict):
        super().__init__()
        self._criteria = criteria
    def run(self):
        files = _find_wiglecsv_files()
        results = _search_wiglecsv(files, self._criteria)
        # Dedup KML against CSV — skip KML entries whose SSID already appeared
        seen_ssids = {r['ssid'].lower() for r in results if r.get('ssid')}
        for r in _search_kml_files(self._criteria):
            if r['ssid'].lower() not in seen_ssids:
                seen_ssids.add(r['ssid'].lower())
                results.append(r)
        self.result.emit(results)


# ─── Local data helpers ─────────────────────────────────────────────

def _find_wiglecsv_files() -> list[Path]:
    files = []
    for d in [_STAGE_DIR, _PULL_DIR]:
        if d.exists():
            for fp in d.rglob('*.wiglecsv'):
                files.append(fp)
    uploaded = _STAGE_DIR / 'uploaded'
    if uploaded.exists():
        for fp in uploaded.rglob('*.wiglecsv'):
            files.append(fp)
    return files


def _search_wiglecsv(files: list[Path], criteria: dict) -> list[dict]:
    results = []
    seen = set()

    ssid_exact = criteria.get('ssid', '').lower()
    ssid_like = criteria.get('ssidlike', '').lower()
    netid = criteria.get('netid', '').upper()
    encryption = criteria.get('encryption', '').lower()
    lat1 = float(criteria['latrange1']) if criteria.get('latrange1') else None
    lat2 = float(criteria['latrange2']) if criteria.get('latrange2') else None
    lon1 = float(criteria['longrange1']) if criteria.get('longrange1') else None
    lon2 = float(criteria['longrange2']) if criteria.get('longrange2') else None

    netid_prefix = ''
    netid_exact = ''
    if netid:
        if netid.endswith('%'):
            netid_prefix = netid.rstrip('%').rstrip(':').upper()
        else:
            netid_exact = netid

    for fp in files:
        try:
            if fp.name.lower().endswith('.csv.gz'):
                fh = gzip.open(fp, 'rt', encoding='utf-8', errors='replace')
            else:
                fh = open(fp, 'r', encoding='utf-8', errors='replace')
            with fh:
                first = fh.readline()
                if not first.startswith('WigleWifi'):
                    continue
                reader = csv.DictReader(fh)
                for row in reader:
                    mac = row.get('MAC', '').upper()
                    if not mac or mac in seen:
                        continue
                    row_ssid = row.get('SSID', '').strip('"')
                    row_auth = row.get('AuthMode', '')
                    row_chan = row.get('Channel', '')
                    try:
                        row_lat = float(row.get('CurrentLatitude', 0) or 0)
                        row_lon = float(row.get('CurrentLongitude', 0) or 0)
                    except ValueError:
                        row_lat = row_lon = 0.0
                    row_seen = row.get('FirstSeen', '')

                    if ssid_exact and row_ssid.lower() != ssid_exact:
                        continue
                    if ssid_like and ssid_like not in row_ssid.lower():
                        continue
                    if netid_exact and mac != netid_exact:
                        continue
                    if netid_prefix and not mac.startswith(netid_prefix):
                        continue
                    if encryption and encryption not in row_auth.lower():
                        continue
                    if lat1 is not None and row_lat < lat1:
                        continue
                    if lat2 is not None and row_lat > lat2:
                        continue
                    if lon1 is not None and row_lon < lon1:
                        continue
                    if lon2 is not None and row_lon > lon2:
                        continue

                    seen.add(mac)
                    results.append({
                        'ssid': row_ssid,
                        'netid': mac,
                        'encryption': row_auth,
                        'channel': row_chan,
                        'trilat': row_lat,
                        'trilong': row_lon,
                        'lastupdt': row_seen,
                        'source': 'Local (CSV)',
                    })
        except Exception as e:
            log.warning("Failed to search %s: %s", fp.name, e)
    return results


def _search_kml_files(criteria: dict) -> list[dict]:
    if not _KML_DIR.exists():
        return []

    ssid_exact = criteria.get('ssid', '').lower()
    ssid_like = criteria.get('ssidlike', '').lower()
    lat1 = float(criteria['latrange1']) if criteria.get('latrange1') else None
    lat2 = float(criteria['latrange2']) if criteria.get('latrange2') else None
    lon1 = float(criteria['longrange1']) if criteria.get('longrange1') else None
    lon2 = float(criteria['longrange2']) if criteria.get('longrange2') else None

    if criteria.get('netid') or criteria.get('encryption'):
        return []

    results = []
    seen_names = set()
    for kml_file in _KML_DIR.glob('*.kml'):
        for pt in _parse_kml_points(kml_file):
            name = pt.get('name', '')
            name_key = name.lower()
            lat, lon = pt['lat'], pt['lon']
            if name_key in seen_names:
                continue
            if ssid_exact and name_key != ssid_exact:
                continue
            if ssid_like and ssid_like not in name_key:
                continue
            if lat1 is not None and lat < lat1:
                continue
            if lat2 is not None and lat > lat2:
                continue
            if lon1 is not None and lon < lon1:
                continue
            if lon2 is not None and lon > lon2:
                continue
            seen_names.add(name_key)
            results.append({
                'ssid': name,
                'netid': '',
                'encryption': '',
                'channel': '',
                'trilat': lat,
                'trilong': lon,
                'lastupdt': '',
                'source': 'Local (KML)',
            })
    return results


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


# ─── Search View ────────────────────────────────────────────────────

class SearchView(QWidget):

    def __init__(self, parent=None):
        super().__init__(parent)
        self._workers = []
        self._pending_search_criteria = {}
        self._build_ui()

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(12)
        layout.setContentsMargins(16, 16, 16, 16)
        self.setStyleSheet(_INPUT_STYLE)

        title = QLabel("Network Search")
        title.setFont(QFont('', 16, QFont.Weight.Bold))
        title.setStyleSheet(_LABEL_STYLE)
        layout.addWidget(title)

        search_group = QGroupBox("Search Parameters")
        search_group.setStyleSheet(_GROUP_STYLE)
        form = QFormLayout(search_group)

        self._search_ssid = QLineEdit()
        self._search_ssid.setPlaceholderText("Exact SSID or leave blank")
        form.addRow("SSID:", self._search_ssid)

        self._search_ssidlike = QLineEdit()
        self._search_ssidlike.setPlaceholderText("Partial SSID match (wildcard)")
        form.addRow("SSID Like:", self._search_ssidlike)

        self._search_bssid = QLineEdit()
        self._search_bssid.setPlaceholderText("AA:BB:CC:DD:EE:FF")
        form.addRow("BSSID:", self._search_bssid)

        self._search_oui = QLineEdit()
        self._search_oui.setPlaceholderText("AA:BB:CC (manufacturer prefix)")
        form.addRow("OUI:", self._search_oui)

        coord_row = QHBoxLayout()
        self._search_lat1 = QLineEdit()
        self._search_lat1.setPlaceholderText("Min Lat")
        self._search_lon1 = QLineEdit()
        self._search_lon1.setPlaceholderText("Min Lon")
        self._search_lat2 = QLineEdit()
        self._search_lat2.setPlaceholderText("Max Lat")
        self._search_lon2 = QLineEdit()
        self._search_lon2.setPlaceholderText("Max Lon")
        coord_row.addWidget(self._search_lat1)
        coord_row.addWidget(self._search_lon1)
        coord_row.addWidget(QLabel(" to "))
        coord_row.addWidget(self._search_lat2)
        coord_row.addWidget(self._search_lon2)
        form.addRow("Lat/Lon Range:", coord_row)

        self._search_encryption = QLineEdit()
        self._search_encryption.setPlaceholderText("e.g. wpa2, wep, open")
        form.addRow("Encryption:", self._search_encryption)

        addr_row = QHBoxLayout()
        self._search_address = QLineEdit()
        self._search_address.setPlaceholderText("Street address, city, zip — auto-fills lat/lon")
        addr_row.addWidget(self._search_address, 1)
        self._geocode_btn = _action_btn("Geocode")
        self._geocode_btn.clicked.connect(self._geocode_address)
        addr_row.addWidget(self._geocode_btn)
        form.addRow("Address:", addr_row)

        self._search_mode = QComboBox()
        self._search_mode.addItems(["Local First", "WiGLE Only"])
        self._search_mode.setStyleSheet("""
            QComboBox { background-color: #3c3f41; color: #e0e0e0;
                        border: 1px solid #555; border-radius: 3px; padding: 4px; }
            QComboBox::drop-down { border: none; }
            QComboBox QAbstractItemView { background-color: #3c3f41; color: #e0e0e0;
                                          selection-background-color: #2980b9; }
        """)
        form.addRow("Mode:", self._search_mode)

        search_btn_row = QHBoxLayout()
        search_btn_row.addStretch()
        self._search_btn = _action_btn("Search", "#2980b9", "white", bold=True)
        self._search_btn.clicked.connect(self._run_search)
        search_btn_row.addWidget(self._search_btn)
        form.addRow(search_btn_row)

        layout.addWidget(search_group)

        results_group = QGroupBox("Results")
        results_group.setStyleSheet(_GROUP_STYLE)
        results_layout = QVBoxLayout(results_group)

        self._search_status = QLabel("")
        self._search_status.setStyleSheet(_DIM_STYLE)
        results_layout.addWidget(self._search_status)

        self._search_tree = QTreeWidget()
        self._search_tree.setHeaderLabels(["SSID", "BSSID", "Encryption", "Channel", "Lat", "Lon", "Last Seen", "Source"])
        self._search_tree.setAlternatingRowColors(True)
        self._search_tree.setStyleSheet(_TREE_STYLE)
        self._search_tree.setRootIsDecorated(False)
        sh = self._search_tree.header()
        sh.setStretchLastSection(False)
        sh.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        for i in range(1, 8):
            sh.setSectionResizeMode(i, QHeaderView.ResizeMode.ResizeToContents)
        results_layout.addWidget(self._search_tree)

        layout.addWidget(results_group, 1)

    # ─── Geocoding ────────────────────────────────────────────────

    def _geocode_address(self):
        addr = self._search_address.text().strip()
        if not addr:
            return
        self._geocode_btn.setEnabled(False)
        self._geocode_btn.setText("...")
        worker = _GeocodeWorker(addr)
        worker.result.connect(self._on_geocode_result)
        worker.result.connect(lambda *_: self._workers.remove(worker))
        self._workers.append(worker)
        worker.start()

    def _on_geocode_result(self, ok: bool, lat: float, lon: float, display_name: str):
        self._geocode_btn.setEnabled(True)
        self._geocode_btn.setText("Geocode")
        if ok:
            offset = 0.005
            self._search_lat1.setText(f"{lat - offset:.6f}")
            self._search_lat2.setText(f"{lat + offset:.6f}")
            self._search_lon1.setText(f"{lon - offset:.6f}")
            self._search_lon2.setText(f"{lon + offset:.6f}")
            self._search_status.setText(f"Geocoded: {display_name}")
        else:
            self._search_status.setText(f"Geocode failed: {display_name}")

    def _on_geocode_then_search(self, ok: bool, lat: float, lon: float, display_name: str):
        if ok:
            offset = 0.005
            self._search_lat1.setText(f"{lat - offset:.6f}")
            self._search_lat2.setText(f"{lat + offset:.6f}")
            self._search_lon1.setText(f"{lon - offset:.6f}")
            self._search_lon2.setText(f"{lon + offset:.6f}")
            self._search_status.setText(f"Geocoded: {display_name}")
            self._run_search()
        else:
            self._search_status.setText(f"Geocode failed: {display_name}")
            self._search_btn.setEnabled(True)
            self._search_btn.setText("Search")

    # ─── Search ───────────────────────────────────────────────────

    def _gather_search_criteria(self) -> dict:
        kwargs = {}
        if v := self._search_ssid.text().strip():
            kwargs['ssid'] = v
        if v := self._search_ssidlike.text().strip():
            kwargs['ssidlike'] = v
        if v := self._search_bssid.text().strip():
            kwargs['netid'] = v
        if v := self._search_oui.text().strip():
            oui = v.upper().replace('-', ':')
            if not oui.endswith('%'):
                oui = oui.rstrip(':') + ':%'
            kwargs['netid'] = oui
        if v := self._search_lat1.text().strip():
            kwargs['latrange1'] = v
        if v := self._search_lon1.text().strip():
            kwargs['longrange1'] = v
        if v := self._search_lat2.text().strip():
            kwargs['latrange2'] = v
        if v := self._search_lon2.text().strip():
            kwargs['longrange2'] = v
        if v := self._search_encryption.text().strip():
            kwargs['encryption'] = v
        return kwargs

    def _run_search(self):
        kwargs = self._gather_search_criteria()
        if not kwargs:
            addr = self._search_address.text().strip()
            if addr:
                self._search_btn.setEnabled(False)
                self._search_btn.setText("Geocoding...")
                self._search_status.setText("Geocoding address...")
                worker = _GeocodeWorker(addr)
                worker.result.connect(self._on_geocode_then_search)
                worker.result.connect(lambda *_: self._workers.remove(worker))
                self._workers.append(worker)
                worker.start()
                return
            self._search_status.setText("Enter at least one search parameter")
            return

        wigle_only = self._search_mode.currentText() == "WiGLE Only"
        self._search_btn.setEnabled(False)
        self._search_tree.clear()
        self._pending_search_criteria = kwargs

        if wigle_only:
            if not WigleApiClient.has_credentials():
                self._search_status.setText("Configure API key in Settings for WiGLE search.")
                self._search_btn.setEnabled(True)
                return
            self._search_btn.setText("Searching WiGLE...")
            self._search_status.setText("Searching WiGLE API...")
            worker = _SearchWorker(kwargs)
            worker.result.connect(self._on_api_search_results)
            worker.result.connect(lambda _: self._workers.remove(worker))
            self._workers.append(worker)
            worker.start()
        else:
            self._search_btn.setText("Searching local...")
            self._search_status.setText("Searching local data first...")
            worker = _LocalSearchWorker(kwargs)
            worker.result.connect(self._on_local_search_results)
            worker.result.connect(lambda _: self._workers.remove(worker))
            self._workers.append(worker)
            worker.start()

    def _on_local_search_results(self, results: list):
        if results:
            self._show_search_results(results)
            local_count = len(results)
            self._search_status.setText(
                f"{local_count} local result{'s' if local_count != 1 else ''} found")
            self._search_btn.setEnabled(True)
            self._search_btn.setText("Search")
        else:
            if not WigleApiClient.has_credentials():
                self._search_status.setText("No local results. Configure API key in Settings for WiGLE search.")
                self._search_btn.setEnabled(True)
                self._search_btn.setText("Search")
                return
            self._search_status.setText("No local results — searching WiGLE API...")
            self._search_btn.setText("Searching WiGLE...")
            worker = _SearchWorker(self._pending_search_criteria)
            worker.result.connect(self._on_api_search_results)
            worker.result.connect(lambda _: self._workers.remove(worker))
            self._workers.append(worker)
            worker.start()

    def _on_api_search_results(self, results: list):
        for r in results:
            r['source'] = 'WiGLE API'
        self._show_search_results(results)
        self._search_status.setText(
            f"{len(results)} result{'s' if len(results) != 1 else ''} from WiGLE API")
        self._search_btn.setEnabled(True)
        self._search_btn.setText("Search")

    def _show_search_results(self, results: list):
        self._search_tree.clear()
        for r in results:
            lat = r.get('trilat', 0)
            lon = r.get('trilong', 0)
            item = QTreeWidgetItem([
                r.get('ssid', ''),
                r.get('netid', ''),
                r.get('encryption', ''),
                str(r.get('channel', '')),
                f"{lat:.5f}" if lat else '',
                f"{lon:.5f}" if lon else '',
                r.get('lastupdt', ''),
                r.get('source', ''),
            ])
            source = r.get('source', '')
            if 'Local' in source:
                item.setForeground(7, QColor('#2ecc71'))
            else:
                item.setForeground(7, QColor('#3498db'))
            self._search_tree.addTopLevelItem(item)
