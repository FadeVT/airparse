"""Connect dialog for discovering devices and pulling capture data."""

import logging
from pathlib import Path
from typing import Optional

from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QFrame, QScrollArea, QWidget, QCheckBox, QLineEdit,
    QComboBox, QSpinBox, QFileDialog, QMessageBox, QProgressBar,
    QGroupBox, QFormLayout
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QTimer, QElapsedTimer
from PyQt6.QtGui import QFont

from sources import (
    SourceConfig, DeviceSource, RemoteFile, PullResult,
    load_sources, save_sources, load_manifest, save_manifest,
    PULL_DIR, DEFAULT_SOURCES,
)
from sources.kismet_source import KismetSource
from sources.pwnagotchi_source import PwnagotchiSource
from sources.pager_source import PagerSource
from database.merged_db import MergedDatabase
from database.pcap_reader import PcapReader
from database.reader import KismetDBReader
from database.wigle_reader import WigleCsvReader
from database.hc22000_reader import Hc22000Reader

log = logging.getLogger(__name__)

_DARK_STYLE = """
    QDialog { background-color: #2b2b2b; color: #e0e0e0; }
    QLabel { color: #e0e0e0; }
    QGroupBox { color: #e0e0e0; border: 1px solid #444; border-radius: 4px;
                margin-top: 8px; padding-top: 12px; }
    QGroupBox::title { subcontrol-origin: margin; padding: 0 4px; }
    QPushButton {
        background-color: #3c3f41; color: #e0e0e0;
        border: 1px solid #555; border-radius: 4px; padding: 6px 16px;
    }
    QPushButton:hover { background-color: #4c5052; }
    QPushButton:disabled { background-color: #333; color: #666; }
    QLineEdit, QSpinBox, QComboBox {
        background-color: #3c3f41; color: #e0e0e0;
        border: 1px solid #555; border-radius: 3px; padding: 4px;
    }
    QCheckBox { color: #e0e0e0; }
    QCheckBox::indicator { width: 16px; height: 16px; }
    QProgressBar {
        border: 1px solid #555; border-radius: 3px;
        text-align: center; background-color: #2b2b2b; color: #e0e0e0;
    }
    QProgressBar::chunk { background-color: #2ecc71; border-radius: 2px; }
    QScrollArea { border: none; background-color: #2b2b2b; }
"""

SOURCE_CONSTRUCTORS = {
    'kismet': KismetSource,
    'pwnagotchi': PwnagotchiSource,
    'pager': PagerSource,
    'custom': DeviceSource,
}


class _SourceWidget(QFrame):
    """Widget representing a single device source in the connect dialog."""

    def __init__(self, config: SourceConfig, parent=None):
        super().__init__(parent)
        self.config = config
        self._probing = False
        self._file_count = 0
        self._total_size = 0
        self._online = False
        self._files: list[RemoteFile] = []
        self.setFrameShape(QFrame.Shape.StyledPanel)
        self.setStyleSheet("""
            QFrame { background-color: #333; border: 1px solid #444;
                     border-radius: 6px; padding: 8px; }
        """)
        self._setup_ui()

    def _setup_ui(self):
        layout = QHBoxLayout(self)
        layout.setContentsMargins(12, 8, 12, 8)

        # Checkbox + status indicator
        self.check = QCheckBox()
        self.check.setChecked(self.config.enabled and bool(self.config.host))
        layout.addWidget(self.check)

        # Status dot
        self.status_dot = QLabel("\u25cf")
        self.status_dot.setStyleSheet("color: #666; font-size: 14px;")
        self.status_dot.setFixedWidth(20)
        layout.addWidget(self.status_dot)

        # Info
        info_layout = QVBoxLayout()
        info_layout.setSpacing(2)

        name_label = QLabel(f"<b>{self.config.name}</b>")
        name_label.setStyleSheet("color: #e0e0e0; border: none; background: transparent;")
        info_layout.addWidget(name_label)

        host_text = self.config.host or "(not configured)"
        type_label = QLabel(f"{self.config.source_type} \u2014 {host_text}")
        type_label.setStyleSheet("color: #999; font-size: 11px; border: none; background: transparent;")
        info_layout.addWidget(type_label)

        self.status_label = QLabel("Not probed")
        self.status_label.setStyleSheet("color: #777; font-size: 11px; border: none; background: transparent;")
        info_layout.addWidget(self.status_label)

        layout.addLayout(info_layout, 1)

        # Edit button
        edit_btn = QPushButton("Edit")
        edit_btn.setFixedWidth(60)
        edit_btn.clicked.connect(self._on_edit)
        layout.addWidget(edit_btn)

    def set_probing(self):
        self._probing = True
        self.status_dot.setStyleSheet("color: #f39c12; font-size: 14px;")
        self.status_label.setText("Probing...")
        self.status_label.setStyleSheet("color: #f39c12; font-size: 11px; border: none; background: transparent;")

    def set_online(self, file_count: int, total_size: int, files: list[RemoteFile]):
        self._online = True
        self._probing = False
        self._file_count = file_count
        self._total_size = total_size
        self._files = files
        self.status_dot.setStyleSheet("color: #2ecc71; font-size: 14px;")
        size_str = self._format_size(total_size)
        self.status_label.setText(f"Online \u2014 {file_count} files ({size_str})")
        self.status_label.setStyleSheet("color: #2ecc71; font-size: 11px; border: none; background: transparent;")
        self.check.setChecked(True)

    def set_offline(self, error: str = ''):
        self._online = False
        self._probing = False
        self.status_dot.setStyleSheet("color: #e74c3c; font-size: 14px;")
        msg = f"Offline \u2014 {error}" if error else "Offline"
        self.status_label.setText(msg)
        self.status_label.setStyleSheet("color: #e74c3c; font-size: 11px; border: none; background: transparent;")
        self.check.setChecked(False)

    def _on_edit(self):
        dlg = _SourceEditorDialog(self.config, self)
        if dlg.exec() == QDialog.DialogCode.Accepted:
            self.config = dlg.get_config()
            self._refresh_labels()

    def _refresh_labels(self):
        layout = self.layout()
        info = layout.itemAt(2).layout()
        name_lbl = info.itemAt(0).widget()
        name_lbl.setText(f"<b>{self.config.name}</b>")
        type_lbl = info.itemAt(1).widget()
        host_text = self.config.host or "(not configured)"
        type_lbl.setText(f"{self.config.source_type} \u2014 {host_text}")

    @staticmethod
    def _format_size(nbytes: int) -> str:
        if nbytes >= 1_073_741_824:
            return f"{nbytes / 1_073_741_824:.1f} GB"
        if nbytes >= 1_048_576:
            return f"{nbytes / 1_048_576:.1f} MB"
        if nbytes >= 1024:
            return f"{nbytes / 1024:.0f} KB"
        return f"{nbytes} B"


class _SourceEditorDialog(QDialog):
    """Dialog for editing a source configuration."""

    def __init__(self, config: SourceConfig, parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"Edit Source: {config.name}")
        self.setMinimumWidth(400)
        self.setStyleSheet(_DARK_STYLE)
        self._config = SourceConfig(**config.__dict__)
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)

        form = QFormLayout()

        self._name_edit = QLineEdit(self._config.name)
        form.addRow("Name:", self._name_edit)

        self._type_combo = QComboBox()
        self._type_combo.addItems(['kismet', 'pwnagotchi', 'pager', 'custom'])
        idx = self._type_combo.findText(self._config.source_type)
        if idx >= 0:
            self._type_combo.setCurrentIndex(idx)
        form.addRow("Type:", self._type_combo)

        self._host_edit = QLineEdit(self._config.host)
        self._host_edit.setPlaceholderText("IP address or hostname")
        form.addRow("Host:", self._host_edit)

        self._port_spin = QSpinBox()
        self._port_spin.setRange(1, 65535)
        self._port_spin.setValue(self._config.port)
        form.addRow("Port:", self._port_spin)

        self._user_edit = QLineEdit(self._config.user)
        form.addRow("User:", self._user_edit)

        self._auth_combo = QComboBox()
        self._auth_combo.addItems(['key', 'password'])
        self._auth_combo.setCurrentText(self._config.auth)
        form.addRow("Auth:", self._auth_combo)

        self._key_edit = QLineEdit(self._config.key_file)
        form.addRow("Key file:", self._key_edit)

        self._path_edit = QLineEdit(self._config.remote_path)
        form.addRow("Remote path:", self._path_edit)

        self._types_edit = QLineEdit(', '.join(self._config.file_types))
        self._types_edit.setPlaceholderText(".pcap, .kismet, .22000")
        form.addRow("File types:", self._types_edit)

        layout.addLayout(form)

        # Test connection button
        test_btn = QPushButton("Test Connection")
        test_btn.clicked.connect(self._test_connection)
        layout.addWidget(test_btn)

        self._test_label = QLabel("")
        self._test_label.setStyleSheet("border: none; background: transparent;")
        layout.addWidget(self._test_label)

        # OK / Cancel
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)
        btn_layout.addWidget(cancel_btn)
        ok_btn = QPushButton("Save")
        ok_btn.setStyleSheet(
            "QPushButton { background-color: #2ecc71; color: #1e1e1e; font-weight: bold; }"
            "QPushButton:hover { background-color: #27ae60; }")
        ok_btn.clicked.connect(self.accept)
        btn_layout.addWidget(ok_btn)
        layout.addLayout(btn_layout)

    def _test_connection(self):
        config = self.get_config()
        cls = SOURCE_CONSTRUCTORS.get(config.source_type, DeviceSource)
        source = cls(config)
        self._test_label.setText("Testing...")
        self._test_label.setStyleSheet("color: #f39c12; border: none; background: transparent;")
        self._test_label.repaint()

        try:
            if source.probe():
                files = source.list_files()
                self._test_label.setText(f"Connected! {len(files)} files found.")
                self._test_label.setStyleSheet("color: #2ecc71; border: none; background: transparent;")
            else:
                self._test_label.setText("Connection failed.")
                self._test_label.setStyleSheet("color: #e74c3c; border: none; background: transparent;")
        except Exception as e:
            self._test_label.setText(f"Error: {e}")
            self._test_label.setStyleSheet("color: #e74c3c; border: none; background: transparent;")

    def get_config(self) -> SourceConfig:
        types_str = self._types_edit.text().strip()
        file_types = [t.strip() for t in types_str.split(',') if t.strip()] if types_str else []
        return SourceConfig(
            source_type=self._type_combo.currentText(),
            name=self._name_edit.text().strip() or 'Unnamed',
            host=self._host_edit.text().strip(),
            port=self._port_spin.value(),
            user=self._user_edit.text().strip() or 'pi',
            auth=self._auth_combo.currentText(),
            key_file=self._key_edit.text().strip(),
            remote_path=self._path_edit.text().strip(),
            file_types=file_types,
            enabled=True,
        )


class _ProbeWorker(QThread):
    """Background thread for probing device sources."""
    result = pyqtSignal(int, bool, list, str)  # index, online, files, error

    def __init__(self, index: int, config: SourceConfig):
        super().__init__()
        self._index = index
        self._config = config

    def run(self):
        if not self._config.host:
            self.result.emit(self._index, False, [], "No host configured")
            return
        cls = SOURCE_CONSTRUCTORS.get(self._config.source_type, DeviceSource)
        source = cls(self._config)
        try:
            if source.probe():
                files = source.list_files()
                self.result.emit(self._index, True, files, '')
            else:
                self.result.emit(self._index, False, [], "Connection refused")
        except Exception as e:
            self.result.emit(self._index, False, [], str(e))


class _PullWorker(QThread):
    """Background thread for pulling files and building merged database."""
    progress = pyqtSignal(str)  # status message
    file_pulled = pyqtSignal(str, str)  # source_name, filename
    byte_progress = pyqtSignal(int, int)  # bytes_transferred, bytes_total
    finished = pyqtSignal(object, str)  # MergedDatabase or None, error

    def __init__(self, source_widgets: list[_SourceWidget],
                 local_files: list[str], only_new: bool, delete_after: bool = False):
        super().__init__()
        # Snapshot widget state on GUI thread to avoid cross-thread access
        self._sources = [
            (sw.config, sw.config.file_types, sw._total_size)
            for sw in source_widgets
            if sw.check.isChecked() and sw._online
        ]
        self._local_files = local_files
        self._only_new = only_new
        self._delete_after = delete_after
        self._cancelled = False
        self._bytes_total = sum(s[2] for s in self._sources)
        self._bytes_transferred = 0
        self._current_file_base = 0

    def cancel(self):
        self._cancelled = True

    def _stop_service(self, config):
        """Stop the capture service on a device before pulling."""
        try:
            import paramiko
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            key_path = Path(config.key_file).expanduser()
            kwargs = {
                'hostname': config.host,
                'port': config.port,
                'username': config.user,
                'timeout': 10,
            }
            if key_path.exists():
                kwargs['key_filename'] = str(key_path)
            ssh.connect(**kwargs)
            if config.source_type == 'kismet':
                ssh.exec_command('sudo systemctl stop kismet', timeout=10)
            elif config.source_type == 'pager':
                ssh.exec_command('/etc/init.d/pineapd stop', timeout=10)
            ssh.close()
        except Exception as e:
            log.warning("Failed to stop service on %s: %s", config.name, e)

    def _start_service(self, config):
        """Restart the capture service on a device after pulling."""
        try:
            import paramiko
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            key_path = Path(config.key_file).expanduser()
            kwargs = {
                'hostname': config.host,
                'port': config.port,
                'username': config.user,
                'timeout': 10,
            }
            if key_path.exists():
                kwargs['key_filename'] = str(key_path)
            ssh.connect(**kwargs)
            if config.source_type == 'kismet':
                ssh.exec_command('sudo systemctl start kismet', timeout=10)
            elif config.source_type == 'pager':
                ssh.exec_command('/etc/init.d/pineapd start', timeout=10)
            ssh.close()
        except Exception as e:
            log.warning("Failed to start service on %s: %s", config.name, e)

    def run(self):
        try:
            merged = MergedDatabase()
            manifest = load_manifest()

            # Stop capture services before pulling for clean files
            for config, file_types, source_size in self._sources:
                if config.source_type in ('kismet', 'pager'):
                    self.progress.emit(f"Stopping {config.name}...")
                    self._stop_service(config)

            # Pull from remote sources (snapshotted in __init__)
            for config, file_types, source_size in self._sources:
                if self._cancelled:
                    break

                cls = SOURCE_CONSTRUCTORS.get(config.source_type, DeviceSource)
                source = cls(config)

                self.progress.emit(f"Pulling from {config.name}...")
                dest = PULL_DIR / config.name.lower().replace(' ', '_')

                src_name = config.name
                def _file_cb(name, transferred, total, _sn=src_name):
                    self.file_pulled.emit(_sn, name)
                    current = self._current_file_base + transferred
                    self.byte_progress.emit(current, self._bytes_total)

                result = source.pull_files(dest, manifest, self._only_new, _file_cb)
                # Advance base offset for next source
                self._current_file_base += source_size

                # Parse pulled files
                for local_path in result.files_pulled:
                    if self._cancelled:
                        break
                    self.progress.emit(f"Parsing {Path(local_path).name}...")
                    self._ingest_file(merged, local_path, config.name)

                # Also parse any previously pulled files that are still on disk
                if dest.exists():
                    for existing in dest.iterdir():
                        if self._cancelled:
                            break
                        if str(existing) not in result.files_pulled and existing.is_file():
                            ext = existing.suffix.lower()
                            if ext in (file_types or ['.pcap', '.kismet', '.22000']):
                                self._ingest_file(merged, str(existing), config.name)

                # Delete pulled files from device
                if self._delete_after and result.files_pulled and not self._cancelled:
                    self._delete_remote_files(config, source, result)

            save_manifest(manifest)
            self.byte_progress.emit(-1, -1)  # signal: pull phase done, switch to indeterminate

            # Process local files
            for lf in self._local_files:
                if self._cancelled:
                    break
                self.progress.emit(f"Parsing {Path(lf).name}...")
                self._ingest_file(merged, lf, 'Local')

            if not self._cancelled:
                self.progress.emit("Enriching GPS data...")
                merged.enrich_gps()

            # WiGLE API enrichment for remaining GPS-less BSSIDs
            if not self._cancelled:
                try:
                    from database.wigle_api import WigleApiClient
                    client = WigleApiClient()
                    if client.has_credentials():
                        missing = merged.get_networks_without_gps()
                        uncached = [b for b in missing if not client.is_cached(b)]
                        if uncached:
                            self.progress.emit(
                                f"WiGLE API: looking up {len(uncached)} BSSIDs...")
                            for i, bssid in enumerate(uncached):
                                if self._cancelled:
                                    break
                                self.progress.emit(
                                    f"WiGLE API: {i + 1}/{len(uncached)} — {bssid}")
                                result = client.lookup_bssid(bssid)
                                if result.found and result.lat != 0:
                                    merged.apply_wigle_result(
                                        bssid, result.lat, result.lon,
                                        result.ssid, result.channel,
                                        result.encryption)
                        # Also apply cached positive results for remaining missing
                        still_missing = merged.get_networks_without_gps()
                        for bssid in still_missing:
                            cached = client.get_cached(bssid)
                            if cached and cached.found and cached.lat != 0:
                                merged.apply_wigle_result(
                                    bssid, cached.lat, cached.lon,
                                    cached.ssid, cached.channel,
                                    cached.encryption)
                except Exception as e:
                    log.warning("WiGLE API enrichment failed: %s", e)

            self.finished.emit(merged, '')

        except Exception as e:
            log.exception("Pull worker error")
            self.finished.emit(None, str(e))

    def _delete_remote_files(self, config, source, result):
        """Delete successfully pulled files from the remote device."""
        try:
            cls = SOURCE_CONSTRUCTORS.get(config.source_type, DeviceSource)
            src = cls(config)
            sftp = src._get_sftp()
            remote_files = src.list_files()
            pulled_names = {Path(p).name for p in result.files_pulled}
            deleted = 0
            for rf in remote_files:
                if Path(rf.path).name in pulled_names:
                    try:
                        self.progress.emit(f"Deleting {Path(rf.path).name} from {config.name}...")
                        sftp.remove(rf.path)
                        deleted += 1
                    except Exception as e:
                        log.warning("Failed to delete %s: %s", rf.path, e)
            src._close()
            self.progress.emit(f"Deleted {deleted} files from {config.name}")
        except Exception as e:
            log.warning("Failed to clean up files on %s: %s", config.name, e)

    def _ingest_file(self, merged: MergedDatabase, path: str, source_name: str):
        p = Path(path)
        name_lower = p.name.lower()
        ext = p.suffix.lower()
        try:
            if ext == '.kismet':
                reader = KismetDBReader()
                reader.open_database(path)
                merged.ingest_kismet(reader, source_name)
                reader.close_database()
            elif ext in ('.pcap', '.pcapng', '.cap'):
                reader = PcapReader()
                reader.open_database(path)
                reader.parse()
                merged.ingest_pcap(reader, source_name, pcap_path=path)
            elif ext in ('.hc22000', '.22000'):
                reader = Hc22000Reader()
                reader.open_database(path)
                merged.ingest_hc22000(reader, source_name)
            elif ext == '.csv' or name_lower.endswith('.csv.gz'):
                reader = WigleCsvReader()
                reader.open_database(path)
                merged.ingest_wigle(reader, source_name)
            elif ext == '.zip':
                self._ingest_zip(merged, path, source_name)
            elif name_lower.endswith('.tar.gz') or ext == '.tgz':
                self._ingest_targz(merged, path, source_name)
        except Exception as e:
            log.warning("Failed to parse %s: %s", path, e)

    def _ingest_zip(self, merged: MergedDatabase, path: str, source_name: str):
        import zipfile, tempfile
        with zipfile.ZipFile(path, 'r') as zf:
            with tempfile.TemporaryDirectory() as tmpdir:
                zf.extractall(tmpdir)
                for f in Path(tmpdir).rglob('*'):
                    if f.is_file():
                        self._ingest_file(merged, str(f), source_name)

    def _ingest_targz(self, merged: MergedDatabase, path: str, source_name: str):
        import tarfile, tempfile
        with tarfile.open(path, 'r:gz') as tf:
            with tempfile.TemporaryDirectory() as tmpdir:
                tf.extractall(tmpdir, filter='data')
                for f in Path(tmpdir).rglob('*'):
                    if f.is_file():
                        self._ingest_file(merged, str(f), source_name)


class ConnectDialog(QDialog):
    """Main dialog for connecting to devices and pulling capture data."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Connect to Devices")
        self.setMinimumWidth(560)
        self.setMinimumHeight(480)
        self.setStyleSheet(_DARK_STYLE)

        self._source_widgets: list[_SourceWidget] = []
        self._probe_workers: list[_ProbeWorker] = []
        self._pull_worker: Optional[_PullWorker] = None
        self._local_files: list[str] = []
        self._result_db: Optional[MergedDatabase] = None

        self._setup_ui()
        self._load_sources()
        QTimer.singleShot(100, self._start_probes)

    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(8)

        # Header
        header = QHBoxLayout()
        title = QLabel("Connect to Devices")
        title.setFont(QFont('', 14, QFont.Weight.Bold))
        title.setStyleSheet("border: none; background: transparent;")
        header.addWidget(title)
        header.addStretch()

        add_btn = QPushButton("+ Add Source")
        add_btn.clicked.connect(self._add_source)
        header.addWidget(add_btn)

        refresh_btn = QPushButton("Refresh")
        refresh_btn.clicked.connect(self._start_probes)
        header.addWidget(refresh_btn)

        layout.addLayout(header)

        # Source list (scrollable)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        self._source_container = QWidget()
        self._source_layout = QVBoxLayout(self._source_container)
        self._source_layout.setSpacing(6)
        self._source_layout.addStretch()
        scroll.setWidget(self._source_container)
        layout.addWidget(scroll, 1)

        # Local files section
        local_group = QGroupBox("Local Files")
        local_layout = QHBoxLayout(local_group)
        self._local_label = QLabel("No files selected")
        self._local_label.setStyleSheet("color: #999; border: none; background: transparent;")
        local_layout.addWidget(self._local_label, 1)
        browse_btn = QPushButton("Browse...")
        browse_btn.clicked.connect(self._browse_local)
        local_layout.addWidget(browse_btn)
        layout.addWidget(local_group)

        # Options
        self._only_new_check = QCheckBox("Pull only new files (skip already seen)")
        self._only_new_check.setChecked(True)
        layout.addWidget(self._only_new_check)

        self._delete_after_check = QCheckBox("Delete files from devices after pull")
        self._delete_after_check.setChecked(False)
        layout.addWidget(self._delete_after_check)

        # Progress
        self._progress_bar = QProgressBar()
        self._progress_bar.setVisible(False)
        self._progress_bar.setRange(0, 0)  # indeterminate
        layout.addWidget(self._progress_bar)

        self._progress_label = QLabel("")
        self._progress_label.setStyleSheet("color: #999; border: none; background: transparent;")
        self._progress_label.setVisible(False)
        layout.addWidget(self._progress_label)

        # Buttons
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()

        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self._on_cancel)
        btn_layout.addWidget(cancel_btn)

        self._pull_btn = QPushButton("Pull && Merge")
        self._pull_btn.setStyleSheet(
            "QPushButton { background-color: #2ecc71; color: #1e1e1e; font-weight: bold; padding: 8px 24px; }"
            "QPushButton:hover { background-color: #27ae60; }"
            "QPushButton:disabled { background-color: #555; color: #999; }")
        self._pull_btn.clicked.connect(self._start_pull)
        btn_layout.addWidget(self._pull_btn)

        layout.addLayout(btn_layout)

    def _load_sources(self):
        configs = load_sources()
        for config in configs:
            self._add_source_widget(config)

    def _add_source_widget(self, config: SourceConfig):
        widget = _SourceWidget(config, self)
        self._source_widgets.append(widget)
        # Insert before the stretch
        self._source_layout.insertWidget(self._source_layout.count() - 1, widget)

    def _add_source(self):
        config = SourceConfig(
            source_type='custom',
            name='New Source',
            host='',
            remote_path='',
            file_types=['.pcap'],
        )
        dlg = _SourceEditorDialog(config, self)
        if dlg.exec() == QDialog.DialogCode.Accepted:
            new_config = dlg.get_config()
            self._add_source_widget(new_config)
            self._save_all_sources()

    def _save_all_sources(self):
        configs = [sw.config for sw in self._source_widgets]
        save_sources(configs)

    def _start_probes(self):
        # Clean up old workers
        for w in self._probe_workers:
            if w.isRunning():
                w.quit()
                w.wait(1000)
        self._probe_workers.clear()

        for i, sw in enumerate(self._source_widgets):
            if not sw.config.host:
                sw.set_offline("No host configured")
                continue
            sw.set_probing()
            worker = _ProbeWorker(i, sw.config)
            worker.result.connect(self._on_probe_result)
            self._probe_workers.append(worker)
            worker.start()

    def _on_probe_result(self, index: int, online: bool, files: list, error: str):
        if index >= len(self._source_widgets):
            return
        sw = self._source_widgets[index]
        if online:
            total_size = sum(f.size for f in files)
            sw.set_online(len(files), total_size, files)
        else:
            sw.set_offline(error)

    def _browse_local(self):
        paths, _ = QFileDialog.getOpenFileNames(
            self, "Select Local Files", "",
            "All Supported (*.kismet *.pcap *.pcapng *.cap *.csv *.gz *.zip *.tar.gz *.tgz *.hc22000 *.22000);;"
            "Kismet Database (*.kismet);;"
            "PCAP Files (*.pcap *.pcapng *.cap);;"
            "WiGLE CSV (*.csv *.gz);;"
            "Zip Archives (*.zip);;"
            "Tar Archives (*.tar.gz *.tgz);;"
            "Hashcat Hashes (*.hc22000 *.22000);;"
            "All Files (*)"
        )
        if paths:
            self._local_files = paths
            names = [Path(p).name for p in paths]
            if len(names) > 3:
                self._local_label.setText(f"{', '.join(names[:3])}... ({len(names)} files)")
            else:
                self._local_label.setText(', '.join(names))
            self._local_label.setStyleSheet("color: #e0e0e0; border: none; background: transparent;")

    def _start_pull(self):
        # Check if anything is selected
        has_sources = any(sw.check.isChecked() and sw._online for sw in self._source_widgets)
        if not has_sources and not self._local_files:
            QMessageBox.information(self, "Nothing Selected",
                                    "No online sources selected and no local files chosen.")
            return

        self._save_all_sources()
        self._pull_btn.setEnabled(False)
        self._progress_bar.setVisible(True)
        self._progress_label.setVisible(True)
        self._progress_label.setText("Starting...")
        self._pull_timer = QElapsedTimer()
        self._pull_timer.start()

        checked_sources = [sw for sw in self._source_widgets
                           if sw.check.isChecked() and sw._online]

        self._pull_worker = _PullWorker(
            checked_sources, self._local_files,
            self._only_new_check.isChecked(),
            self._delete_after_check.isChecked()
        )
        self._pull_worker.progress.connect(self._on_pull_progress)
        self._pull_worker.file_pulled.connect(self._on_file_pulled)
        self._pull_worker.byte_progress.connect(self._on_byte_progress)
        self._pull_worker.finished.connect(self._on_pull_finished)

        total = self._pull_worker._bytes_total
        if total > 0:
            self._progress_bar.setRange(0, 100)
            self._progress_bar.setValue(0)
        else:
            self._progress_bar.setRange(0, 0)

        self._pull_worker.start()

    def _on_pull_progress(self, message: str):
        self._progress_label.setText(message)

    def _on_file_pulled(self, source: str, filename: str):
        pass  # byte_progress handles the display now

    def _on_byte_progress(self, transferred: int, total: int):
        if transferred == -1:
            self._progress_bar.setRange(0, 0)  # indeterminate for parsing phase
            return
        if total <= 0:
            return

        # Files may grow while pulling (e.g. live Kismet logging) — adjust total upward
        if transferred > total:
            total = transferred

        pct = min(int(transferred * 100 / total), 100)
        self._progress_bar.setValue(pct)

        elapsed_ms = self._pull_timer.elapsed()
        if elapsed_ms < 1000 or transferred < 1:
            self._progress_label.setText(
                f"{self._fmt_size(transferred)} / {self._fmt_size(total)}  ({pct}%)")
            return

        speed = transferred / (elapsed_ms / 1000.0)
        remaining = max(0, total - transferred)
        if speed > 0 and remaining > 0:
            eta_secs = int(remaining / speed)
            if eta_secs >= 3600:
                eta_str = f"{eta_secs // 3600}h {(eta_secs % 3600) // 60}m"
            elif eta_secs >= 60:
                eta_str = f"{eta_secs // 60}m {eta_secs % 60}s"
            else:
                eta_str = f"{eta_secs}s"
            eta_part = f"  •  {eta_str} remaining"
        else:
            eta_part = ""

        self._progress_label.setText(
            f"{self._fmt_size(transferred)} / {self._fmt_size(total)}  "
            f"({pct}%)  •  {self._fmt_size(int(speed))}/s{eta_part}")

    @staticmethod
    def _fmt_size(nbytes: int) -> str:
        if nbytes >= 1_073_741_824:
            return f"{nbytes / 1_073_741_824:.1f} GB"
        if nbytes >= 1_048_576:
            return f"{nbytes / 1_048_576:.1f} MB"
        if nbytes >= 1024:
            return f"{nbytes / 1024:.0f} KB"
        return f"{nbytes} B"

    def _on_pull_finished(self, merged_db: Optional[MergedDatabase], error: str):
        stopped_sources = self._pull_worker._sources if self._pull_worker else []
        self._pull_worker = None
        self._progress_bar.setVisible(False)
        self._pull_btn.setEnabled(True)

        if error:
            self._progress_label.setText(f"Error: {error}")
            self._progress_label.setStyleSheet("color: #e74c3c; border: none; background: transparent;")
            QMessageBox.critical(self, "Error", f"Pull failed:\n{error}")
            self._offer_restart(stopped_sources)
            return

        if merged_db and merged_db.is_connected():
            self._result_db = merged_db
            info = merged_db.get_device_summary()
            self._progress_label.setText(
                f"Done: {info['access_points']} APs, {info['clients']} clients, "
                f"{info['handshakes']} handshakes, GPS for {info.get('gps_enriched', 0)}")
            self._progress_label.setStyleSheet("color: #2ecc71; border: none; background: transparent;")
            self._offer_restart(stopped_sources)
            self.accept()
        else:
            self._progress_label.setText("No data found.")
            self._progress_label.setStyleSheet("color: #f39c12; border: none; background: transparent;")
            self._offer_restart(stopped_sources)

    def _offer_restart(self, sources):
        """Ask user if they want to restart stopped capture services."""
        restartable = [
            (config, ft, sz) for config, ft, sz in sources
            if config.source_type in ('kismet', 'pager')
        ]
        if not restartable:
            return

        names = ', '.join(c.name for c, _, _ in restartable)
        reply = QMessageBox.question(
            self, "Restart Services",
            f"Transfer complete. Restart capture services?\n\n{names}",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.Yes
        )
        if reply == QMessageBox.StandardButton.Yes:
            for config, _, _ in restartable:
                self._restart_service(config)

    def _restart_service(self, config):
        """Restart a capture service in a background thread."""
        import paramiko
        def _do_restart():
            try:
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                key_path = Path(config.key_file).expanduser()
                kwargs = {
                    'hostname': config.host,
                    'port': config.port,
                    'username': config.user,
                    'timeout': 10,
                }
                if key_path.exists():
                    kwargs['key_filename'] = str(key_path)
                ssh.connect(**kwargs)
                if config.source_type == 'kismet':
                    ssh.exec_command('sudo systemctl start kismet', timeout=10)
                elif config.source_type == 'pager':
                    ssh.exec_command('/etc/init.d/pineapd start', timeout=10)
                ssh.close()
            except Exception as e:
                log.warning("Failed to restart %s: %s", config.name, e)

        worker = QThread()
        worker.run = _do_restart
        worker.start()
        # Keep reference so it doesn't get GC'd
        if not hasattr(self, '_restart_workers'):
            self._restart_workers = []
        self._restart_workers.append(worker)

    def _on_cancel(self):
        if self._pull_worker and self._pull_worker.isRunning():
            self._pull_worker.cancel()
            self._pull_worker.wait(3000)
        self.reject()

    def get_merged_database(self) -> Optional[MergedDatabase]:
        return self._result_db
