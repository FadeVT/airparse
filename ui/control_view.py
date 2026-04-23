"""Control view for managing wardriving gear over SSH."""

import json
import logging
from pathlib import Path

import shutil

from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QFrame, QCheckBox, QGroupBox, QGridLayout, QSizePolicy, QScrollArea,
    QTreeWidget, QTreeWidgetItem, QHeaderView, QMessageBox,
    QStackedWidget
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QTimer
from PyQt6.QtGui import QFont, QColor

import paramiko

from sources import load_sources, SourceConfig, PULL_DIR

log = logging.getLogger(__name__)

_CARD_STYLE = """
    QFrame {
        background-color: #333;
        border: 1px solid #444;
        border-radius: 6px;
        padding: 12px;
    }
"""

_LABEL_STYLE = "color: #e0e0e0; border: none; background: transparent;"
_DIM_STYLE = "color: #999; border: none; background: transparent; font-size: 10pt; font-family: 'Segoe UI';"
_GREEN = "color: #2ecc71; border: none; background: transparent;"
_RED = "color: #e74c3c; border: none; background: transparent;"
_YELLOW = "color: #f39c12; border: none; background: transparent;"


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


class _SshWorker(QThread):
    """Run an SSH command in the background."""
    finished = pyqtSignal(str, str, int)  # stdout, stderr, exit_code

    def __init__(self, config: SourceConfig, command: str):
        super().__init__()
        self._config = config
        self._command = command

    def run(self):
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            key_path = Path(self._config.key_file).expanduser()
            kwargs = {
                'hostname': self._config.host,
                'port': self._config.port,
                'username': self._config.user,
                'timeout': 10,
            }
            if key_path.exists():
                kwargs['key_filename'] = str(key_path)
            ssh.connect(**kwargs)
            _, stdout, stderr = ssh.exec_command(self._command, timeout=15)
            exit_code = stdout.channel.recv_exit_status()
            out = stdout.read().decode('utf-8', errors='replace').strip()
            err = stderr.read().decode('utf-8', errors='replace').strip()
            ssh.close()
            self.finished.emit(out, err, exit_code)
        except Exception as e:
            self.finished.emit('', str(e), -1)


class _GpsWorker(QThread):
    """Poll gpsd for fix status."""
    result = pyqtSignal(dict)  # parsed GPS info

    def __init__(self, config: SourceConfig):
        super().__init__()
        self._config = config

    def run(self):
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            key_path = Path(self._config.key_file).expanduser()
            kwargs = {
                'hostname': self._config.host,
                'port': self._config.port,
                'username': self._config.user,
                'timeout': 10,
            }
            if key_path.exists():
                kwargs['key_filename'] = str(key_path)
            ssh.connect(**kwargs)
            _, stdout, _ = ssh.exec_command(
                'timeout 4 gpspipe -w 2>/dev/null | head -20', timeout=10)
            lines = stdout.read().decode('utf-8', errors='replace').strip().split('\n')
            ssh.close()

            info = {'mode': 0, 'sats': 0, 'hdop': None, 'lat': None, 'lon': None}
            for line in lines:
                try:
                    obj = json.loads(line)
                except (json.JSONDecodeError, ValueError):
                    continue
                if obj.get('class') == 'TPV':
                    info['mode'] = obj.get('mode', 0)
                    info['lat'] = obj.get('lat')
                    info['lon'] = obj.get('lon')
                elif obj.get('class') == 'SKY':
                    info['sats'] = obj.get('uSat', 0)
                    if obj.get('hdop'):
                        info['hdop'] = obj['hdop']
            self.result.emit(info)
        except Exception as e:
            self.result.emit({'mode': -1, 'error': str(e)})


class _ListRemoteFilesWorker(QThread):
    """List files on a remote device via SFTP."""
    result = pyqtSignal(list)  # list of (path, size, mtime) tuples

    def __init__(self, config: SourceConfig, extra_dirs: list[str] = None):
        super().__init__()
        self._config = config
        self._extra_dirs = extra_dirs or []

    def run(self):
        files = []
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            key_path = Path(self._config.key_file).expanduser()
            kwargs = {
                'hostname': self._config.host,
                'port': self._config.port,
                'username': self._config.user,
                'timeout': 10,
            }
            if key_path.exists():
                kwargs['key_filename'] = str(key_path)
            ssh.connect(**kwargs)
            sftp = ssh.open_sftp()
            dirs = [self._config.remote_path.rstrip('/')]
            dirs.extend(self._extra_dirs)
            for d in dirs:
                try:
                    for entry in sftp.listdir_attr(d):
                        if not entry.filename.startswith('.') and not (entry.st_mode & 0o40000):
                            files.append((
                                f"{d}/{entry.filename}",
                                entry.st_size or 0,
                                entry.st_mtime or 0,
                            ))
                except Exception:
                    continue
            sftp.close()
            ssh.close()
        except Exception as e:
            log.warning("Failed to list remote files on %s: %s", self._config.name, e)
        self.result.emit(files)


class _DeleteRemoteFilesWorker(QThread):
    """Delete files from a remote device via SFTP."""
    progress = pyqtSignal(str)
    finished = pyqtSignal(int, int)  # deleted, failed

    def __init__(self, config: SourceConfig, paths: list[str]):
        super().__init__()
        self._config = config
        self._paths = paths

    def run(self):
        deleted = 0
        failed = 0
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            key_path = Path(self._config.key_file).expanduser()
            kwargs = {
                'hostname': self._config.host,
                'port': self._config.port,
                'username': self._config.user,
                'timeout': 10,
            }
            if key_path.exists():
                kwargs['key_filename'] = str(key_path)
            ssh.connect(**kwargs)
            sftp = ssh.open_sftp()
            for p in self._paths:
                try:
                    self.progress.emit(f"Deleting {Path(p).name}...")
                    sftp.remove(p)
                    deleted += 1
                except Exception as e:
                    log.warning("Failed to delete %s: %s", p, e)
                    failed += 1
            sftp.close()
            ssh.close()
        except Exception as e:
            log.warning("Remote delete connection failed: %s", e)
            failed = len(self._paths)
        self.finished.emit(deleted, failed)


class _LogConfigWorker(QThread):
    """Read kismet_logging.conf log_types over SSH."""
    result = pyqtSignal(list)  # list of active log types

    def __init__(self, config: SourceConfig):
        super().__init__()
        self._config = config

    def run(self):
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            key_path = Path(self._config.key_file).expanduser()
            kwargs = {
                'hostname': self._config.host,
                'port': self._config.port,
                'username': self._config.user,
                'timeout': 10,
            }
            if key_path.exists():
                kwargs['key_filename'] = str(key_path)
            ssh.connect(**kwargs)
            _, stdout, _ = ssh.exec_command(
                "grep '^log_types=' /etc/kismet/kismet_logging.conf", timeout=5)
            line = stdout.read().decode('utf-8', errors='replace').strip()
            ssh.close()
            if '=' in line:
                types = [t.strip() for t in line.split('=', 1)[1].split(',') if t.strip()]
                self.result.emit(types)
            else:
                self.result.emit([])
        except Exception as e:
            log.warning("Failed to read logging config: %s", e)
            self.result.emit([])


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


def _fmt_bytes(size: int) -> str:
    for unit in ('B', 'KB', 'MB', 'GB'):
        if size < 1024:
            return f"{size:.1f} {unit}" if unit != 'B' else f"{size} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


class ControlView(QWidget):
    """Control panel for managing wardriving gear."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._kismet_config: SourceConfig | None = None
        self._pager_config: SourceConfig | None = None
        self._workers: list[QThread] = []
        self._log_types: list[str] = []
        self._log_dirty = False
        self._setup_ui()

    PAGE_DEVICES = 0
    PAGE_KISMET_FILES = 1
    PAGE_PAGER_FILES = 2
    PAGE_LOCAL_STORAGE = 3
    PAGE_FILTERS = 4

    def _setup_ui(self):
        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)

        self._pages = QStackedWidget()
        self._pages.addWidget(self._build_devices_page())
        self._pages.addWidget(self._build_kismet_files_page())
        self._pages.addWidget(self._build_pager_files_page())
        self._pages.addWidget(self._build_local_storage_page())
        self._pages.addWidget(self._build_filters_page())

        outer.addWidget(self._pages)

    def show_page(self, index: int):
        self._pages.setCurrentIndex(index)
        if index == self.PAGE_LOCAL_STORAGE:
            self._refresh_storage()

    def _build_devices_page(self) -> QWidget:
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        scroll.setStyleSheet("QScrollArea { background: transparent; border: none; }")
        self._devices_scroll = scroll

        content = QWidget()
        layout = QVBoxLayout(content)
        layout.setSpacing(16)
        layout.setContentsMargins(16, 16, 16, 16)

        # ======== KISMET SECTION ========
        kismet_title = QLabel("Kismet RPi5")
        kismet_title.setFont(QFont('Segoe UI', 13, QFont.Weight.Bold))
        kismet_title.setStyleSheet(_LABEL_STYLE)
        layout.addWidget(kismet_title)

        self._kismet_conn_label = QLabel("Not connected")
        self._kismet_conn_label.setStyleSheet(_DIM_STYLE)
        layout.addWidget(self._kismet_conn_label)

        # --- Kismet Service ---
        svc_group = QGroupBox("Service")
        svc_group.setObjectName("kismet_service")
        svc_group.setStyleSheet(_GROUP_STYLE)
        svc_layout = QHBoxLayout(svc_group)

        self._kismet_status = QLabel("Unknown")
        self._kismet_status.setFont(QFont('Segoe UI', 11))
        self._kismet_status.setStyleSheet(_YELLOW)
        svc_layout.addWidget(self._kismet_status)
        svc_layout.addStretch()

        self._kismet_start_btn = _action_btn("Start Kismet", "#27ae60", "white", bold=True)
        self._kismet_start_btn.clicked.connect(self._start_kismet)
        svc_layout.addWidget(self._kismet_start_btn)

        self._kismet_stop_btn = _action_btn("Stop Kismet", "#c0392b", "white", bold=True)
        self._kismet_stop_btn.clicked.connect(self._stop_kismet)
        svc_layout.addWidget(self._kismet_stop_btn)

        layout.addWidget(svc_group)

        # --- GPS ---
        gps_group = QGroupBox("GPS")
        gps_group.setObjectName("kismet_gps")
        gps_group.setStyleSheet(_GROUP_STYLE)
        gps_layout = QGridLayout(gps_group)

        self._gps_fix_label = QLabel("Fix: Unknown")
        self._gps_fix_label.setFont(QFont('Segoe UI', 11))
        self._gps_fix_label.setStyleSheet(_YELLOW)
        gps_layout.addWidget(self._gps_fix_label, 0, 0)

        self._gps_sats_label = QLabel("")
        self._gps_sats_label.setStyleSheet(_DIM_STYLE)
        gps_layout.addWidget(self._gps_sats_label, 0, 1)

        self._gps_coords_label = QLabel("")
        self._gps_coords_label.setStyleSheet(_DIM_STYLE)
        gps_layout.addWidget(self._gps_coords_label, 1, 0, 1, 2)

        gps_btn_layout = QHBoxLayout()
        gps_btn_layout.addStretch()
        self._gps_refresh_btn = _action_btn("Refresh")
        self._gps_refresh_btn.clicked.connect(self._poll_gps)
        gps_btn_layout.addWidget(self._gps_refresh_btn)

        self._gps_restart_btn = _action_btn("Restart GPS", "#e67e22", "white", bold=True)
        self._gps_restart_btn.clicked.connect(self._restart_gps)
        gps_btn_layout.addWidget(self._gps_restart_btn)
        gps_layout.addLayout(gps_btn_layout, 2, 0, 1, 2)

        layout.addWidget(gps_group)

        # --- Logging config ---
        log_group = QGroupBox("Logging")
        log_group.setObjectName("kismet_logging")
        log_group.setStyleSheet(_GROUP_STYLE)
        log_layout = QVBoxLayout(log_group)

        self._log_checks: dict[str, QCheckBox] = {}
        log_types_row = QHBoxLayout()
        for ltype in ['kismet', 'wiglecsv', 'pcapng', 'pcapppi']:
            cb = QCheckBox(ltype)
            cb.setStyleSheet("color: #e0e0e0;")
            cb.toggled.connect(self._on_log_type_toggled)
            self._log_checks[ltype] = cb
            log_types_row.addWidget(cb)
        log_types_row.addStretch()
        log_layout.addLayout(log_types_row)

        self._restart_banner = QLabel("Restart Kismet for changes to take effect")
        self._restart_banner.setStyleSheet(
            "color: #f39c12; font-weight: bold; border: none; background: transparent;")
        self._restart_banner.setVisible(False)
        log_layout.addWidget(self._restart_banner)

        log_btn_layout = QHBoxLayout()
        log_btn_layout.addStretch()
        self._log_save_btn = _action_btn("Save Logging Config", "#2980b9", "white", bold=True)
        self._log_save_btn.setEnabled(False)
        self._log_save_btn.clicked.connect(self._save_log_config)
        log_btn_layout.addWidget(self._log_save_btn)
        log_layout.addLayout(log_btn_layout)

        layout.addWidget(log_group)

        # ======== PAGER SECTION ========
        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.HLine)
        sep.setStyleSheet("color: #444;")
        layout.addWidget(sep)

        pager_title = QLabel("Hak5 Pager")
        pager_title.setFont(QFont('Segoe UI', 13, QFont.Weight.Bold))
        pager_title.setStyleSheet(_LABEL_STYLE)
        layout.addWidget(pager_title)

        self._pager_conn_label = QLabel("Not connected")
        self._pager_conn_label.setStyleSheet(_DIM_STYLE)
        layout.addWidget(self._pager_conn_label)

        pager_svc_group = QGroupBox("Recon Service")
        pager_svc_group.setObjectName("pager_recon")
        pager_svc_group.setStyleSheet(_GROUP_STYLE)
        pager_svc_layout = QHBoxLayout(pager_svc_group)

        self._pager_status = QLabel("Unknown")
        self._pager_status.setFont(QFont('Segoe UI', 11))
        self._pager_status.setStyleSheet(_YELLOW)
        pager_svc_layout.addWidget(self._pager_status)
        pager_svc_layout.addStretch()

        self._pager_start_btn = _action_btn("Start Recon", "#27ae60", "white", bold=True)
        self._pager_start_btn.clicked.connect(self._start_pager)
        pager_svc_layout.addWidget(self._pager_start_btn)

        self._pager_stop_btn = _action_btn("Stop Recon", "#c0392b", "white", bold=True)
        self._pager_stop_btn.clicked.connect(self._stop_pager)
        pager_svc_layout.addWidget(self._pager_stop_btn)

        layout.addWidget(pager_svc_group)

        layout.addStretch()
        scroll.setWidget(content)
        return scroll

    def _build_remote_files_page(self, title: str, tree_attr: str,
                                 status_attr: str, scan_attr: str,
                                 delete_attr: str, scan_cb, delete_cb) -> QWidget:
        """Generic builder for a remote files page."""
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setSpacing(12)
        layout.setContentsMargins(16, 16, 16, 16)

        header = QLabel(title)
        header.setFont(QFont('Segoe UI', 13, QFont.Weight.Bold))
        header.setStyleSheet(_LABEL_STYLE)
        layout.addWidget(header)

        tree = QTreeWidget()
        tree.setHeaderLabels(["File", "Size"])
        tree.setAlternatingRowColors(True)
        tree.setStyleSheet(_TREE_STYLE)
        tree.setRootIsDecorated(False)
        tree.setFont(QFont('Segoe UI', 11))
        h = tree.header()
        h.setStretchLastSection(False)
        h.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        h.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        setattr(self, tree_attr, tree)
        layout.addWidget(tree, 1)

        status = QLabel("Click Scan Device to list remote files")
        status.setFont(QFont('Segoe UI', 10))
        status.setStyleSheet(_DIM_STYLE)
        setattr(self, status_attr, status)
        layout.addWidget(status)

        btn_row = QHBoxLayout()
        scan_btn = _action_btn("Scan Device")
        scan_btn.clicked.connect(scan_cb)
        setattr(self, scan_attr, scan_btn)
        btn_row.addWidget(scan_btn)

        select_btn = _action_btn("Select All")
        select_btn.clicked.connect(lambda: self._select_all_files(tree))
        btn_row.addWidget(select_btn)

        deselect_btn = _action_btn("Deselect All")
        deselect_btn.clicked.connect(lambda: self._deselect_all_files(tree))
        btn_row.addWidget(deselect_btn)

        btn_row.addStretch()
        del_btn = _action_btn("Delete Selected", "#c0392b", "white", bold=True)
        del_btn.clicked.connect(delete_cb)
        del_btn.setEnabled(False)
        setattr(self, delete_attr, del_btn)
        btn_row.addWidget(del_btn)
        layout.addLayout(btn_row)

        return page

    def _build_kismet_files_page(self) -> QWidget:
        return self._build_remote_files_page(
            "Kismet RPi5 — Remote Files",
            '_kismet_files_tree', '_kismet_files_status',
            '_kismet_scan_btn', '_kismet_delete_btn',
            self._scan_kismet_files, self._delete_kismet_files)

    def _build_pager_files_page(self) -> QWidget:
        return self._build_remote_files_page(
            "Hak5 Pager — Remote Files",
            '_pager_files_tree', '_pager_files_status',
            '_pager_scan_btn', '_pager_delete_btn',
            self._scan_pager_files, self._delete_pager_files)

    def _build_local_storage_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setSpacing(12)
        layout.setContentsMargins(16, 16, 16, 16)

        header = QLabel("Local Storage")
        header.setFont(QFont('Segoe UI', 13, QFont.Weight.Bold))
        header.setStyleSheet(_LABEL_STYLE)
        layout.addWidget(header)

        self._storage_tree = QTreeWidget()
        self._storage_tree.setHeaderLabels(["Source / File", "Size", "Files"])
        self._storage_tree.setAlternatingRowColors(True)
        self._storage_tree.setStyleSheet(_TREE_STYLE)
        self._storage_tree.setRootIsDecorated(True)
        self._storage_tree.setFont(QFont('Segoe UI', 11))
        h = self._storage_tree.header()
        h.setStretchLastSection(False)
        h.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        h.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        h.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        layout.addWidget(self._storage_tree, 1)

        self._storage_status = QLabel("")
        self._storage_status.setFont(QFont('Segoe UI', 10))
        self._storage_status.setStyleSheet(_DIM_STYLE)
        layout.addWidget(self._storage_status)

        btn_row = QHBoxLayout()
        self._storage_refresh_btn = _action_btn("Refresh")
        self._storage_refresh_btn.clicked.connect(self._refresh_storage)
        btn_row.addWidget(self._storage_refresh_btn)

        self._storage_select_all_btn = _action_btn("Select All")
        self._storage_select_all_btn.clicked.connect(self._storage_select_all)
        btn_row.addWidget(self._storage_select_all_btn)

        self._storage_deselect_btn = _action_btn("Deselect All")
        self._storage_deselect_btn.clicked.connect(self._storage_deselect_all)
        btn_row.addWidget(self._storage_deselect_btn)

        btn_row.addStretch()

        self._clean_uploaded_btn = _action_btn("Clean Uploaded WiGLE CSVs")
        self._clean_uploaded_btn.clicked.connect(self._clean_uploaded)
        btn_row.addWidget(self._clean_uploaded_btn)

        self._storage_delete_btn = _action_btn("Delete Selected", "#c0392b", "white", bold=True)
        self._storage_delete_btn.clicked.connect(self._delete_selected_storage)
        self._storage_delete_btn.setEnabled(False)
        btn_row.addWidget(self._storage_delete_btn)
        layout.addLayout(btn_row)

        return page

    def _build_filters_page(self) -> QWidget:
        from ui.wigle_view import WigleView

        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setSpacing(16)
        layout.setContentsMargins(16, 16, 16, 16)

        header = QLabel("MAC Filter Lists")
        header.setFont(QFont('Segoe UI', 13, QFont.Weight.Bold))
        header.setStyleSheet(_LABEL_STYLE)
        layout.addWidget(header)

        desc = QLabel(
            "Manage MAC address filter lists used across AirParse. "
            "The blocklist strips devices from WiGLE uploads and Kismet captures. "
            "The watchlist highlights devices of interest in device tables."
        )
        desc.setWordWrap(True)
        desc.setStyleSheet(_DIM_STYLE)
        layout.addWidget(desc)

        # --- Blocklist card ---
        block_card = QFrame()
        block_card.setStyleSheet(_CARD_STYLE)
        block_layout = QVBoxLayout(block_card)

        block_title = QLabel("Blocklist")
        block_title.setFont(QFont('Segoe UI', 12, QFont.Weight.Bold))
        block_title.setStyleSheet(_LABEL_STYLE)
        block_layout.addWidget(block_title)

        block_desc = QLabel(
            "Devices filtered from WiGLE CSV uploads and Kismet capture logging. "
            "Your travel kit (car WiFi, router, RPi, phones) goes here."
        )
        block_desc.setWordWrap(True)
        block_desc.setStyleSheet(_DIM_STYLE)
        block_layout.addWidget(block_desc)

        self._block_count_label = QLabel()
        self._block_count_label.setStyleSheet(_LABEL_STYLE)
        block_layout.addWidget(self._block_count_label)

        block_btn_row = QHBoxLayout()
        edit_block_btn = _action_btn("Edit Blocklist", "#8e44ad", "white", bold=True)
        edit_block_btn.clicked.connect(lambda: self._open_list_editor('blocklist'))
        block_btn_row.addWidget(edit_block_btn)
        block_btn_row.addStretch()
        block_layout.addLayout(block_btn_row)

        layout.addWidget(block_card)

        # --- Watchlist card ---
        watch_card = QFrame()
        watch_card.setStyleSheet(_CARD_STYLE)
        watch_layout = QVBoxLayout(watch_card)

        watch_title = QLabel("Watchlist")
        watch_title.setFont(QFont('Segoe UI', 12, QFont.Weight.Bold))
        watch_title.setStyleSheet(_LABEL_STYLE)
        watch_layout.addWidget(watch_title)

        watch_desc = QLabel(
            "Devices highlighted in orange across all device tables. "
            "Use this for OUIs or MACs you want to track — interesting vendors, "
            "known targets, or devices of interest."
        )
        watch_desc.setWordWrap(True)
        watch_desc.setStyleSheet(_DIM_STYLE)
        watch_layout.addWidget(watch_desc)

        self._watch_count_label = QLabel()
        self._watch_count_label.setStyleSheet(_LABEL_STYLE)
        watch_layout.addWidget(self._watch_count_label)

        watch_btn_row = QHBoxLayout()
        edit_watch_btn = _action_btn("Edit Watchlist", "#e67e22", "white", bold=True)
        edit_watch_btn.clicked.connect(lambda: self._open_list_editor('watchlist'))
        watch_btn_row.addWidget(edit_watch_btn)
        watch_btn_row.addStretch()
        watch_layout.addLayout(watch_btn_row)

        layout.addWidget(watch_card)
        layout.addStretch()

        self._refresh_filter_counts()
        return page

    def _refresh_filter_counts(self):
        from ui.wigle_view import WigleView
        bl = WigleView._parse_list_file(WigleView._BLOCKLIST_PATH)
        wl = WigleView._parse_list_file(WigleView._WATCHLIST_PATH)
        if hasattr(self, '_block_count_label'):
            full = sum(1 for _, _, t in bl if t == "Full MAC")
            oui = sum(1 for _, _, t in bl if t == "OUI Prefix")
            self._block_count_label.setText(f"{len(bl)} entries ({full} MACs, {oui} OUI prefixes)")
        if hasattr(self, '_watch_count_label'):
            full = sum(1 for _, _, t in wl if t == "Full MAC")
            oui = sum(1 for _, _, t in wl if t == "OUI Prefix")
            self._watch_count_label.setText(f"{len(wl)} entries ({full} MACs, {oui} OUI prefixes)")

    def _open_list_editor(self, which: str):
        from ui.wigle_view import WigleView
        view = WigleView.__new__(WigleView)
        QWidget.__init__(view, self)
        if which == 'blocklist':
            view._show_blocklist_editor()
        else:
            view._show_watchlist_editor()
        self._refresh_filter_counts()

    def scroll_to_section(self, object_name: str):
        for i in range(self._pages.count()):
            page = self._pages.widget(i)
            target = page
            if hasattr(page, 'widget'):
                target = page.widget()
            widget = target.findChild(QGroupBox, object_name)
            if widget:
                self._pages.setCurrentIndex(i)
                if hasattr(page, 'ensureWidgetVisible'):
                    page.ensureWidgetVisible(widget, 0, 50)
                break

    def showEvent(self, event):
        super().showEvent(event)
        self._find_sources()
        if self._kismet_config:
            self._poll_kismet_all()
        if self._pager_config:
            self._poll_pager_status()
        self._refresh_storage()

    def _find_sources(self):
        sources = load_sources()
        self._kismet_config = None
        self._pager_config = None
        for src in sources:
            if src.source_type == 'kismet' and src.host and src.enabled:
                self._kismet_config = src
                self._kismet_conn_label.setText(f"Connected to {src.host} as {src.user}")
                self._kismet_conn_label.setStyleSheet(
                    "color: #2ecc71; border: none; background: transparent; font-size: 11px;")
            elif src.source_type == 'pager' and src.host and src.enabled:
                self._pager_config = src
                self._pager_conn_label.setText(f"Connected to {src.host} as {src.user}")
                self._pager_conn_label.setStyleSheet(
                    "color: #2ecc71; border: none; background: transparent; font-size: 11px;")
        if not self._kismet_config:
            self._kismet_conn_label.setText("No Kismet source configured — add one via Connect")
            self._kismet_conn_label.setStyleSheet(
                "color: #e74c3c; border: none; background: transparent; font-size: 11px;")
        if not self._pager_config:
            self._pager_conn_label.setText("No Pager source configured — add one via Connect")
            self._pager_conn_label.setStyleSheet(
                "color: #e74c3c; border: none; background: transparent; font-size: 11px;")

    def _poll_kismet_all(self):
        self._poll_kismet_status()
        self._poll_gps()
        self._poll_log_config()

    def _run_ssh(self, config: SourceConfig, command: str, callback):
        if not config:
            return
        worker = _SshWorker(config, command)
        worker.finished.connect(callback)
        worker.finished.connect(lambda *_: self._workers.remove(worker))
        self._workers.append(worker)
        worker.start()

    # --- Kismet service ---

    def _poll_kismet_status(self):
        self._run_ssh(self._kismet_config, "sudo systemctl is-active kismet", self._on_kismet_status)

    def _on_kismet_status(self, stdout: str, stderr: str, code: int):
        if code == -1:
            self._kismet_status.setText("⚠  Connection failed")
            self._kismet_status.setStyleSheet(_RED)
            return
        status = stdout.strip()
        if status == 'active':
            self._kismet_status.setText("●  Running")
            self._kismet_status.setStyleSheet(_GREEN)
            self._kismet_start_btn.setEnabled(False)
            self._kismet_stop_btn.setEnabled(True)
        else:
            self._kismet_status.setText("●  Stopped")
            self._kismet_status.setStyleSheet(_RED)
            self._kismet_start_btn.setEnabled(True)
            self._kismet_stop_btn.setEnabled(False)

    def _start_kismet(self):
        self._kismet_status.setText("●  Starting...")
        self._kismet_status.setStyleSheet(_YELLOW)
        self._kismet_start_btn.setEnabled(False)
        self._run_ssh(self._kismet_config, "sudo systemctl restart kismet", self._on_kismet_action_done)

    def _stop_kismet(self):
        self._kismet_status.setText("●  Stopping...")
        self._kismet_status.setStyleSheet(_YELLOW)
        self._kismet_stop_btn.setEnabled(False)
        self._run_ssh(self._kismet_config, "sudo systemctl stop kismet", self._on_kismet_action_done)

    def _on_kismet_action_done(self, stdout: str, stderr: str, code: int):
        QTimer.singleShot(1000, self._poll_kismet_status)

    # --- GPS ---

    def _poll_gps(self):
        if not self._kismet_config:
            return
        self._gps_fix_label.setText("Fix: Polling...")
        self._gps_fix_label.setStyleSheet(_YELLOW)
        worker = _GpsWorker(self._kismet_config)
        worker.result.connect(self._on_gps_result)
        worker.result.connect(lambda *_: self._workers.remove(worker))
        self._workers.append(worker)
        worker.start()

    def _on_gps_result(self, info: dict):
        if info.get('mode', 0) == -1:
            self._gps_fix_label.setText(f"Fix: Error — {info.get('error', 'unknown')}")
            self._gps_fix_label.setStyleSheet(_RED)
            self._gps_sats_label.setText("")
            self._gps_coords_label.setText("")
            return

        mode = info.get('mode', 0)
        sats = info.get('sats', 0)

        if mode >= 3:
            self._gps_fix_label.setText("Fix: 3D Fix")
            self._gps_fix_label.setStyleSheet(_GREEN)
        elif mode == 2:
            self._gps_fix_label.setText("Fix: 2D Fix")
            self._gps_fix_label.setStyleSheet(_YELLOW)
        else:
            self._gps_fix_label.setText("Fix: No Fix")
            self._gps_fix_label.setStyleSheet(_RED)

        self._gps_sats_label.setText(f"{sats} satellites")
        hdop = info.get('hdop')
        hdop_str = f"  •  HDOP: {hdop:.1f}" if hdop else ""
        self._gps_sats_label.setText(f"{sats} satellites{hdop_str}")

        lat, lon = info.get('lat'), info.get('lon')
        if lat is not None and lon is not None:
            self._gps_coords_label.setText(f"{lat:.6f}, {lon:.6f}")
        else:
            self._gps_coords_label.setText("")

    def _restart_gps(self):
        self._gps_fix_label.setText("Fix: Restarting...")
        self._gps_fix_label.setStyleSheet(_YELLOW)
        self._gps_restart_btn.setEnabled(False)
        cmd = (
            "sudo systemctl stop gpsd gpsd.socket && "
            "sudo rm -rf /run/gpsd.sock && "
            "sudo systemctl start gpsd"
        )
        self._run_ssh(self._kismet_config, cmd, self._on_gps_restart_done)

    def _on_gps_restart_done(self, stdout: str, stderr: str, code: int):
        self._gps_restart_btn.setEnabled(True)
        if code == 0:
            QTimer.singleShot(3000, self._poll_gps)
        else:
            self._gps_fix_label.setText(f"Fix: Restart failed — {stderr or 'unknown error'}")
            self._gps_fix_label.setStyleSheet(_RED)

    # --- Logging config ---

    def _poll_log_config(self):
        if not self._kismet_config:
            return
        worker = _LogConfigWorker(self._kismet_config)
        worker.result.connect(self._on_log_config_result)
        worker.result.connect(lambda *_: self._workers.remove(worker))
        self._workers.append(worker)
        worker.start()

    def _on_log_config_result(self, types: list[str]):
        self._log_types = types
        self._log_dirty = False
        self._restart_banner.setVisible(False)
        self._log_save_btn.setEnabled(False)
        for name, cb in self._log_checks.items():
            cb.blockSignals(True)
            cb.setChecked(name in types)
            cb.blockSignals(False)

    def _on_log_type_toggled(self, checked: bool):
        current = [name for name, cb in self._log_checks.items() if cb.isChecked()]
        self._log_dirty = sorted(current) != sorted(self._log_types)
        self._log_save_btn.setEnabled(self._log_dirty)
        self._restart_banner.setVisible(self._log_dirty)

    def _save_log_config(self):
        if not self._kismet_config:
            return
        new_types = [name for name, cb in self._log_checks.items() if cb.isChecked()]
        if not new_types:
            return
        types_str = ','.join(new_types)
        cmd = (
            f"sudo sed -i 's/^log_types=.*/log_types={types_str}/' "
            f"/etc/kismet/kismet_logging.conf"
        )
        self._log_save_btn.setEnabled(False)
        self._run_ssh(self._kismet_config, cmd, self._on_log_config_saved)

    def _on_log_config_saved(self, stdout: str, stderr: str, code: int):
        if code == 0:
            self._poll_log_config()
            self._restart_banner.setVisible(True)
        else:
            self._log_save_btn.setEnabled(True)
            log.warning("Failed to save logging config: %s", stderr)

    # --- Pager service ---

    def _poll_pager_status(self):
        self._run_ssh(self._pager_config,
                      "pidof pineapd > /dev/null 2>&1 && echo running || echo stopped",
                      self._on_pager_status)

    def _on_pager_status(self, stdout: str, stderr: str, code: int):
        if code == -1:
            self._pager_status.setText("⚠  Connection failed")
            self._pager_status.setStyleSheet(_RED)
            return
        status = stdout.strip()
        if status == 'running':
            self._pager_status.setText("●  Running")
            self._pager_status.setStyleSheet(_GREEN)
            self._pager_start_btn.setEnabled(False)
            self._pager_stop_btn.setEnabled(True)
        else:
            self._pager_status.setText("●  Stopped")
            self._pager_status.setStyleSheet(_RED)
            self._pager_start_btn.setEnabled(True)
            self._pager_stop_btn.setEnabled(False)

    def _start_pager(self):
        self._pager_status.setText("●  Starting...")
        self._pager_status.setStyleSheet(_YELLOW)
        self._pager_start_btn.setEnabled(False)
        self._run_ssh(self._pager_config, "/etc/init.d/pineapd start",
                      self._on_pager_action_done)

    def _stop_pager(self):
        self._pager_status.setText("●  Stopping...")
        self._pager_status.setStyleSheet(_YELLOW)
        self._pager_stop_btn.setEnabled(False)
        self._run_ssh(self._pager_config, "/etc/init.d/pineapd stop",
                      self._on_pager_action_done)

    def _on_pager_action_done(self, stdout: str, stderr: str, code: int):
        QTimer.singleShot(1000, self._poll_pager_status)

    # --- Remote file management helpers ---

    @staticmethod
    def _select_all_files(tree: QTreeWidget):
        for i in range(tree.topLevelItemCount()):
            tree.topLevelItem(i).setCheckState(0, Qt.CheckState.Checked)

    @staticmethod
    def _deselect_all_files(tree: QTreeWidget):
        for i in range(tree.topLevelItemCount()):
            tree.topLevelItem(i).setCheckState(0, Qt.CheckState.Unchecked)

    def _get_checked_paths(self, tree: QTreeWidget) -> list[str]:
        paths = []
        for i in range(tree.topLevelItemCount()):
            item = tree.topLevelItem(i)
            if item.checkState(0) == Qt.CheckState.Checked:
                paths.append(item.data(0, Qt.ItemDataRole.UserRole))
        return paths

    def _populate_file_tree(self, tree: QTreeWidget, files: list[tuple],
                            status_label: QLabel, delete_btn: QPushButton):
        tree.clear()
        from datetime import datetime
        total_size = 0
        for path, size, mtime in sorted(files, key=lambda f: f[2], reverse=True):
            name = Path(path).name
            item = QTreeWidgetItem([name, _fmt_bytes(size)])
            item.setData(0, Qt.ItemDataRole.UserRole, path)
            item.setCheckState(0, Qt.CheckState.Unchecked)
            total_size += size
            tree.addTopLevelItem(item)
        count = len(files)
        status_label.setText(f"{count} file{'s' if count != 1 else ''} — {_fmt_bytes(total_size)}")
        delete_btn.setEnabled(count > 0)
        tree.itemChanged.connect(
            lambda: delete_btn.setEnabled(bool(self._get_checked_paths(tree))))

    def _confirm_delete(self, count: int, target: str) -> bool:
        reply = QMessageBox.warning(
            self, "Confirm Delete",
            f"Delete {count} file{'s' if count != 1 else ''} from {target}?\n\n"
            "This cannot be undone.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No)
        return reply == QMessageBox.StandardButton.Yes

    # --- Kismet remote files ---

    def _scan_kismet_files(self):
        if not self._kismet_config:
            return
        self._kismet_scan_btn.setEnabled(False)
        self._kismet_scan_btn.setText("Scanning...")
        worker = _ListRemoteFilesWorker(self._kismet_config)
        worker.result.connect(self._on_kismet_files_listed)
        worker.result.connect(lambda _: self._workers.remove(worker))
        self._workers.append(worker)
        worker.start()

    def _on_kismet_files_listed(self, files: list):
        self._kismet_scan_btn.setEnabled(True)
        self._kismet_scan_btn.setText("Scan Device")
        self._populate_file_tree(
            self._kismet_files_tree, files,
            self._kismet_files_status, self._kismet_delete_btn)

    def _delete_kismet_files(self):
        paths = self._get_checked_paths(self._kismet_files_tree)
        if not paths or not self._confirm_delete(len(paths), self._kismet_config.name):
            return
        self._kismet_delete_btn.setEnabled(False)
        self._kismet_delete_btn.setText("Deleting...")
        worker = _DeleteRemoteFilesWorker(self._kismet_config, paths)
        worker.finished.connect(self._on_kismet_files_deleted)
        worker.finished.connect(lambda *_: self._workers.remove(worker))
        self._workers.append(worker)
        worker.start()

    def _on_kismet_files_deleted(self, deleted: int, failed: int):
        self._kismet_delete_btn.setText("Delete Selected")
        msg = f"Deleted {deleted} file{'s' if deleted != 1 else ''}"
        if failed:
            msg += f", {failed} failed"
        self._kismet_files_status.setText(msg)
        self._scan_kismet_files()

    # --- Pager remote files ---

    def _scan_pager_files(self):
        if not self._pager_config:
            return
        self._pager_scan_btn.setEnabled(False)
        self._pager_scan_btn.setText("Scanning...")
        extra_dirs = []
        base = self._pager_config.remote_path.rstrip('/')
        extra_dirs.extend([f"{base}/../wigle", f"{base}/wigle", '/root/wigle'])
        worker = _ListRemoteFilesWorker(self._pager_config, extra_dirs)
        worker.result.connect(self._on_pager_files_listed)
        worker.result.connect(lambda _: self._workers.remove(worker))
        self._workers.append(worker)
        worker.start()

    def _on_pager_files_listed(self, files: list):
        self._pager_scan_btn.setEnabled(True)
        self._pager_scan_btn.setText("Scan Device")
        self._populate_file_tree(
            self._pager_files_tree, files,
            self._pager_files_status, self._pager_delete_btn)

    def _delete_pager_files(self):
        paths = self._get_checked_paths(self._pager_files_tree)
        if not paths or not self._confirm_delete(len(paths), self._pager_config.name):
            return
        self._pager_delete_btn.setEnabled(False)
        self._pager_delete_btn.setText("Deleting...")
        worker = _DeleteRemoteFilesWorker(self._pager_config, paths)
        worker.finished.connect(self._on_pager_files_deleted)
        worker.finished.connect(lambda *_: self._workers.remove(worker))
        self._workers.append(worker)
        worker.start()

    def _on_pager_files_deleted(self, deleted: int, failed: int):
        self._pager_delete_btn.setText("Delete Selected")
        msg = f"Deleted {deleted} file{'s' if deleted != 1 else ''}"
        if failed:
            msg += f", {failed} failed"
        self._pager_files_status.setText(msg)
        self._scan_pager_files()

    # --- Local storage management ---

    def _refresh_storage(self):
        self._storage_tree.blockSignals(True)
        self._storage_tree.clear()
        total_size = 0
        total_files = 0

        # Pulled files by source
        if PULL_DIR.exists():
            for source_dir in sorted(PULL_DIR.iterdir()):
                if not source_dir.is_dir():
                    continue
                files = list(source_dir.rglob('*'))
                files = [f for f in files if f.is_file()]
                dir_size = sum(f.stat().st_size for f in files)
                total_size += dir_size
                total_files += len(files)

                source_item = QTreeWidgetItem([
                    source_dir.name, _fmt_bytes(dir_size), str(len(files))])
                source_item.setData(0, Qt.ItemDataRole.UserRole, str(source_dir))
                for f in sorted(files, key=lambda x: x.stat().st_mtime, reverse=True):
                    rel = f.relative_to(source_dir)
                    child = QTreeWidgetItem([str(rel), _fmt_bytes(f.stat().st_size), ""])
                    child.setData(0, Qt.ItemDataRole.UserRole, str(f))
                    child.setCheckState(0, Qt.CheckState.Unchecked)
                    source_item.addChild(child)
                self._storage_tree.addTopLevelItem(source_item)

        # WiGLE upload staging
        stage_dir = Path.home() / '.config' / 'airparse' / 'wigle_uploads'
        if stage_dir.exists():
            staged = [f for f in stage_dir.iterdir() if f.is_file()]
            uploaded_dir = stage_dir / 'uploaded'
            uploaded = list(uploaded_dir.iterdir()) if uploaded_dir.exists() else []
            stage_size = sum(f.stat().st_size for f in staged)
            uploaded_size = sum(f.stat().st_size for f in uploaded)

            wigle_item = QTreeWidgetItem([
                "WiGLE Uploads",
                _fmt_bytes(stage_size + uploaded_size),
                f"{len(staged)} staged, {len(uploaded)} uploaded"])
            wigle_item.setData(0, Qt.ItemDataRole.UserRole, str(stage_dir))

            for f in staged:
                child = QTreeWidgetItem([f.name, _fmt_bytes(f.stat().st_size), "staged"])
                child.setData(0, Qt.ItemDataRole.UserRole, str(f))
                child.setCheckState(0, Qt.CheckState.Unchecked)
                child.setForeground(2, QColor('#f39c12'))
                wigle_item.addChild(child)
            for f in uploaded:
                child = QTreeWidgetItem([f.name, _fmt_bytes(f.stat().st_size), "uploaded"])
                child.setData(0, Qt.ItemDataRole.UserRole, str(f))
                child.setCheckState(0, Qt.CheckState.Unchecked)
                child.setForeground(2, QColor('#2ecc71'))
                wigle_item.addChild(child)

            self._storage_tree.addTopLevelItem(wigle_item)
            total_size += stage_size + uploaded_size
            total_files += len(staged) + len(uploaded)

        # KML files
        kml_dir = Path.home() / 'AirParse' / 'Wigle'
        if kml_dir.exists():
            kml_files = sorted(
                [f for f in kml_dir.iterdir() if f.is_file() and f.suffix.lower() == '.kml'],
                key=lambda x: x.stat().st_mtime, reverse=True)
            if kml_files:
                kml_size = sum(f.stat().st_size for f in kml_files)
                kml_item = QTreeWidgetItem([
                    "WiGLE KML Files",
                    _fmt_bytes(kml_size),
                    str(len(kml_files))])
                kml_item.setData(0, Qt.ItemDataRole.UserRole, str(kml_dir))
                for f in kml_files:
                    child = QTreeWidgetItem([f.name, _fmt_bytes(f.stat().st_size), ""])
                    child.setData(0, Qt.ItemDataRole.UserRole, str(f))
                    child.setCheckState(0, Qt.CheckState.Unchecked)
                    kml_item.addChild(child)
                self._storage_tree.addTopLevelItem(kml_item)
                total_size += kml_size
                total_files += len(kml_files)

        self._storage_tree.blockSignals(False)
        try:
            self._storage_tree.itemChanged.disconnect(self._on_storage_item_changed)
        except TypeError:
            pass
        self._storage_tree.itemChanged.connect(self._on_storage_item_changed)
        self._storage_status.setText(
            f"Total: {_fmt_bytes(total_size)} across {total_files} files")
        self._storage_delete_btn.setEnabled(False)

    def _clean_uploaded(self):
        uploaded_dir = Path.home() / '.config' / 'airparse' / 'wigle_uploads' / 'uploaded'
        if not uploaded_dir.exists():
            self._storage_status.setText("No uploaded files to clean")
            return
        files = list(uploaded_dir.iterdir())
        if not files:
            self._storage_status.setText("No uploaded files to clean")
            return
        reply = QMessageBox.question(
            self, "Clean Uploaded Files",
            f"Remove {len(files)} already-uploaded WiGLE CSV{'s' if len(files) != 1 else ''}?\n\n"
            "These have already been uploaded to WiGLE.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No)
        if reply != QMessageBox.StandardButton.Yes:
            return
        count = 0
        for f in files:
            try:
                f.unlink()
                count += 1
            except Exception as e:
                log.warning("Failed to remove %s: %s", f.name, e)
        self._storage_status.setText(f"Cleaned {count} uploaded file{'s' if count != 1 else ''}")
        self._refresh_storage()

    def _clean_pulls(self):
        if not PULL_DIR.exists():
            self._storage_status.setText("No pulled files to clean")
            return
        dirs = [d for d in PULL_DIR.iterdir() if d.is_dir()]
        if not dirs:
            self._storage_status.setText("No pulled files to clean")
            return
        total = sum(1 for d in dirs for f in d.rglob('*') if f.is_file())
        reply = QMessageBox.warning(
            self, "Clean All Pulled Files",
            f"Delete all {total} pulled files from {len(dirs)} source{'s' if len(dirs) != 1 else ''}?\n\n"
            "This removes local copies only — remote files are not affected.\n"
            "This cannot be undone.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No)
        if reply != QMessageBox.StandardButton.Yes:
            return
        for d in dirs:
            try:
                shutil.rmtree(d)
            except Exception as e:
                log.warning("Failed to clean %s: %s", d.name, e)
        self._storage_status.setText(f"Cleaned {total} files from {len(dirs)} sources")
        self._refresh_storage()

    def _get_checked_storage_files(self) -> list[str]:
        paths = []
        for i in range(self._storage_tree.topLevelItemCount()):
            parent = self._storage_tree.topLevelItem(i)
            for j in range(parent.childCount()):
                child = parent.child(j)
                if child.checkState(0) == Qt.CheckState.Checked:
                    path = child.data(0, Qt.ItemDataRole.UserRole)
                    if path:
                        paths.append(path)
        return paths

    def _on_storage_item_changed(self, item, column):
        checked = self._get_checked_storage_files()
        self._storage_delete_btn.setEnabled(bool(checked))
        if checked:
            total = sum(Path(p).stat().st_size for p in checked if Path(p).exists())
            self._storage_delete_btn.setText(
                f"Delete Selected ({len(checked)} — {_fmt_bytes(total)})")
        else:
            self._storage_delete_btn.setText("Delete Selected")

    def _storage_select_all(self):
        self._storage_tree.blockSignals(True)
        for i in range(self._storage_tree.topLevelItemCount()):
            parent = self._storage_tree.topLevelItem(i)
            for j in range(parent.childCount()):
                parent.child(j).setCheckState(0, Qt.CheckState.Checked)
        self._storage_tree.blockSignals(False)
        self._on_storage_item_changed(None, 0)

    def _storage_deselect_all(self):
        self._storage_tree.blockSignals(True)
        for i in range(self._storage_tree.topLevelItemCount()):
            parent = self._storage_tree.topLevelItem(i)
            for j in range(parent.childCount()):
                parent.child(j).setCheckState(0, Qt.CheckState.Unchecked)
        self._storage_tree.blockSignals(False)
        self._on_storage_item_changed(None, 0)

    def _delete_selected_storage(self):
        paths = self._get_checked_storage_files()
        if not paths:
            return
        reply = QMessageBox.warning(
            self, "Delete Selected Files",
            f"Delete {len(paths)} file{'s' if len(paths) != 1 else ''}?\n\n"
            "This cannot be undone.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No)
        if reply != QMessageBox.StandardButton.Yes:
            return
        deleted = 0
        for p in paths:
            try:
                Path(p).unlink()
                deleted += 1
            except Exception as e:
                log.warning("Failed to delete %s: %s", p, e)
        self._storage_status.setText(
            f"Deleted {deleted} file{'s' if deleted != 1 else ''}")
        self._refresh_storage()
