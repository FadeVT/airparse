"""Control view for managing wardriving gear over SSH."""

import json
import logging
from pathlib import Path

from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QFrame, QCheckBox, QGroupBox, QGridLayout, QSizePolicy
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QTimer
from PyQt6.QtGui import QFont

import paramiko

from sources import load_sources, SourceConfig

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
_DIM_STYLE = "color: #999; border: none; background: transparent; font-size: 11px;"
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

    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(16)
        layout.setContentsMargins(16, 16, 16, 16)

        # ======== KISMET SECTION ========
        kismet_title = QLabel("Kismet RPi5")
        kismet_title.setFont(QFont('', 16, QFont.Weight.Bold))
        kismet_title.setStyleSheet(_LABEL_STYLE)
        layout.addWidget(kismet_title)

        self._kismet_conn_label = QLabel("Not connected")
        self._kismet_conn_label.setStyleSheet(_DIM_STYLE)
        layout.addWidget(self._kismet_conn_label)

        # --- Kismet Service ---
        svc_group = QGroupBox("Service")
        svc_group.setStyleSheet(_GROUP_STYLE)
        svc_layout = QHBoxLayout(svc_group)

        self._kismet_status = QLabel("Unknown")
        self._kismet_status.setFont(QFont('', 12))
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
        gps_group.setStyleSheet(_GROUP_STYLE)
        gps_layout = QGridLayout(gps_group)

        self._gps_fix_label = QLabel("Fix: Unknown")
        self._gps_fix_label.setFont(QFont('', 12))
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

        self._restart_banner = QLabel("⚠  Restart Kismet for changes to take effect")
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
        # Separator
        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.HLine)
        sep.setStyleSheet("color: #444;")
        layout.addWidget(sep)

        pager_title = QLabel("Hak5 Pager")
        pager_title.setFont(QFont('', 16, QFont.Weight.Bold))
        pager_title.setStyleSheet(_LABEL_STYLE)
        layout.addWidget(pager_title)

        self._pager_conn_label = QLabel("Not connected")
        self._pager_conn_label.setStyleSheet(_DIM_STYLE)
        layout.addWidget(self._pager_conn_label)

        # --- Pager Service ---
        pager_svc_group = QGroupBox("Recon Service")
        pager_svc_group.setStyleSheet(_GROUP_STYLE)
        pager_svc_layout = QHBoxLayout(pager_svc_group)

        self._pager_status = QLabel("Unknown")
        self._pager_status.setFont(QFont('', 12))
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

    def showEvent(self, event):
        super().showEvent(event)
        self._find_sources()
        if self._kismet_config:
            self._poll_kismet_all()
        if self._pager_config:
            self._poll_pager_status()

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
