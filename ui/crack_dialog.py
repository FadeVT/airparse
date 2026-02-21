"""Dialogs for hashcat WPA password cracking."""

import gzip
import shutil
import tarfile
import time
import urllib.request
from pathlib import Path

from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel,
    QProgressBar, QPushButton, QLineEdit, QApplication, QFrame, QMessageBox
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QFont


_DARK_STYLE = """
    QDialog { background-color: #2b2b2b; color: #e0e0e0; }
    QLabel { color: #e0e0e0; }
    QPushButton {
        background-color: #3c3f41; color: #e0e0e0;
        border: 1px solid #555; border-radius: 4px;
        padding: 6px 16px;
    }
    QPushButton:hover { background-color: #4c5052; }
    QLineEdit {
        background-color: #1e1e1e; color: #e0e0e0;
        border: 1px solid #555; border-radius: 3px;
        padding: 6px; font-family: monospace; font-size: 14px;
    }
    QFrame { border-color: #444; }
"""


class CrackProgressDialog(QDialog):
    """Non-modal progress dialog for hashcat crack operations."""

    cancelled = pyqtSignal()

    def __init__(self, ssid: str, bssid: str, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Cracking Password")
        self.setMinimumWidth(450)
        self.setModal(False)
        self.setStyleSheet(_DARK_STYLE)
        self._start_time = time.time()
        self._ssid = ssid
        self._finished = False
        self._setup_ui(ssid, bssid)

    def _setup_ui(self, ssid: str, bssid: str):
        layout = QVBoxLayout(self)
        layout.setSpacing(10)

        # SSID header
        ssid_label = QLabel(ssid)
        font = QFont()
        font.setPointSize(16)
        font.setBold(True)
        ssid_label.setFont(font)
        layout.addWidget(ssid_label)

        # BSSID
        bssid_label = QLabel(bssid)
        bssid_label.setStyleSheet("color: #999; font-size: 12px; font-family: monospace;")
        layout.addWidget(bssid_label)

        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setMinimum(0)
        self.progress_bar.setMaximum(100)
        self.progress_bar.setTextVisible(True)
        self.progress_bar.setStyleSheet("""
            QProgressBar {
                border: 1px solid #555;
                border-radius: 3px;
                text-align: center;
                background-color: #1e1e1e;
                color: #e0e0e0;
                min-height: 24px;
            }
            QProgressBar::chunk {
                background-color: #2ecc71;
                border-radius: 2px;
            }
        """)
        layout.addWidget(self.progress_bar)

        # Stats row
        stats_frame = QFrame()
        stats_frame.setFrameStyle(QFrame.Shape.Box | QFrame.Shadow.Sunken)
        stats_frame.setStyleSheet("QFrame { background-color: #1e1e1e; border: 1px solid #444; border-radius: 4px; }")
        stats_layout = QHBoxLayout(stats_frame)
        stats_layout.setContentsMargins(10, 6, 10, 6)

        self.speed_label = QLabel("Speed: --")
        self.speed_label.setStyleSheet("color: #e0e0e0; font-size: 12px; border: none;")
        stats_layout.addWidget(self.speed_label)

        self.progress_label = QLabel("Progress: --")
        self.progress_label.setStyleSheet("color: #e0e0e0; font-size: 12px; border: none;")
        stats_layout.addWidget(self.progress_label)

        self.eta_label = QLabel("ETA: --")
        self.eta_label.setStyleSheet("color: #e0e0e0; font-size: 12px; border: none;")
        stats_layout.addWidget(self.eta_label)

        layout.addWidget(stats_frame)

        # Status text
        self.status_label = QLabel("Preparing...")
        self.status_label.setStyleSheet("color: #999; font-size: 11px;")
        layout.addWidget(self.status_label)

        # Cancel button
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self._on_cancel)
        btn_layout.addWidget(cancel_btn)
        layout.addLayout(btn_layout)

    def update_progress(self, status: dict):
        """Update from hashcat --status-json parsed dict."""
        # Progress
        prog = status.get('progress', [0, 1])
        if len(prog) >= 2 and prog[1] > 0:
            pct = int(prog[0] / prog[1] * 100)
            self.progress_bar.setValue(min(pct, 100))
            self.progress_label.setText(
                f"Progress: {prog[0]:,} / {prog[1]:,}")

        # Speed — sum all device speeds
        devices = status.get('devices', [])
        total_speed = sum(d.get('speed', 0) for d in devices)
        if total_speed > 0:
            if total_speed >= 1_000_000:
                self.speed_label.setText(f"Speed: {total_speed / 1_000_000:.1f} MH/s")
            elif total_speed >= 1_000:
                self.speed_label.setText(f"Speed: {total_speed / 1_000:.1f} kH/s")
            else:
                self.speed_label.setText(f"Speed: {total_speed} H/s")

        # ETA
        est_stop = status.get('estimated_stop', 0)
        if est_stop > 0:
            remaining = max(0, est_stop - time.time())
            if remaining > 3600:
                self.eta_label.setText(f"ETA: {remaining / 3600:.1f}h")
            elif remaining > 60:
                self.eta_label.setText(f"ETA: {remaining / 60:.0f}m")
            else:
                self.eta_label.setText(f"ETA: {remaining:.0f}s")

    def update_status(self, text: str):
        self.status_label.setText(text)

    def _on_cancel(self):
        self.status_label.setText("Cancelling...")
        self.cancelled.emit()

    def finish(self):
        """Allow the dialog to be closed programmatically."""
        self._finished = True
        self.close()

    def closeEvent(self, event):
        if self._finished:
            event.accept()
        else:
            event.ignore()
            self._on_cancel()

    def elapsed_seconds(self) -> float:
        return time.time() - self._start_time


class CrackResultDialog(QDialog):
    """Result dialog shown after crack attempt completes."""

    def __init__(self, ssid: str, bssid: str, password: str | None,
                 elapsed: float = 0, error: str | None = None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Crack Result")
        self.setMinimumWidth(400)
        self.setModal(True)
        self.setStyleSheet(_DARK_STYLE)
        self._setup_ui(ssid, bssid, password, elapsed, error)

    def _setup_ui(self, ssid: str, bssid: str, password: str | None,
                  elapsed: float, error: str | None):
        layout = QVBoxLayout(self)
        layout.setSpacing(12)

        # SSID header
        ssid_label = QLabel(ssid)
        font = QFont()
        font.setPointSize(14)
        font.setBold(True)
        ssid_label.setFont(font)
        layout.addWidget(ssid_label)

        bssid_label = QLabel(bssid)
        bssid_label.setStyleSheet("color: #999; font-size: 11px; font-family: monospace;")
        layout.addWidget(bssid_label)

        # Separator
        sep = QFrame()
        sep.setFrameStyle(QFrame.Shape.HLine | QFrame.Shadow.Sunken)
        sep.setStyleSheet("QFrame { border-top: 1px solid #444; }")
        layout.addWidget(sep)

        if error and not password:
            # Error state
            result_label = QLabel("Crack Failed")
            result_label.setStyleSheet("color: #e74c3c; font-size: 16px; font-weight: bold;")
            layout.addWidget(result_label)

            error_label = QLabel(error)
            error_label.setWordWrap(True)
            error_label.setStyleSheet("color: #ccc; font-size: 12px;")
            layout.addWidget(error_label)

        elif password:
            # Password found
            result_label = QLabel("Password Found")
            result_label.setStyleSheet("color: #2ecc71; font-size: 16px; font-weight: bold;")
            layout.addWidget(result_label)

            # Password field
            pw_layout = QHBoxLayout()
            self.pw_field = QLineEdit(password)
            self.pw_field.setReadOnly(True)
            pw_layout.addWidget(self.pw_field)

            copy_btn = QPushButton("Copy")
            copy_btn.setStyleSheet(
                "QPushButton { background-color: #2ecc71; color: #1e1e1e; "
                "font-weight: bold; border: none; padding: 6px 16px; border-radius: 4px; }"
                "QPushButton:hover { background-color: #27ae60; }")
            copy_btn.clicked.connect(
                lambda: QApplication.clipboard().setText(password))
            pw_layout.addWidget(copy_btn)
            layout.addLayout(pw_layout)

            # Stats
            if elapsed > 0:
                if elapsed > 60:
                    time_str = f"{elapsed / 60:.1f} minutes"
                else:
                    time_str = f"{elapsed:.1f} seconds"
                stats_label = QLabel(f"Cracked in {time_str}")
                stats_label.setStyleSheet("color: #999; font-size: 11px;")
                layout.addWidget(stats_label)

        else:
            # Not found
            result_label = QLabel("Password Not Found")
            result_label.setStyleSheet("color: #f39c12; font-size: 16px; font-weight: bold;")
            layout.addWidget(result_label)

            msg = QLabel(
                "The password was not found in the wordlist.\n\n"
                "Try a larger wordlist or a rule-based attack for more coverage.")
            msg.setWordWrap(True)
            msg.setStyleSheet("color: #ccc; font-size: 12px;")
            layout.addWidget(msg)

        # OK button
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        ok_btn = QPushButton("OK")
        ok_btn.clicked.connect(self.accept)
        btn_layout.addWidget(ok_btn)
        layout.addLayout(btn_layout)


def check_dependencies() -> tuple[bool, str]:
    """Check if hashcat and hcxpcapngtool are installed."""
    missing = []
    if not shutil.which('hashcat'):
        missing.append('hashcat')
    if not shutil.which('hcxpcapngtool'):
        missing.append('hcxtools (provides hcxpcapngtool)')

    if missing:
        names = ' and '.join(missing)
        return False, (
            f"{names} not found.\n\n"
            f"Install with: sudo pacman -S hashcat hcxtools")

    return True, ''


def find_wordlist() -> str | None:
    """Search common locations for rockyou.txt."""
    candidates = [
        '/usr/share/wordlists/rockyou.txt',
        '/usr/share/wordlists/rockyou.txt.gz',
        '/usr/share/seclists/Passwords/Leaked-Databases/rockyou.txt',
        Path.home() / 'wordlists' / 'rockyou.txt',
        Path.home() / 'Downloads' / 'rockyou.txt',
    ]
    for p in candidates:
        p = Path(p)
        if p.exists() and p.stat().st_size > 0:
            return str(p)
    return None


# Download URLs in priority order (tar.gz/gz — all verified live Feb 2026)
_ROCKYOU_URLS = [
    'https://github.com/praetorian-inc/Hob0Rules/raw/master/wordlists/rockyou.txt.gz',
    'https://gitlab.com/kalilinux/packages/wordlists/-/raw/kali/master/rockyou.txt.gz',
    'https://github.com/danielmiessler/SecLists/raw/master/Passwords/Leaked-Databases/rockyou.txt.tar.gz',
]

_DEFAULT_WORDLIST_DIR = Path.home() / 'wordlists'
_DEFAULT_WORDLIST_PATH = _DEFAULT_WORDLIST_DIR / 'rockyou.txt'


class _WordlistDownloader(QThread):
    """Downloads rockyou.txt in background with progress reporting."""

    progress = pyqtSignal(int, int)  # bytes_downloaded, total_bytes
    status = pyqtSignal(str)
    finished = pyqtSignal(bool, str)  # success, path_or_error

    def __init__(self, dest_path: str):
        super().__init__()
        self._dest = dest_path
        self._cancelled = False

    def run(self):
        dest = Path(self._dest)
        dest.parent.mkdir(parents=True, exist_ok=True)

        for url in _ROCKYOU_URLS:
            if self._cancelled:
                self.finished.emit(False, 'Cancelled')
                return

            is_gz = url.endswith('.gz')
            self.status.emit(f"Downloading from {url.split('/')[2]}...")

            try:
                req = urllib.request.Request(url, headers={
                    'User-Agent': 'AirParse/2.0'
                })
                resp = urllib.request.urlopen(req, timeout=30)
                total = int(resp.headers.get('Content-Length', 0))
                downloaded = 0
                tmp_path = str(dest) + ('.gz.tmp' if is_gz else '.tmp')

                with open(tmp_path, 'wb') as f:
                    while True:
                        if self._cancelled:
                            Path(tmp_path).unlink(missing_ok=True)
                            self.finished.emit(False, 'Cancelled')
                            return
                        chunk = resp.read(65536)
                        if not chunk:
                            break
                        f.write(chunk)
                        downloaded += len(chunk)
                        self.progress.emit(downloaded, total)

                # Verify we got something real (>1MB)
                if Path(tmp_path).stat().st_size < 1_000_000:
                    Path(tmp_path).unlink(missing_ok=True)
                    continue

                # Decompress if compressed
                if url.endswith('.tar.gz'):
                    self.status.emit("Extracting...")
                    txt_tmp = str(dest) + '.tmp'
                    with tarfile.open(tmp_path, 'r:gz') as tar:
                        for member in tar.getmembers():
                            if member.name.endswith('rockyou.txt'):
                                f_in = tar.extractfile(member)
                                if f_in:
                                    with open(txt_tmp, 'wb') as out:
                                        shutil.copyfileobj(f_in, out)
                                    break
                    Path(tmp_path).unlink(missing_ok=True)
                    if not Path(txt_tmp).exists():
                        continue
                    tmp_path = txt_tmp
                elif is_gz:
                    self.status.emit("Decompressing...")
                    txt_tmp = str(dest) + '.tmp'
                    with gzip.open(tmp_path, 'rb') as gz_in, \
                            open(txt_tmp, 'wb') as out:
                        while True:
                            chunk = gz_in.read(1_048_576)
                            if not chunk:
                                break
                            out.write(chunk)
                    Path(tmp_path).unlink(missing_ok=True)
                    tmp_path = txt_tmp

                # Move to final location
                Path(tmp_path).rename(dest)
                self.finished.emit(True, str(dest))
                return

            except Exception:
                # Try next URL
                continue

        self.finished.emit(False, 'All download sources failed. Download rockyou.txt manually.')

    def cancel(self):
        self._cancelled = True


class WordlistDownloadDialog(QDialog):
    """Progress dialog for downloading rockyou.txt."""

    download_finished = pyqtSignal(bool, str)  # success, path_or_error

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Downloading Wordlist")
        self.setMinimumWidth(420)
        self.setModal(True)
        self.setStyleSheet(_DARK_STYLE)
        self._worker = None
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(10)

        title = QLabel("Downloading rockyou.txt")
        font = QFont()
        font.setPointSize(14)
        font.setBold(True)
        title.setFont(font)
        layout.addWidget(title)

        desc = QLabel("14 million passwords (~134 MB)")
        desc.setStyleSheet("color: #999; font-size: 12px;")
        layout.addWidget(desc)

        self.progress_bar = QProgressBar()
        self.progress_bar.setMinimum(0)
        self.progress_bar.setMaximum(100)
        self.progress_bar.setTextVisible(True)
        self.progress_bar.setStyleSheet("""
            QProgressBar {
                border: 1px solid #555; border-radius: 3px;
                text-align: center; background-color: #1e1e1e;
                color: #e0e0e0; min-height: 24px;
            }
            QProgressBar::chunk {
                background-color: #3498db; border-radius: 2px;
            }
        """)
        layout.addWidget(self.progress_bar)

        self.status_label = QLabel("Connecting...")
        self.status_label.setStyleSheet("color: #999; font-size: 11px;")
        layout.addWidget(self.status_label)

        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        self._cancel_btn = QPushButton("Cancel")
        self._cancel_btn.clicked.connect(self._on_cancel)
        btn_layout.addWidget(self._cancel_btn)
        layout.addLayout(btn_layout)

    def start_download(self, dest_path: str | None = None):
        path = dest_path or str(_DEFAULT_WORDLIST_PATH)
        self._worker = _WordlistDownloader(path)
        self._worker.progress.connect(self._on_progress)
        self._worker.status.connect(self._on_status)
        self._worker.finished.connect(self._on_finished)
        self._worker.start()

    def _on_progress(self, downloaded: int, total: int):
        if total > 0:
            pct = int(downloaded / total * 100)
            self.progress_bar.setValue(min(pct, 100))
            mb_done = downloaded / 1_048_576
            mb_total = total / 1_048_576
            self.status_label.setText(f"Downloaded {mb_done:.1f} / {mb_total:.1f} MB")
        else:
            mb_done = downloaded / 1_048_576
            self.progress_bar.setMaximum(0)  # indeterminate
            self.status_label.setText(f"Downloaded {mb_done:.1f} MB")

    def _on_status(self, text: str):
        self.status_label.setText(text)

    def _on_finished(self, success: bool, result: str):
        self._worker = None
        self.download_finished.emit(success, result)
        self.accept()

    def _on_cancel(self):
        if self._worker:
            self._worker.cancel()
        self.reject()

    def closeEvent(self, event):
        if self._worker:
            self._worker.cancel()
            self._worker.wait(3000)
        super().closeEvent(event)
