"""Dialogs for multi-stage hashcat cracking pipeline."""

import os
import time
import urllib.request
from pathlib import Path

from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel,
    QProgressBar, QPushButton, QFrame, QWidget
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QFont

from config import DEFAULT_CONFIG
from database.pipeline_worker import STAGES, CRACK_LEVELS

_DARK_STYLE = """
    QDialog { background-color: #2b2b2b; color: #e0e0e0; }
    QLabel { color: #e0e0e0; }
    QPushButton {
        background-color: #3c3f41; color: #e0e0e0;
        border: 1px solid #555; border-radius: 4px;
        padding: 6px 16px;
    }
    QPushButton:hover { background-color: #4c5052; }
    QFrame { border-color: #444; }
"""


class PipelineProgressDialog(QDialog):
    """Progress dialog for multi-stage hashcat cracking pipeline."""

    cancelled = pyqtSignal()

    def __init__(self, crack_level: str, target_count: int, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Cracking Pipeline")
        self.setMinimumWidth(520)
        self.setMinimumHeight(300)
        self.setModal(False)
        self.setStyleSheet(_DARK_STYLE)

        self._crack_level = crack_level
        self._target_count = target_count
        self._cracked_count = 0
        self._start_time = time.time()
        self._finished = False
        self._stage_start_times: dict[int, float] = {}
        self._stage_widgets: dict[int, dict] = {}
        self._last_found: str = ''

        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(8)

        # Title
        title = QLabel("Cracking Pipeline")
        font = QFont()
        font.setPointSize(14)
        font.setBold(True)
        title.setFont(font)
        layout.addWidget(title)

        # Target info
        max_stage = CRACK_LEVELS.get(self._crack_level, 2)
        info = QLabel(f"{self._target_count} target(s)  |  "
                      f"Level: {self._crack_level.title()}  |  "
                      f"Stages: 1-{max_stage}")
        info.setStyleSheet("color: #999; font-size: 11px;")
        layout.addWidget(info)

        # Separator
        sep = QFrame()
        sep.setFrameStyle(QFrame.Shape.HLine | QFrame.Shadow.Sunken)
        sep.setStyleSheet("QFrame { border-top: 1px solid #444; }")
        layout.addWidget(sep)

        # Stage rows
        self._stages_container = QVBoxLayout()
        self._stages_container.setSpacing(4)

        for stage in STAGES:
            if stage['num'] > max_stage:
                break
            row = self._create_stage_row(stage)
            self._stages_container.addLayout(row)

        layout.addLayout(self._stages_container)

        # Separator
        sep2 = QFrame()
        sep2.setFrameStyle(QFrame.Shape.HLine | QFrame.Shadow.Sunken)
        sep2.setStyleSheet("QFrame { border-top: 1px solid #444; }")
        layout.addWidget(sep2)

        # Stats bar
        stats_frame = QFrame()
        stats_frame.setFrameStyle(QFrame.Shape.Box | QFrame.Shadow.Sunken)
        stats_frame.setStyleSheet(
            "QFrame { background-color: #1e1e1e; border: 1px solid #444; border-radius: 4px; }")
        stats_layout = QHBoxLayout(stats_frame)
        stats_layout.setContentsMargins(10, 6, 10, 6)

        self._cracked_label = QLabel(f"Cracked: 0 of {self._target_count}")
        self._cracked_label.setStyleSheet("color: #e0e0e0; font-size: 12px; border: none;")
        stats_layout.addWidget(self._cracked_label)

        self._speed_label = QLabel("Speed: --")
        self._speed_label.setStyleSheet("color: #e0e0e0; font-size: 12px; border: none;")
        stats_layout.addWidget(self._speed_label)

        self._eta_label = QLabel("ETA: --")
        self._eta_label.setStyleSheet("color: #e0e0e0; font-size: 12px; border: none;")
        stats_layout.addWidget(self._eta_label)

        layout.addWidget(stats_frame)

        # Found password display
        self._found_label = QLabel("")
        self._found_label.setStyleSheet("color: #2ecc71; font-size: 12px;")
        self._found_label.setWordWrap(True)
        layout.addWidget(self._found_label)

        # Cancel button
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        self._cancel_btn = QPushButton("Cancel")
        self._cancel_btn.clicked.connect(self._on_cancel)
        btn_layout.addWidget(self._cancel_btn)
        layout.addLayout(btn_layout)

    def _create_stage_row(self, stage: dict) -> QHBoxLayout:
        row = QHBoxLayout()
        row.setSpacing(8)

        icon = QLabel("\u2b1c")  # White medium square (pending)
        icon.setFixedWidth(20)
        icon.setStyleSheet("font-size: 14px; border: none;")

        name = QLabel(f"Stage {stage['num']}: {stage['name']}")
        name.setStyleSheet("color: #999; font-size: 12px; border: none;")
        name.setMinimumWidth(220)

        result = QLabel("")
        result.setStyleSheet("color: #999; font-size: 12px; border: none;")
        result.setMinimumWidth(80)

        progress = QLabel("")
        progress.setStyleSheet("color: #999; font-size: 12px; border: none;")
        progress.setAlignment(Qt.AlignmentFlag.AlignRight)

        row.addWidget(icon)
        row.addWidget(name)
        row.addWidget(result)
        row.addStretch()
        row.addWidget(progress)

        self._stage_widgets[stage['num']] = {
            'icon': icon,
            'name': name,
            'result': result,
            'progress': progress,
            'found': 0,
        }

        return row

    # ── Slots ──────────────────────────────────────────────────────

    def on_stage_changed(self, num: int, name: str):
        """Update stage display when a new stage starts."""
        # Mark previous stages as done
        for snum, widgets in self._stage_widgets.items():
            if snum < num:
                if widgets['icon'].text() != '\u2705':  # Not already marked done
                    widgets['icon'].setText('\u2705')  # Green check
                    widgets['name'].setStyleSheet("color: #e0e0e0; font-size: 12px; border: none;")
                    elapsed = time.time() - self._stage_start_times.get(snum, self._start_time)
                    widgets['progress'].setText(self._format_elapsed(elapsed))

        # Mark current stage as running
        if num in self._stage_widgets:
            w = self._stage_widgets[num]
            w['icon'].setText('\U0001f504')  # Counterclockwise arrows (running)
            w['name'].setStyleSheet("color: #00d4ff; font-size: 12px; font-weight: bold; border: none;")
            w['progress'].setText("0%")
            self._stage_start_times[num] = time.time()

    def on_stage_progress(self, status: dict):
        """Update current stage progress from hashcat --status-json."""
        # Find the current (running) stage
        current = None
        for snum, widgets in self._stage_widgets.items():
            if widgets['icon'].text() == '\U0001f504':
                current = snum
                break

        if current and current in self._stage_widgets:
            prog = status.get('progress', [0, 1])
            if len(prog) >= 2 and prog[1] > 0:
                pct = int(prog[0] / prog[1] * 100)
                self._stage_widgets[current]['progress'].setText(f"{min(pct, 100)}%")

        # Speed
        devices = status.get('devices', [])
        total_speed = sum(d.get('speed', 0) for d in devices)
        if total_speed > 0:
            if total_speed >= 1_000_000:
                self._speed_label.setText(f"Speed: {total_speed / 1_000_000:.1f} MH/s")
            elif total_speed >= 1_000:
                self._speed_label.setText(f"Speed: {total_speed / 1_000:.1f} kH/s")
            else:
                self._speed_label.setText(f"Speed: {total_speed} H/s")

        # ETA
        est_stop = status.get('estimated_stop', 0)
        if est_stop > 0:
            remaining = max(0, est_stop - time.time())
            if remaining > 3600:
                self._eta_label.setText(f"ETA: {remaining / 3600:.1f}h")
            elif remaining > 60:
                self._eta_label.setText(f"ETA: {remaining / 60:.0f}m")
            else:
                self._eta_label.setText(f"ETA: {remaining:.0f}s")

    def on_hash_cracked(self, bssid: str, client_mac: str, password: str):
        """Handle a newly cracked hash."""
        self._cracked_count += 1
        self._cracked_label.setText(
            f"Cracked: {self._cracked_count} of {self._target_count}")

        # Update current stage's found count
        for snum, widgets in self._stage_widgets.items():
            if widgets['icon'].text() == '\U0001f504':
                widgets['found'] += 1
                widgets['result'].setText(f"{widgets['found']} found")
                widgets['result'].setStyleSheet(
                    "color: #2ecc71; font-size: 12px; border: none;")
                break

        # Show found password
        self._found_label.setText(f"Found: {bssid} \u2192 {password}")

    def on_finished(self, cracked: int, attempted: int):
        """Handle pipeline completion."""
        self._finished = True
        elapsed = time.time() - self._start_time

        # Mark all remaining stages
        for snum, widgets in self._stage_widgets.items():
            icon_text = widgets['icon'].text()
            if icon_text == '\U0001f504':  # Was running
                widgets['icon'].setText('\u2705')
                widgets['name'].setStyleSheet("color: #e0e0e0; font-size: 12px; border: none;")
                stage_elapsed = time.time() - self._stage_start_times.get(snum, self._start_time)
                widgets['progress'].setText(self._format_elapsed(stage_elapsed))

        # Update stats
        self._cracked_label.setText(f"Cracked: {cracked} of {attempted}")
        self._speed_label.setText(f"Total: {self._format_elapsed(elapsed)}")
        self._eta_label.setText("Complete")

        # Change cancel to close
        self._cancel_btn.setText("Close")
        self._cancel_btn.clicked.disconnect()
        self._cancel_btn.clicked.connect(self.close)

    def _on_cancel(self):
        self.cancelled.emit()

    def finish(self):
        self._finished = True
        self.close()

    def closeEvent(self, event):
        if self._finished:
            event.accept()
        else:
            event.ignore()
            self._on_cancel()

    @staticmethod
    def _format_elapsed(seconds: float) -> str:
        if seconds > 3600:
            return f"{seconds / 3600:.1f}h"
        elif seconds > 60:
            return f"{seconds / 60:.0f}m"
        else:
            return f"{seconds:.0f}s"


# ── Rule Download ──────────────────────────────────────────────────

_OTRTRA_URL = ('https://raw.githubusercontent.com/stealthsploit/'
               'Optimised-hashcat-Rule/master/OneRuleToRuleThemAll.rule')


class _RuleDownloader(QThread):
    """Downloads OneRuleToRuleThemAll.rule in background."""

    progress = pyqtSignal(int, int)
    status = pyqtSignal(str)
    finished = pyqtSignal(bool, str)  # success, path_or_error

    def __init__(self, dest_path: str):
        super().__init__()
        self._dest = dest_path
        self._cancelled = False

    def run(self):
        dest = Path(self._dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = str(dest) + '.tmp'

        self.status.emit("Downloading OneRuleToRuleThemAll.rule...")

        try:
            req = urllib.request.Request(_OTRTRA_URL, headers={
                'User-Agent': 'AirParse/2.0'
            })
            resp = urllib.request.urlopen(req, timeout=30)
            total = int(resp.headers.get('Content-Length', 0))
            downloaded = 0

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

            # Validate (should be >500KB)
            if Path(tmp_path).stat().st_size < 500_000:
                Path(tmp_path).unlink(missing_ok=True)
                self.finished.emit(False, 'Downloaded file too small — may be corrupted')
                return

            Path(tmp_path).rename(dest)
            self.finished.emit(True, str(dest))

        except Exception as e:
            Path(tmp_path).unlink(missing_ok=True)
            self.finished.emit(False, f'Download failed: {e}')

    def cancel(self):
        self._cancelled = True


class RuleDownloadDialog(QDialog):
    """Progress dialog for downloading OneRuleToRuleThemAll.rule."""

    download_finished = pyqtSignal(bool, str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Downloading Rule File")
        self.setMinimumWidth(420)
        self.setModal(True)
        self.setStyleSheet(_DARK_STYLE)
        self._worker = None
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(10)

        title = QLabel("Downloading OneRuleToRuleThemAll.rule")
        font = QFont()
        font.setPointSize(12)
        font.setBold(True)
        title.setFont(font)
        layout.addWidget(title)

        desc = QLabel("~50K optimized mutation rules (~1.2 MB)")
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

    def start_download(self):
        user_dir = os.path.expanduser(
            DEFAULT_CONFIG['hashcat'].get('user_rules_dir',
                                          '~/.local/share/hashcat/rules'))
        dest = os.path.join(user_dir, 'OneRuleToRuleThemAll.rule')

        self._worker = _RuleDownloader(dest)
        self._worker.progress.connect(self._on_progress)
        self._worker.status.connect(self._on_status)
        self._worker.finished.connect(self._on_finished)
        self._worker.start()

    def _on_progress(self, downloaded: int, total: int):
        if total > 0:
            pct = int(downloaded / total * 100)
            self.progress_bar.setValue(min(pct, 100))
            kb_done = downloaded / 1024
            kb_total = total / 1024
            self.status_label.setText(f"Downloaded {kb_done:.0f} / {kb_total:.0f} KB")
        else:
            self.progress_bar.setMaximum(0)
            self.status_label.setText(f"Downloaded {downloaded / 1024:.0f} KB")

    def _on_status(self, text: str):
        self.status_label.setText(text)

    def _on_finished(self, success: bool, result: str):
        self._worker = None
        self.download_finished.emit(success, result)
        if success:
            self.accept()
        else:
            self.reject()

    def _on_cancel(self):
        if self._worker:
            self._worker.cancel()
        self.reject()

    def closeEvent(self, event):
        if self._worker:
            self._worker.cancel()
            self._worker.wait(3000)
        super().closeEvent(event)
