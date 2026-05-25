"""Aggregate Pipeline page — manual end-to-end workflow.

Each step is a button-driven action; no auto-advance until each piece is
verified working in isolation.
"""

from pathlib import Path

from PyQt6.QtCore import Qt, pyqtSignal
from PyQt6.QtGui import QFont
from PyQt6.QtWidgets import (
    QFrame, QHBoxLayout, QLabel, QPushButton, QVBoxLayout, QWidget)


_BTN_PRIMARY = """
QPushButton {
    background-color: #2980b9; color: white; border: none;
    padding: 10px 20px; border-radius: 4px; font-weight: bold;
}
QPushButton:hover { background-color: #3498db; }
QPushButton:disabled { background-color: #555; color: #aaa; }
"""

_BTN_GO = """
QPushButton {
    background-color: #27ae60; color: white; border: none;
    padding: 10px 20px; border-radius: 4px; font-weight: bold;
}
QPushButton:hover { background-color: #2ecc71; }
QPushButton:disabled { background-color: #555; color: #aaa; }
"""

_CARD_STYLE = """
QFrame#StepCard {
    background-color: #2a2a2a;
    border: 1px solid #444;
    border-radius: 6px;
}
QLabel { color: #e0e0e0; background: transparent; border: none; }
"""


class _StepCard(QFrame):
    """One step in the pipeline. Title, description, action button, status line."""

    def __init__(self, number: int, title: str, description: str,
                 action_label: str, action_style: str = _BTN_PRIMARY, parent=None):
        super().__init__(parent)
        self.setObjectName('StepCard')
        self.setStyleSheet(_CARD_STYLE)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 12, 16, 12)
        layout.setSpacing(8)

        header = QHBoxLayout()
        num = QLabel(f'Step {number}')
        num.setStyleSheet('color: #888; font-size: 11px; font-weight: 600;')
        header.addWidget(num)
        header.addStretch()
        self.status = QLabel('Not run yet')
        self.status.setStyleSheet('color: #888; font-size: 11px;')
        header.addWidget(self.status)
        layout.addLayout(header)

        title_lbl = QLabel(title)
        title_lbl.setFont(QFont('', 14, QFont.Weight.Bold))
        layout.addWidget(title_lbl)

        desc = QLabel(description)
        desc.setStyleSheet('color: #aaa;')
        desc.setWordWrap(True)
        layout.addWidget(desc)

        btn_row = QHBoxLayout()
        btn_row.addStretch()
        self.button = QPushButton(action_label)
        self.button.setStyleSheet(action_style)
        btn_row.addWidget(self.button)
        layout.addLayout(btn_row)

    def set_status(self, text: str, color: str = '#888'):
        self.status.setText(text)
        self.status.setStyleSheet(f'color: {color}; font-size: 11px;')


class _StepCardWithSecondary(_StepCard):
    """Step card with an extra secondary button next to the primary action."""

    def __init__(self, number: int, title: str, description: str,
                 action_label: str, secondary_label: str,
                 action_style: str = _BTN_PRIMARY, parent=None):
        super().__init__(number, title, description, action_label, action_style, parent)
        # Insert secondary button before the primary in the existing btn row.
        # The button row is the last layout item we added.
        btn_row = self.layout().itemAt(self.layout().count() - 1).layout()
        self.secondary = QPushButton(secondary_label)
        self.secondary.setStyleSheet(
            'QPushButton { background-color: #34495e; color: white; '
            'border: none; padding: 10px 16px; border-radius: 4px; }'
            'QPushButton:hover { background-color: #455a73; }')
        # Insert secondary just before the primary button (which is at the end)
        btn_row.insertWidget(btn_row.count() - 1, self.secondary)


class AggregatePipeline(QWidget):
    """Pipeline landing — manual buttons for the full capture workflow.

    Emits high-level signals back to MainWindow; MainWindow owns the dialogs
    and engines this page drives.
    """

    connect_requested = pyqtSignal()
    filter_requested = pyqtSignal()
    edit_blocklist_requested = pyqtSignal()
    write_and_upload_requested = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setStyleSheet('background-color: #1e1e1e;')

        outer = QVBoxLayout(self)
        outer.setContentsMargins(24, 24, 24, 24)
        outer.setSpacing(16)

        title = QLabel('Aggregate Pipeline')
        title.setFont(QFont('', 18, QFont.Weight.Bold))
        title.setStyleSheet('color: #e0e0e0;')
        outer.addWidget(title)

        subtitle = QLabel(
            'Manual workflow — run each step in order. '
            'Merging and BSSID dedupe happen automatically inside Connect.')
        subtitle.setStyleSheet('color: #888;')
        subtitle.setWordWrap(True)
        outer.addWidget(subtitle)

        self.step_connect = _StepCard(
            1, 'Connect & Pull',
            'Open the Connect dialog: pick which devices to pull from '
            '(Kismet RPi5, Hak5 Pager, local phone CSVs), pull capture files. '
            'Merge and BSSID-dedupe happen automatically before the dialog closes.',
            'Open Connect Dialog…')
        self.step_connect.button.clicked.connect(self.connect_requested.emit)
        outer.addWidget(self.step_connect)

        self.step_filter = _StepCardWithSecondary(
            2, 'Filter (Blocklist)',
            'Strip MACs and OUI prefixes on the blocklist '
            '(~/.config/airparse/mac_blocklist.txt) from the merged data '
            'before export.',
            action_label='Apply Blocklist Filter',
            secondary_label='Edit Blocklist…')
        self.step_filter.button.clicked.connect(self.filter_requested.emit)
        self.step_filter.secondary.clicked.connect(self.edit_blocklist_requested.emit)
        outer.addWidget(self.step_filter)

        self.step_send = _StepCard(
            3, 'Write & Upload',
            'Write the unified WigleWifi-1.4 CSV (AirParse_<ts>.wiglecsv) to '
            'staging, then upload it to WiGLE. The staged file stays in '
            '~/.config/airparse/wigle_uploads/uploaded/ as your local record '
            'of what was shipped.',
            'Write & Upload to WiGLE', action_style=_BTN_GO)
        self.step_send.button.clicked.connect(self.write_and_upload_requested.emit)
        outer.addWidget(self.step_send)

        outer.addStretch()

    # Public helpers MainWindow uses to reflect state on this page.
    def on_merge_complete(self, network_count: int, source_count: int):
        self.step_connect.set_status(
            f'{network_count:,} networks · {source_count} sources', '#2ecc71')

    def on_filter_complete(self, removed: int):
        if removed:
            self.step_filter.set_status(f'Removed {removed:,} matches', '#2ecc71')
        else:
            self.step_filter.set_status('No matches in current data', '#888')

    def on_write_complete(self, path: Path, network_count: int):
        self.step_send.set_status(
            f'Wrote {network_count:,} → {path.name}; uploading…', '#f39c12')

    def on_upload_complete(self, transid: str):
        self.step_send.set_status(f'Uploaded · transid {transid}', '#2ecc71')

    def on_send_failed(self, reason: str):
        self.step_send.set_status(f'Failed: {reason}', '#e74c3c')
