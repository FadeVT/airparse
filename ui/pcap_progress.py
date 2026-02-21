"""Progress dialog for PCAP file parsing."""

from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel,
    QProgressBar, QPushButton, QFrame
)
from PyQt6.QtCore import Qt, pyqtSignal


class PcapProgressDialog(QDialog):
    """Modal progress dialog shown while parsing a PCAP file."""

    cancelled = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Parsing PCAP")
        self.setMinimumWidth(500)
        self.setModal(True)
        self.setWindowFlags(
            self.windowFlags() & ~Qt.WindowType.WindowContextHelpButtonHint
        )
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(12)

        # Status label
        self.status_label = QLabel("Preparing...")
        self.status_label.setStyleSheet("font-size: 14px; font-weight: bold;")
        layout.addWidget(self.status_label)

        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setMinimum(0)
        self.progress_bar.setMaximum(100)
        self.progress_bar.setTextVisible(True)
        layout.addWidget(self.progress_bar)

        # Size info
        self.size_label = QLabel("")
        self.size_label.setStyleSheet("color: #999;")
        layout.addWidget(self.size_label)

        # Stats frame
        stats_frame = QFrame()
        stats_frame.setFrameStyle(QFrame.Shape.Box | QFrame.Shadow.Sunken)
        stats_layout = QHBoxLayout(stats_frame)
        stats_layout.setContentsMargins(10, 8, 10, 8)

        self.ap_label = QLabel("APs: 0")
        self.client_label = QLabel("Clients: 0")
        self.handshake_label = QLabel("Handshakes: 0")
        self.deauth_label = QLabel("Deauths: 0")

        for label in (self.ap_label, self.client_label,
                      self.handshake_label, self.deauth_label):
            label.setStyleSheet("font-size: 12px;")
            stats_layout.addWidget(label)

        layout.addWidget(stats_frame)

        # Packet count
        self.packet_label = QLabel("Packets processed: 0")
        self.packet_label.setStyleSheet("color: #999; font-size: 11px;")
        layout.addWidget(self.packet_label)

        # Cancel button
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self._on_cancel)
        btn_layout.addWidget(cancel_btn)
        layout.addLayout(btn_layout)

    def update_progress(self, bytes_read: int, total_bytes: int, stats: dict):
        """Update progress bar and stats from worker signal."""
        if total_bytes > 0:
            pct = int((bytes_read / total_bytes) * 100)
            self.progress_bar.setValue(min(pct, 100))

        # Format sizes
        read_mb = bytes_read / (1024 * 1024)
        total_mb = total_bytes / (1024 * 1024)
        if total_mb >= 1024:
            self.size_label.setText(
                f"{read_mb:.0f} MB / {total_mb / 1024:.1f} GB"
            )
        else:
            self.size_label.setText(
                f"{read_mb:.0f} MB / {total_mb:.0f} MB"
            )

        # Update stats
        self.ap_label.setText(f"APs: {stats.get('aps', 0):,}")
        self.client_label.setText(f"Clients: {stats.get('clients', 0):,}")
        self.handshake_label.setText(f"Handshakes: {stats.get('handshakes', 0):,}")
        self.deauth_label.setText(f"Deauths: {stats.get('deauths', 0):,}")
        self.packet_label.setText(
            f"Packets processed: {stats.get('packets', 0):,}"
        )

    def update_status(self, text: str):
        """Update status text from worker signal."""
        self.status_label.setText(text)

    def _on_cancel(self):
        self.status_label.setText("Cancelling...")
        self.cancelled.emit()

    def closeEvent(self, event):
        """Prevent closing via X button — use cancel instead."""
        event.ignore()
        self._on_cancel()
