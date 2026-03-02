"""Settings dialog with tabbed layout for AirParse configuration."""

from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QTabWidget, QWidget, QFormLayout, QLineEdit, QGroupBox,
)
from PyQt6.QtCore import Qt, QTimer
from PyQt6.QtGui import QFont

from database.wigle_api import WigleApiClient

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
    QLineEdit {
        background-color: #3c3f41; color: #e0e0e0;
        border: 1px solid #555; border-radius: 3px; padding: 4px;
    }
    QTabWidget::pane { border: 1px solid #444; background-color: #2b2b2b; }
    QTabBar::tab {
        background-color: #333; color: #aaa;
        border: 1px solid #444; border-bottom: none;
        padding: 6px 16px; margin-right: 2px; border-radius: 4px 4px 0 0;
    }
    QTabBar::tab:selected { background-color: #2b2b2b; color: #e0e0e0; }
    QTabBar::tab:hover { background-color: #3c3f41; }
"""


class SettingsDialog(QDialog):
    """Application settings dialog with tabbed layout."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Settings")
        self.setMinimumWidth(480)
        self.setMinimumHeight(360)
        self.setStyleSheet(_DARK_STYLE)
        self._setup_ui()
        self._load_credentials()

    def _setup_ui(self):
        layout = QVBoxLayout(self)

        title = QLabel("Settings")
        title.setFont(QFont('', 14, QFont.Weight.Bold))
        title.setStyleSheet("border: none; background: transparent;")
        layout.addWidget(title)

        self._tabs = QTabWidget()
        self._tabs.addTab(self._create_wigle_tab(), "WiGLE API")
        layout.addWidget(self._tabs)

        # Bottom buttons
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()

        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)
        btn_layout.addWidget(cancel_btn)

        save_btn = QPushButton("Save")
        save_btn.setStyleSheet(
            "QPushButton { background-color: #2ecc71; color: #1e1e1e; font-weight: bold; }"
            "QPushButton:hover { background-color: #27ae60; }")
        save_btn.clicked.connect(self._save_and_close)
        btn_layout.addWidget(save_btn)

        layout.addLayout(btn_layout)

    def _create_wigle_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)

        # Credentials group
        cred_group = QGroupBox("API Credentials")
        cred_form = QFormLayout(cred_group)

        info = QLabel(
            "Get your API credentials from "
            "<a href='https://wigle.net/account' style='color: #3498db;'>wigle.net/account</a> "
            "under API Token.")
        info.setOpenExternalLinks(True)
        info.setWordWrap(True)
        info.setStyleSheet("border: none; background: transparent;")
        cred_form.addRow(info)

        self._api_name_edit = QLineEdit()
        self._api_name_edit.setPlaceholderText("API Name (encoded)")
        cred_form.addRow("API Name:", self._api_name_edit)

        self._api_token_edit = QLineEdit()
        self._api_token_edit.setPlaceholderText("API Token (encoded)")
        self._api_token_edit.setEchoMode(QLineEdit.EchoMode.Password)
        cred_form.addRow("API Token:", self._api_token_edit)

        # Test button + result
        test_row = QHBoxLayout()
        self._test_btn = QPushButton("Test Connection")
        self._test_btn.clicked.connect(self._test_connection)
        test_row.addWidget(self._test_btn)

        self._test_label = QLabel("")
        self._test_label.setStyleSheet("border: none; background: transparent;")
        test_row.addWidget(self._test_label, 1)
        cred_form.addRow(test_row)

        layout.addWidget(cred_group)

        # Cache group
        cache_group = QGroupBox("Cache")
        cache_layout = QHBoxLayout(cache_group)

        client = WigleApiClient()
        self._cache_label = QLabel(f"{client.cache_size()} cached lookups")
        self._cache_label.setStyleSheet("border: none; background: transparent;")
        cache_layout.addWidget(self._cache_label, 1)

        clear_btn = QPushButton("Clear Cache")
        clear_btn.clicked.connect(self._clear_cache)
        cache_layout.addWidget(clear_btn)

        layout.addWidget(cache_group)

        layout.addStretch()
        return tab

    def _load_credentials(self):
        name, token = WigleApiClient.get_credentials()
        self._api_name_edit.setText(name)
        self._api_token_edit.setText(token)

    def _test_connection(self):
        self._test_btn.setEnabled(False)
        self._test_label.setText("Testing...")
        self._test_label.setStyleSheet("color: #f39c12; border: none; background: transparent;")
        self._test_label.repaint()

        # Save temp credentials before testing
        name = self._api_name_edit.text().strip()
        token = self._api_token_edit.text().strip()
        if not name or not token:
            self._test_label.setText("Enter both API Name and Token")
            self._test_label.setStyleSheet("color: #e74c3c; border: none; background: transparent;")
            self._test_btn.setEnabled(True)
            return

        WigleApiClient.save_credentials(name, token)
        client = WigleApiClient()
        ok, msg = client.test_credentials()

        if ok:
            self._test_label.setText(msg)
            self._test_label.setStyleSheet("color: #2ecc71; border: none; background: transparent;")
        else:
            self._test_label.setText(msg)
            self._test_label.setStyleSheet("color: #e74c3c; border: none; background: transparent;")

        self._test_btn.setEnabled(True)

    def _clear_cache(self):
        client = WigleApiClient()
        count = client.clear_cache()
        self._cache_label.setText(f"Cache cleared ({count} entries removed)")

    def _save_and_close(self):
        name = self._api_name_edit.text().strip()
        token = self._api_token_edit.text().strip()
        if name and token:
            WigleApiClient.save_credentials(name, token)
        self.accept()
