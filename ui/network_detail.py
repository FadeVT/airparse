"""Network/SSID detail dialog for viewing APs with the same SSID."""

import json
from typing import Optional

from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QTabWidget, QWidget,
    QLabel, QTextEdit, QTableWidget, QTableWidgetItem,
    QPushButton, QHeaderView, QScrollArea, QFrame, QGridLayout
)
from PyQt6.QtCore import Qt, pyqtSignal
from PyQt6.QtGui import QFont


class NetworkDetailDialog(QDialog):
    """Dialog showing detailed information about a network (SSID)."""

    # Signal to show AP on map
    showOnMap = pyqtSignal(float, float, str)

    def __init__(self, network_data: dict, parent=None, db_reader=None):
        super().__init__(parent)
        self.network_data = network_data
        self.db_reader = db_reader
        self._parent = parent
        self._aps_df = None
        self._setup_ui()

    def _setup_ui(self):
        """Set up the dialog UI."""
        ssid = self.network_data.get('ssid', 'Unknown Network')
        self.setWindowTitle(f"Network Details - {ssid}")
        self.setMinimumSize(800, 500)
        self.resize(900, 600)

        layout = QVBoxLayout(self)

        # Header with SSID
        header = self._create_header()
        layout.addWidget(header)

        # Tab widget for different sections
        self.tab_widget = QTabWidget()

        # APs tab - list of access points with this SSID
        aps_tab = self._create_aps_tab()
        self.tab_widget.addTab(aps_tab, "Access Points")

        # Summary tab
        summary_tab = self._create_summary_tab()
        self.tab_widget.addTab(summary_tab, "Summary")

        # Raw data tab
        json_tab = self._create_json_tab()
        self.tab_widget.addTab(json_tab, "Raw Data")

        layout.addWidget(self.tab_widget)

        # Close button
        button_layout = QHBoxLayout()
        button_layout.addStretch()

        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.accept)
        button_layout.addWidget(close_btn)

        layout.addLayout(button_layout)

    def _create_header(self) -> QWidget:
        """Create the header with SSID name."""
        header = QFrame()
        header.setFrameStyle(QFrame.Shape.Box | QFrame.Shadow.Raised)
        header.setStyleSheet("background-color: #2b2b2b; padding: 10px;")

        layout = QVBoxLayout(header)

        ssid = self.network_data.get('ssid', 'Unknown Network')
        if not ssid or ssid == '':
            ssid = '<Hidden Network>'

        ssid_label = QLabel(ssid)
        font = QFont()
        font.setPointSize(18)
        font.setBold(True)
        ssid_label.setFont(font)
        layout.addWidget(ssid_label)

        # AP count
        ap_count = self.network_data.get('ap_count', 0)
        count_label = QLabel(f"Access Points: {ap_count}")
        count_label.setStyleSheet("color: #999;")
        layout.addWidget(count_label)

        return header

    def _create_aps_tab(self) -> QWidget:
        """Create the Access Points tab showing all APs with this SSID."""
        tab = QWidget()
        layout = QVBoxLayout(tab)

        if not self.db_reader:
            no_data_label = QLabel("Database connection not available.")
            no_data_label.setStyleSheet("color: #888; font-style: italic;")
            layout.addWidget(no_data_label)
            return tab

        # Get APs with this SSID
        ssid = self.network_data.get('ssid', '')
        try:
            aps_df = self.db_reader.get_access_points()
            if aps_df.empty:
                no_aps_label = QLabel("No access points found.")
                no_aps_label.setStyleSheet("color: #888; font-style: italic;")
                layout.addWidget(no_aps_label)
                return tab

            # Filter by SSID
            self._aps_df = aps_df[aps_df['name'] == ssid]

            if self._aps_df.empty:
                no_aps_label = QLabel(f"No access points found with SSID: {ssid}")
                no_aps_label.setStyleSheet("color: #888; font-style: italic;")
                layout.addWidget(no_aps_label)
                return tab

            # Create table
            table = QTableWidget()
            table.setColumnCount(7)
            table.setHorizontalHeaderLabels([
                'MAC Address', 'Manufacturer', 'Encryption', 'Channel',
                'Signal', 'GPS', 'Actions'
            ])
            table.setRowCount(len(self._aps_df))
            table.setAlternatingRowColors(True)
            table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
            table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)

            for i, (_, ap) in enumerate(self._aps_df.iterrows()):
                table.setItem(i, 0, QTableWidgetItem(str(ap.get('devmac', '-'))))
                table.setItem(i, 1, QTableWidgetItem(str(ap.get('manufacturer', '-') or '-')))
                table.setItem(i, 2, QTableWidgetItem(str(ap.get('encryption', '-') or '-')))
                table.setItem(i, 3, QTableWidgetItem(str(ap.get('channel', '-') or '-')))

                signal = ap.get('strongest_signal', '-')
                table.setItem(i, 4, QTableWidgetItem(f"{signal} dBm" if signal else '-'))

                # GPS availability
                lat = ap.get('min_lat', 0)
                lon = ap.get('min_lon', 0)
                has_gps = lat and lon and lat != 0 and lon != 0
                table.setItem(i, 5, QTableWidgetItem("Yes" if has_gps else "No"))

                # Actions - Show on Map button
                if has_gps:
                    map_btn = QPushButton("Show on Map")
                    map_btn.clicked.connect(lambda checked, r=i: self._show_ap_on_map(r))
                    table.setCellWidget(i, 6, map_btn)
                else:
                    table.setItem(i, 6, QTableWidgetItem("-"))

            table.horizontalHeader().setStretchLastSection(True)
            table.resizeColumnsToContents()

            # Double-click to show AP details
            table.cellDoubleClicked.connect(self._on_ap_double_clicked)

            layout.addWidget(table)
            self._aps_table = table

        except Exception as e:
            error_label = QLabel(f"Error loading access points: {e}")
            error_label.setStyleSheet("color: red;")
            layout.addWidget(error_label)

        return tab

    def _create_summary_tab(self) -> QWidget:
        """Create summary statistics tab."""
        tab = QWidget()
        layout = QGridLayout(tab)
        layout.setColumnStretch(1, 1)

        row = 0

        ssid = self.network_data.get('ssid', '-')
        layout.addWidget(QLabel("<b>SSID:</b>"), row, 0)
        layout.addWidget(QLabel(str(ssid) if ssid else '<Hidden>'), row, 1)
        row += 1

        ap_count = self.network_data.get('ap_count', 0)
        layout.addWidget(QLabel("<b>Access Point Count:</b>"), row, 0)
        layout.addWidget(QLabel(str(ap_count)), row, 1)
        row += 1

        # If we have the APs data, show more stats
        if self._aps_df is not None and not self._aps_df.empty:
            # Encryption types
            if 'encryption' in self._aps_df.columns:
                encryptions = self._aps_df['encryption'].dropna().unique()
                layout.addWidget(QLabel("<b>Encryption Types:</b>"), row, 0)
                layout.addWidget(QLabel(', '.join(str(e) for e in encryptions)), row, 1)
                row += 1

            # Channels
            if 'channel' in self._aps_df.columns:
                channels = sorted(self._aps_df['channel'].dropna().unique())
                layout.addWidget(QLabel("<b>Channels:</b>"), row, 0)
                layout.addWidget(QLabel(', '.join(str(c) for c in channels)), row, 1)
                row += 1

            # Signal range
            if 'strongest_signal' in self._aps_df.columns:
                signals = self._aps_df['strongest_signal'].dropna()
                if not signals.empty:
                    layout.addWidget(QLabel("<b>Signal Range:</b>"), row, 0)
                    layout.addWidget(QLabel(f"{signals.min()} to {signals.max()} dBm"), row, 1)
                    row += 1

            # APs with GPS
            lat_col = 'min_lat' if 'min_lat' in self._aps_df.columns else None
            if lat_col:
                with_gps = len(self._aps_df[
                    (self._aps_df[lat_col].notna()) &
                    (self._aps_df[lat_col] != 0)
                ])
                layout.addWidget(QLabel("<b>APs with GPS:</b>"), row, 0)
                layout.addWidget(QLabel(f"{with_gps} of {len(self._aps_df)}"), row, 1)
                row += 1

        layout.setRowStretch(row, 1)
        return tab

    def _create_json_tab(self) -> QWidget:
        """Create the raw JSON data tab."""
        tab = QWidget()
        layout = QVBoxLayout(tab)

        label = QLabel("Raw network data (JSON):")
        layout.addWidget(label)

        self.json_text = QTextEdit()
        self.json_text.setReadOnly(True)
        self.json_text.setFont(QFont("Consolas", 10))

        # Format and display JSON
        try:
            data_to_show = dict(self.network_data)
            if self._aps_df is not None and not self._aps_df.empty:
                # Add AP list (without the device blob)
                cols_to_include = [c for c in self._aps_df.columns if c != 'device']
                data_to_show['access_points'] = self._aps_df[cols_to_include].to_dict('records')
            json_str = json.dumps(data_to_show, indent=2, default=str)
            self.json_text.setText(json_str)
        except Exception as e:
            self.json_text.setText(f"Error formatting data: {e}\n\n{str(self.network_data)}")

        layout.addWidget(self.json_text)

        # Copy button
        copy_btn = QPushButton("Copy to Clipboard")
        copy_btn.clicked.connect(self._copy_json)
        layout.addWidget(copy_btn)

        return tab

    def _show_ap_on_map(self, row_index: int):
        """Show a specific AP on the map."""
        if self._aps_df is None or row_index >= len(self._aps_df):
            return

        ap = self._aps_df.iloc[row_index]
        lat = ap.get('min_lat')
        lon = ap.get('min_lon')

        if lat and lon and self._parent:
            main_window = self._parent
            if hasattr(main_window, 'show_map') and hasattr(main_window, 'main_map_view'):
                main_window.show_map()
                main_window.main_map_view.set_center(float(lat), float(lon), 17)
                self.accept()

    def _on_ap_double_clicked(self, row: int, col: int):
        """Handle double-click on an AP row to show its details."""
        if self._aps_df is None or row >= len(self._aps_df):
            return

        ap_data = self._aps_df.iloc[row].to_dict()

        # Show device detail dialog for this AP
        from ui.device_detail import show_device_detail
        show_device_detail(ap_data, self._parent, self.db_reader)

    def _copy_json(self):
        """Copy JSON data to clipboard."""
        from PyQt6.QtWidgets import QApplication

        clipboard = QApplication.clipboard()
        clipboard.setText(self.json_text.toPlainText())


def show_network_detail(network_data: dict, parent=None, db_reader=None) -> Optional[int]:
    """
    Show the network detail dialog.

    Args:
        network_data: Dictionary containing network information (ssid, ap_count)
        parent: Parent widget
        db_reader: Optional KismetDBReader for fetching related data

    Returns:
        Dialog result code
    """
    # Try to get db_reader from parent if not provided
    if db_reader is None and parent is not None:
        db_reader = getattr(parent, 'db_reader', None)

    dialog = NetworkDetailDialog(network_data, parent, db_reader)
    return dialog.exec()
