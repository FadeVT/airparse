"""Device detail dialog for viewing full device information."""

import json
from typing import Optional

from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QTabWidget, QWidget,
    QLabel, QTextEdit, QTableWidget, QTableWidgetItem,
    QPushButton, QHeaderView, QScrollArea, QFrame, QGridLayout
)
from PyQt6.QtCore import Qt, pyqtSignal
from PyQt6.QtGui import QFont

from utils.oui_lookup import lookup_manufacturer, is_randomized_mac


class DeviceDetailDialog(QDialog):
    """Dialog showing detailed information about a device."""

    # Signal to show device on map
    showOnMap = pyqtSignal(float, float, str)

    def __init__(self, device_data: dict, parent=None, db_reader=None):
        super().__init__(parent)
        self.device_data = device_data
        self.db_reader = db_reader
        self._parent = parent
        self._setup_ui()
        self._populate_data()

    def _setup_ui(self):
        """Set up the dialog UI."""
        self.setWindowTitle("Device Details")
        self.setMinimumSize(700, 500)
        self.resize(800, 600)

        layout = QVBoxLayout(self)

        # Header with MAC address
        header = self._create_header()
        layout.addWidget(header)

        # Tab widget for different sections
        self.tab_widget = QTabWidget()

        # General info tab
        general_tab = self._create_general_tab()
        self.tab_widget.addTab(general_tab, "General")

        # Wi-Fi tab (if applicable)
        wifi_tab = self._create_wifi_tab()
        self.tab_widget.addTab(wifi_tab, "Wi-Fi Details")

        # Associated Clients tab (for APs only)
        device_type = self.device_data.get('type', '')
        if device_type in ('Wi-Fi AP', 'Wi-Fi WDS AP'):
            clients_tab = self._create_clients_tab()
            self.tab_widget.addTab(clients_tab, "Associated Clients")

        # Location tab
        location_tab = self._create_location_tab()
        self.tab_widget.addTab(location_tab, "Location")

        # Raw JSON tab
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
        """Create the header with device MAC."""
        header = QFrame()
        header.setFrameStyle(QFrame.Shape.Box | QFrame.Shadow.Raised)
        header.setStyleSheet("background-color: #f5f5f5; padding: 10px;")

        layout = QVBoxLayout(header)

        mac = self.device_data.get('devmac') or self.device_data.get('client_mac', 'Unknown')

        mac_label = QLabel(mac)
        font = QFont()
        font.setPointSize(18)
        font.setBold(True)
        font.setFamily("Consolas, monospace")
        mac_label.setFont(font)
        layout.addWidget(mac_label)

        # Manufacturer - prefer database value, fallback to OUI lookup
        manufacturer = self.device_data.get('manufacturer', '')
        if not manufacturer or manufacturer == '-' or manufacturer == 'Unknown':
            manufacturer = lookup_manufacturer(mac)
        if is_randomized_mac(mac):
            manufacturer += " (Randomized MAC)"

        manuf_label = QLabel(f"Manufacturer: {manufacturer}")
        manuf_label.setStyleSheet("color: #666;")
        layout.addWidget(manuf_label)

        # Device name/SSID if available
        name = self.device_data.get('name', '') or self.device_data.get('ssid', '')
        if name and name != '-':
            name_label = QLabel(f"Name: {name}")
            name_label.setStyleSheet("color: #333; font-size: 14px;")
            layout.addWidget(name_label)

        return header

    def _create_general_tab(self) -> QWidget:
        """Create the general information tab."""
        tab = QWidget()
        layout = QGridLayout(tab)
        layout.setColumnStretch(1, 1)

        row = 0

        # Add info fields
        fields = [
            ('MAC Address', self.device_data.get('devmac') or self.device_data.get('client_mac', '-')),
            ('PHY Type', self.device_data.get('phyname', '-')),
            ('Device Type', self.device_data.get('type', '-')),
            ('Name', self.device_data.get('name', '-')),
            ('Manufacturer', self.device_data.get('manufacturer', '-')),
            ('Signal Strength', f"{self.device_data.get('strongest_signal', '-')} dBm"),
            ('Channel', self.device_data.get('channel', '-')),
            ('First Seen', str(self.device_data.get('first_time', '-'))),
            ('Last Seen', str(self.device_data.get('last_time', '-'))),
        ]

        for label, value in fields:
            label_widget = QLabel(f"{label}:")
            label_widget.setStyleSheet("font-weight: bold;")
            layout.addWidget(label_widget, row, 0, Qt.AlignmentFlag.AlignTop)

            value_widget = QLabel(str(value) if value else '-')
            value_widget.setWordWrap(True)
            value_widget.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
            layout.addWidget(value_widget, row, 1)

            row += 1

        layout.setRowStretch(row, 1)
        return tab

    def _create_wifi_tab(self) -> QWidget:
        """Create the Wi-Fi details tab."""
        tab = QWidget()
        layout = QVBoxLayout(tab)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)

        content = QWidget()
        content_layout = QVBoxLayout(content)

        # Basic info grid
        info_grid = QGridLayout()
        info_grid.setColumnStretch(1, 1)
        row = 0

        # SSID/Name info
        name = self.device_data.get('name', '') or self.device_data.get('ssid', '-')
        info_grid.addWidget(QLabel("<b>SSID/Name:</b>"), row, 0)
        info_grid.addWidget(QLabel(str(name) if name else '-'), row, 1)
        row += 1

        # Common name
        commonname = self.device_data.get('commonname', '')
        if commonname and commonname != name:
            info_grid.addWidget(QLabel("<b>Common Name:</b>"), row, 0)
            info_grid.addWidget(QLabel(str(commonname)), row, 1)
            row += 1

        # Encryption
        encryption = self.device_data.get('encryption', '-')
        info_grid.addWidget(QLabel("<b>Encryption:</b>"), row, 0)
        info_grid.addWidget(QLabel(str(encryption) if encryption else '-'), row, 1)
        row += 1

        # Channel
        channel = self.device_data.get('channel', '-')
        info_grid.addWidget(QLabel("<b>Channel:</b>"), row, 0)
        info_grid.addWidget(QLabel(str(channel) if channel else '-'), row, 1)
        row += 1

        # Device type
        dev_type = self.device_data.get('type', '-')
        info_grid.addWidget(QLabel("<b>Type:</b>"), row, 0)
        info_grid.addWidget(QLabel(str(dev_type)), row, 1)
        row += 1

        # Last BSSID (for clients)
        last_bssid = self.device_data.get('last_bssid', '')
        if last_bssid:
            info_grid.addWidget(QLabel("<b>Last BSSID:</b>"), row, 0)
            info_grid.addWidget(QLabel(str(last_bssid)), row, 1)
            row += 1

        content_layout.addLayout(info_grid)

        # Associated clients section (for APs) - no longer requires db_reader
        device_type = self.device_data.get('type', '')
        if device_type in ('Wi-Fi AP', 'Wi-Fi WDS AP'):
            content_layout.addWidget(QLabel("<b>Associated Clients:</b>"))
            clients_table = self._create_clients_table()
            if clients_table:
                content_layout.addWidget(clients_table)
            else:
                no_clients = QLabel("No associated clients found.")
                no_clients.setStyleSheet("color: #888; font-style: italic;")
                content_layout.addWidget(no_clients)

        # Associations (for clients - which APs they've connected to)
        associations = self.device_data.get('associations', [])
        if associations:
            content_layout.addWidget(QLabel("<b>Associated APs:</b>"))
            assoc_list = QLabel('\n'.join(str(a) for a in associations[:10]))
            assoc_list.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
            content_layout.addWidget(assoc_list)

        content_layout.addStretch()
        scroll.setWidget(content)
        layout.addWidget(scroll)

        return tab

    def _create_clients_tab(self) -> QWidget:
        """Create a dedicated tab for associated clients."""
        tab = QWidget()
        layout = QVBoxLayout(tab)

        # Header
        header_label = QLabel("<b>Devices that have connected to this Access Point:</b>")
        layout.addWidget(header_label)

        # Get the clients table
        clients_table = self._create_clients_table()

        if clients_table:
            # Remove height restriction for the dedicated tab
            clients_table.setMaximumHeight(16777215)  # Default max
            clients_table.setMinimumHeight(200)
            layout.addWidget(clients_table)

            # Count label
            count_label = QLabel(f"Total: {clients_table.rowCount()} client(s)")
            count_label.setStyleSheet("color: #666; margin-top: 5px;")
            layout.addWidget(count_label)
        else:
            no_clients = QLabel("No associated clients found for this access point.")
            no_clients.setStyleSheet("color: #888; font-style: italic; padding: 20px;")
            no_clients.setAlignment(Qt.AlignmentFlag.AlignCenter)
            layout.addWidget(no_clients)

        layout.addStretch()
        return tab

    def _create_clients_table(self) -> Optional[QTableWidget]:
        """Create a table of associated clients for this AP."""
        # Get client MACs from the AP's device JSON (associated_client_map)
        device_json = self.device_data.get('device')
        if not device_json:
            return None

        try:
            # Parse device JSON if it's a string or bytes
            if isinstance(device_json, bytes):
                device_json = json.loads(device_json.decode('utf-8'))
            elif isinstance(device_json, str):
                device_json = json.loads(device_json)

            # Get associated client map from dot11.device
            dot11 = device_json.get('dot11.device', {})
            client_map = dot11.get('dot11.device.associated_client_map', {})

            if not client_map:
                return None

            # client_map is a dict where keys are client MAC addresses
            client_macs = list(client_map.keys())

            if not client_macs:
                return None

            # Create table
            table = QTableWidget()
            table.setColumnCount(4)
            table.setHorizontalHeaderLabels(['MAC', 'Name', 'Manufacturer', 'Signal'])
            table.setAlternatingRowColors(True)
            table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
            table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)

            # Try to look up client details from database if available
            clients_df = None
            if self.db_reader:
                try:
                    clients_df = self.db_reader.get_clients()
                except Exception:
                    pass

            rows = []
            for client_mac in client_macs:
                name = '-'
                manufacturer = '-'
                signal = '-'

                # Try to find this client in the database
                if clients_df is not None and not clients_df.empty:
                    # Check both devmac and client_mac columns
                    mac_col = 'client_mac' if 'client_mac' in clients_df.columns else 'devmac'
                    client_row = clients_df[clients_df[mac_col] == client_mac]
                    if not client_row.empty:
                        client_data = client_row.iloc[0]
                        name = client_data.get('name', '-') or '-'
                        manufacturer = client_data.get('manufacturer', '-') or '-'
                        sig = client_data.get('strongest_signal')
                        if sig is not None:
                            signal = f"{sig} dBm"

                # Fall back to OUI lookup for manufacturer
                if manufacturer == '-':
                    from utils.oui_lookup import lookup_manufacturer
                    manufacturer = lookup_manufacturer(client_mac)

                rows.append((client_mac, name, manufacturer, signal))

            table.setRowCount(len(rows))
            for i, (mac, name, manuf, sig) in enumerate(rows):
                table.setItem(i, 0, QTableWidgetItem(str(mac)))
                table.setItem(i, 1, QTableWidgetItem(str(name)))
                table.setItem(i, 2, QTableWidgetItem(str(manuf)))
                table.setItem(i, 3, QTableWidgetItem(str(sig)))

            table.horizontalHeader().setStretchLastSection(True)
            table.resizeColumnsToContents()
            table.setMaximumHeight(200)

            return table

        except Exception as e:
            print(f"Error creating clients table: {e}")
            import traceback
            traceback.print_exc()
            return None

    def _create_location_tab(self) -> QWidget:
        """Create the location/GPS tab."""
        tab = QWidget()
        layout = QGridLayout(tab)
        layout.setColumnStretch(1, 1)

        row = 0

        # GPS coordinates
        min_lat = self.device_data.get('min_lat')
        min_lon = self.device_data.get('min_lon')
        max_lat = self.device_data.get('max_lat')
        max_lon = self.device_data.get('max_lon')

        if min_lat and min_lon and min_lat != 0 and min_lon != 0:
            layout.addWidget(QLabel("<b>Min Latitude:</b>"), row, 0)
            layout.addWidget(QLabel(f"{min_lat:.6f}"), row, 1)
            row += 1

            layout.addWidget(QLabel("<b>Min Longitude:</b>"), row, 0)
            layout.addWidget(QLabel(f"{min_lon:.6f}"), row, 1)
            row += 1

            if max_lat and max_lon:
                layout.addWidget(QLabel("<b>Max Latitude:</b>"), row, 0)
                layout.addWidget(QLabel(f"{max_lat:.6f}"), row, 1)
                row += 1

                layout.addWidget(QLabel("<b>Max Longitude:</b>"), row, 0)
                layout.addWidget(QLabel(f"{max_lon:.6f}"), row, 1)
                row += 1

            # Show on map button
            map_btn = QPushButton("Show on Map")
            map_btn.clicked.connect(self._show_on_map)
            layout.addWidget(map_btn, row, 0, 1, 2)
            row += 1
        else:
            no_gps_label = QLabel("No GPS data available for this device.")
            no_gps_label.setStyleSheet("color: #888; font-style: italic;")
            layout.addWidget(no_gps_label, row, 0, 1, 2)
            row += 1

        layout.setRowStretch(row, 1)
        return tab

    def _create_json_tab(self) -> QWidget:
        """Create the raw JSON data tab."""
        tab = QWidget()
        layout = QVBoxLayout(tab)

        label = QLabel("Raw device data (JSON):")
        layout.addWidget(label)

        self.json_text = QTextEdit()
        self.json_text.setReadOnly(True)
        self.json_text.setFont(QFont("Consolas", 10))

        # Format and display JSON
        try:
            json_str = json.dumps(self.device_data, indent=2, default=str)
            self.json_text.setText(json_str)
        except Exception as e:
            self.json_text.setText(f"Error formatting data: {e}\n\n{str(self.device_data)}")

        layout.addWidget(self.json_text)

        # Copy button
        copy_btn = QPushButton("Copy to Clipboard")
        copy_btn.clicked.connect(self._copy_json)
        layout.addWidget(copy_btn)

        return tab

    def _populate_data(self):
        """Populate all fields with device data."""
        # Data is populated during tab creation
        pass

    def _show_on_map(self):
        """Show device location on map."""
        lat = self.device_data.get('min_lat')
        lon = self.device_data.get('min_lon')

        if lat and lon and self._parent:
            # Get the main window and switch to map view
            main_window = self._parent
            if hasattr(main_window, 'show_map') and hasattr(main_window, 'main_map_view'):
                main_window.show_map()
                # Center on this device
                main_window.main_map_view.set_center(float(lat), float(lon), 17)
                self.accept()  # Close the dialog

    def _copy_json(self):
        """Copy JSON data to clipboard."""
        from PyQt6.QtWidgets import QApplication

        clipboard = QApplication.clipboard()
        clipboard.setText(self.json_text.toPlainText())


def show_device_detail(device_data: dict, parent=None, db_reader=None) -> Optional[int]:
    """
    Show the device detail dialog.

    Args:
        device_data: Dictionary containing device information
        parent: Parent widget
        db_reader: Optional KismetDBReader for fetching related data

    Returns:
        Dialog result code
    """
    # Try to get db_reader from parent if not provided
    if db_reader is None and parent is not None:
        db_reader = getattr(parent, 'db_reader', None)

    dialog = DeviceDetailDialog(device_data, parent, db_reader)
    return dialog.exec()
