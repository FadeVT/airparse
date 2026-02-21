"""Statistics panel — landing page with haul summary, highlights, and hero map."""

import pandas as pd

from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QFrame, QSizePolicy
)
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QFont

try:
    from PyQt6.QtWebEngineWidgets import QWebEngineView
    HAS_WEBENGINE = True
except ImportError:
    HAS_WEBENGINE = False


class _HaulCard(QFrame):
    """Slim headline stat card for the trophy bar."""

    def __init__(self, title: str, value: str = "-", parent=None):
        super().__init__(parent)
        self.setFrameStyle(QFrame.Shape.Box | QFrame.Shadow.Raised)
        self.setStyleSheet("""
            _HaulCard {
                background-color: #2b2b2b;
                border: 1px solid #444;
                border-radius: 6px;
                padding: 2px;
            }
        """)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 4, 8, 4)
        layout.setSpacing(0)
        layout.setAlignment(Qt.AlignmentFlag.AlignCenter)

        self.value_label = QLabel(value)
        font = QFont()
        font.setPointSize(20)
        font.setBold(True)
        self.value_label.setFont(font)
        self.value_label.setStyleSheet("color: #e0e0e0;")
        self.value_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self.value_label)

        self.title_label = QLabel(title)
        self.title_label.setStyleSheet("color: #999; font-size: 10px;")
        self.title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self.title_label)

    def set_value(self, value: str):
        self.value_label.setText(value)


class _HighlightCard(QFrame):
    """Trophy-style highlight card with icon, value, and label."""

    def __init__(self, icon: str, label: str, value: str = "-", parent=None):
        super().__init__(parent)
        self.setFrameStyle(QFrame.Shape.Box | QFrame.Shadow.Raised)
        self.setStyleSheet("""
            _HighlightCard {
                background-color: #2b2b2b;
                border: 1px solid #444;
                border-radius: 6px;
            }
        """)

        layout = QHBoxLayout(self)
        layout.setContentsMargins(10, 8, 10, 8)

        # Icon
        icon_label = QLabel(icon)
        icon_label.setStyleSheet("font-size: 22px; border: none;")
        icon_label.setFixedWidth(30)
        layout.addWidget(icon_label)

        # Text column
        text_layout = QVBoxLayout()
        text_layout.setSpacing(0)
        text_layout.setContentsMargins(0, 0, 0, 0)

        self._value = QLabel(value)
        self._value.setStyleSheet("font-weight: bold; font-size: 13px; color: #e0e0e0; border: none;")
        text_layout.addWidget(self._value)

        self._label = QLabel(label)
        self._label.setStyleSheet("color: #999; font-size: 10px; border: none;")
        text_layout.addWidget(self._label)

        layout.addLayout(text_layout)
        layout.addStretch()

    def set_value(self, value: str):
        self._value.setText(value)


class StatisticsPanel(QWidget):
    """Landing page: trophy bar, highlights, and hero map."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._is_pcap = False
        self._setup_ui()

    def _setup_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(10, 5, 10, 5)
        main_layout.setSpacing(6)

        # === ROW 1: TROPHY BAR (slim, 4 headline numbers) ===
        haul_layout = QHBoxLayout()
        haul_layout.setSpacing(8)

        self.haul_aps = _HaulCard("APs Discovered")
        self.haul_handshakes = _HaulCard("Handshakes")
        self.haul_clients = _HaulCard("Probing Clients")
        self.haul_fourth = _HaulCard("Miles Driven")

        haul_layout.addWidget(self.haul_aps)
        haul_layout.addWidget(self.haul_handshakes)
        haul_layout.addWidget(self.haul_clients)
        haul_layout.addWidget(self.haul_fourth)
        main_layout.addLayout(haul_layout)

        # === ROW 2: HIGHLIGHTS (single compact row) ===
        hl_layout = QHBoxLayout()
        hl_layout.setSpacing(6)

        self.hl_strongest = _HighlightCard("\U0001F4F6", "Strongest Signal")
        self.hl_most_probed = _HighlightCard("\U0001F50D", "Most Probed SSID")
        self.hl_rarest = _HighlightCard("\U0001F48E", "Rarest Find")
        self.hl_handshakes = _HighlightCard("\U0001F91D", "Complete Handshakes")
        self.hl_encryption = _HighlightCard("\U0001F512", "Encryption Breakdown")

        hl_layout.addWidget(self.hl_strongest)
        hl_layout.addWidget(self.hl_most_probed)
        hl_layout.addWidget(self.hl_rarest)
        hl_layout.addWidget(self.hl_handshakes)
        hl_layout.addWidget(self.hl_encryption)
        main_layout.addLayout(hl_layout)

        # === ROW 3: THE MAP (fills all remaining space) ===
        self._map_frame = QFrame()
        self._map_frame.setFrameStyle(QFrame.Shape.Box | QFrame.Shadow.Raised)
        self._map_frame.setStyleSheet("""
            QFrame {
                background-color: #1a1a2e;
                border: 1px solid #444;
                border-radius: 8px;
            }
        """)
        map_inner = QVBoxLayout(self._map_frame)
        map_inner.setContentsMargins(2, 2, 2, 2)

        if HAS_WEBENGINE:
            from ui.map_view import MapView
            self._mini_map = MapView()
            self._mini_map.setSizePolicy(
                QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
            map_inner.addWidget(self._mini_map)
        else:
            self._mini_map = None
            placeholder = QLabel("Map requires PyQt6-WebEngine")
            placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
            placeholder.setStyleSheet("color: #ccc; padding: 80px; font-size: 14px;")
            map_inner.addWidget(placeholder)

        main_layout.addWidget(self._map_frame, 1)  # stretch=1 so map fills space

    @staticmethod
    def _compute_miles_driven(gps_df: pd.DataFrame) -> float:
        """Estimate miles driven from GPS bounding box of all sightings."""
        import math
        if gps_df.empty or len(gps_df) < 2:
            return 0.0

        valid = gps_df[(gps_df['lat'] != 0) & (gps_df['lon'] != 0)]
        if valid.empty:
            return 0.0

        min_lat = valid['lat'].min()
        max_lat = valid['lat'].max()
        min_lon = valid['lon'].min()
        max_lon = valid['lon'].max()

        lat1, lon1 = math.radians(min_lat), math.radians(min_lon)
        lat2, lon2 = math.radians(max_lat), math.radians(max_lon)
        dlat = lat2 - lat1
        dlon = lon2 - lon1

        a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        c = 2 * math.asin(math.sqrt(a))
        r_miles = 3956
        return r_miles * c

    # --- Public update interface (called by main_window.py) ---

    def update_statistics(self, summary: dict, networks_df: pd.DataFrame = None,
                          signal_df: pd.DataFrame = None):
        """Update haul bar and highlights from summary dict."""
        phy_counts = summary.get('by_phy_type', {})
        wifi_count = phy_counts.get('IEEE802.11', 0)

        self.haul_aps.set_value(str(wifi_count))

        geo = summary.get('geographic_bounds', {})
        gps_count = geo.get('unique_devices_with_gps', 0)

        if gps_count > 0 and not self._is_pcap:
            self.haul_fourth.set_value(str(gps_count))
            self.haul_fourth.title_label.setText("GPS Devices")
        elif not self._is_pcap:
            self.haul_fourth.set_value("-")
            self.haul_fourth.title_label.setText("Miles Driven")

        if networks_df is not None and not networks_df.empty:
            if not self._is_pcap:
                top_ssid = networks_df.iloc[0]['ssid'] if len(networks_df) > 0 else '-'
                top_count = networks_df.iloc[0]['ap_count'] if len(networks_df) > 0 else 0
                self.hl_most_probed.set_value(f"{top_ssid} ({top_count} APs)")

    def show_mini_map(self, devices_df: pd.DataFrame, gps_df: pd.DataFrame = None):
        """Plot device markers and GPS track on the hero map."""
        if self._mini_map is None:
            return

        if devices_df is None or devices_df.empty:
            return

        lat_col = 'min_lat' if 'min_lat' in devices_df.columns else 'lat'
        lon_col = 'min_lon' if 'min_lon' in devices_df.columns else 'lon'

        if lat_col not in devices_df.columns:
            return

        valid = devices_df[
            (devices_df[lat_col].notna()) &
            (devices_df[lat_col] != 0) &
            (devices_df[lon_col].notna()) &
            (devices_df[lon_col] != 0)
        ]

        if valid.empty:
            return

        self._mini_map.plot_devices(devices_df)

        if gps_df is not None and not gps_df.empty:
            self._mini_map.plot_gps_track(gps_df)

    def set_alert_count(self, count: int):
        pass  # Removed from landing page — available in individual tabs

    def set_data_source_count(self, count: int):
        pass  # Removed from landing page

    def set_wifi_counts(self, ap_count: int, client_count: int):
        self.haul_aps.set_value(str(ap_count))

    def show_pcap_stats(self, reader):
        """Show PCAP-specific stats — populate haul and highlights."""
        self._is_pcap = True

        # Handshakes
        hs_df = reader.get_handshakes()
        complete = len(hs_df[hs_df['complete']]) if not hs_df.empty else 0
        partial = len(hs_df[~hs_df['complete']]) if not hs_df.empty else 0
        total_hs = complete + partial
        self.haul_handshakes.set_value(str(total_hs))
        if complete > 0:
            self.haul_handshakes.value_label.setStyleSheet(
                "color: #2ecc71; font-size: 20pt; font-weight: bold;")
            self.hl_handshakes.set_value(f"{complete} complete, {partial} partial")
        else:
            self.hl_handshakes.set_value(f"{partial} partial" if partial > 0 else "None")

        # Probing clients
        probe_df = reader.get_probe_requests()
        probe_count = len(probe_df)
        self.haul_clients.set_value(str(probe_count))

        # Miles Driven (from GPS) or fallback to Total Packets
        info = reader.get_database_info()
        total_pkts = info.get('total_packets', 0)
        gps_df = reader.get_gps_data()
        if not gps_df.empty and 'lat' in gps_df.columns and 'lon' in gps_df.columns:
            miles = self._compute_miles_driven(gps_df)
            if miles > 0:
                self.haul_fourth.set_value(f"{miles:.1f}")
                self.haul_fourth.title_label.setText("Miles Driven")
            else:
                self.haul_fourth.set_value(f"{total_pkts:,}")
                self.haul_fourth.title_label.setText("Total Packets")
        else:
            self.haul_fourth.set_value(f"{total_pkts:,}")
            self.haul_fourth.title_label.setText("Total Packets")

        # --- Highlights ---
        ap_df = reader.get_access_points()
        if not ap_df.empty and 'strongest_signal' in ap_df.columns:
            best_row = ap_df.loc[ap_df['strongest_signal'].idxmax()]
            sig = int(best_row['strongest_signal']) if pd.notna(best_row['strongest_signal']) else 0
            name = best_row.get('name', '') or best_row.get('devmac', 'Unknown')
            self.hl_strongest.set_value(f"{sig} dBm — {name}")
        else:
            self.hl_strongest.set_value("-")

        if not probe_df.empty and 'probed_ssids' in probe_df.columns:
            ssid_counts = {}
            for ssids_str in probe_df['probed_ssids'].dropna():
                for s in str(ssids_str).split(', '):
                    s = s.strip()
                    if s and s != '<broadcast>':
                        ssid_counts[s] = ssid_counts.get(s, 0) + 1
            if ssid_counts:
                top = max(ssid_counts, key=ssid_counts.get)
                self.hl_most_probed.set_value(f"{top} ({ssid_counts[top]} devices)")

                rare = [s for s, c in ssid_counts.items() if c == 1]
                if rare:
                    self.hl_rarest.set_value(f"{rare[0]} (unique to 1 device)")
                else:
                    self.hl_rarest.set_value("-")

        if not ap_df.empty and 'encryption' in ap_df.columns:
            enc_counts = ap_df['encryption'].value_counts()
            parts = [f"{enc}: {cnt}" for enc, cnt in enc_counts.items()]
            self.hl_encryption.set_value(" | ".join(parts))
