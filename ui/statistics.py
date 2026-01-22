"""Statistics panel widget with charts and summary data."""

from typing import Optional
import pandas as pd

from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGridLayout, QLabel,
    QFrame, QScrollArea, QSizePolicy
)
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QFont

try:
    import pyqtgraph as pg
    HAS_PYQTGRAPH = True
except ImportError:
    HAS_PYQTGRAPH = False

try:
    from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
    from matplotlib.figure import Figure
    import matplotlib.pyplot as plt
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False


class StatCard(QFrame):
    """A card widget displaying a single statistic."""

    def __init__(self, title: str, value: str = "-", parent=None):
        super().__init__(parent)
        self.setFrameStyle(QFrame.Shape.Box | QFrame.Shadow.Raised)
        self.setStyleSheet("""
            StatCard {
                background-color: #ffffff;
                border: 1px solid #ddd;
                border-radius: 8px;
                padding: 10px;
            }
        """)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(15, 10, 15, 10)

        self.title_label = QLabel(title)
        self.title_label.setStyleSheet("color: #666; font-size: 12px;")
        layout.addWidget(self.title_label)

        self.value_label = QLabel(value)
        font = QFont()
        font.setPointSize(24)
        font.setBold(True)
        self.value_label.setFont(font)
        self.value_label.setStyleSheet("color: #333;")
        layout.addWidget(self.value_label)

    def set_value(self, value: str):
        """Update the displayed value."""
        self.value_label.setText(value)


class StatisticsPanel(QWidget):
    """Panel displaying statistics and charts."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._setup_ui()

    def _setup_ui(self):
        """Set up the UI components."""
        main_layout = QVBoxLayout(self)

        # Create scroll area
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)

        scroll_content = QWidget()
        content_layout = QVBoxLayout(scroll_content)

        # Title
        title = QLabel("Database Statistics")
        title.setStyleSheet("font-size: 20px; font-weight: bold; padding: 10px 0;")
        content_layout.addWidget(title)

        # Summary cards
        cards_widget = self._create_summary_cards()
        content_layout.addWidget(cards_widget)

        # Charts section
        charts_widget = self._create_charts_section()
        content_layout.addWidget(charts_widget)

        content_layout.addStretch()
        scroll.setWidget(scroll_content)
        main_layout.addWidget(scroll)

    def _create_summary_cards(self) -> QWidget:
        """Create the summary statistic cards."""
        widget = QWidget()
        layout = QGridLayout(widget)
        layout.setSpacing(15)

        # Create stat cards
        self.total_devices_card = StatCard("Total Devices")
        layout.addWidget(self.total_devices_card, 0, 0)

        self.wifi_aps_card = StatCard("Wi-Fi Access Points")
        layout.addWidget(self.wifi_aps_card, 0, 1)

        self.wifi_clients_card = StatCard("Wi-Fi Clients")
        layout.addWidget(self.wifi_clients_card, 0, 2)

        self.bluetooth_card = StatCard("Bluetooth Devices")
        layout.addWidget(self.bluetooth_card, 0, 3)

        self.unique_ssids_card = StatCard("Unique SSIDs")
        layout.addWidget(self.unique_ssids_card, 1, 0)

        self.gps_devices_card = StatCard("Devices with GPS")
        layout.addWidget(self.gps_devices_card, 1, 1)

        self.alerts_card = StatCard("Alerts")
        layout.addWidget(self.alerts_card, 1, 2)

        self.data_sources_card = StatCard("Data Sources")
        layout.addWidget(self.data_sources_card, 1, 3)

        return widget

    def _create_charts_section(self) -> QWidget:
        """Create the charts section."""
        widget = QWidget()
        layout = QVBoxLayout(widget)

        # Charts title
        charts_title = QLabel("Charts")
        charts_title.setStyleSheet("font-size: 16px; font-weight: bold; padding: 10px 0;")
        layout.addWidget(charts_title)

        # Charts grid
        charts_grid = QHBoxLayout()

        # Signal distribution chart
        self.signal_chart_widget = self._create_signal_chart()
        charts_grid.addWidget(self.signal_chart_widget)

        # Device type pie chart
        self.device_type_chart_widget = self._create_device_type_chart()
        charts_grid.addWidget(self.device_type_chart_widget)

        layout.addLayout(charts_grid)

        # Second row of charts
        charts_grid2 = QHBoxLayout()

        # Top SSIDs chart
        self.ssid_chart_widget = self._create_ssid_chart()
        charts_grid2.addWidget(self.ssid_chart_widget)

        # Encryption types chart
        self.encryption_chart_widget = self._create_encryption_chart()
        charts_grid2.addWidget(self.encryption_chart_widget)

        layout.addLayout(charts_grid2)

        return widget

    def _create_signal_chart(self) -> QWidget:
        """Create signal strength distribution chart."""
        widget = QFrame()
        widget.setFrameStyle(QFrame.Shape.Box | QFrame.Shadow.Raised)
        widget.setMinimumSize(400, 300)
        layout = QVBoxLayout(widget)

        label = QLabel("Signal Strength Distribution")
        label.setStyleSheet("font-weight: bold;")
        layout.addWidget(label)

        if HAS_PYQTGRAPH:
            self.signal_plot = pg.PlotWidget()
            self.signal_plot.setBackground('w')
            self.signal_plot.setLabel('left', 'Count')
            self.signal_plot.setLabel('bottom', 'Signal (dBm)')
            self.signal_plot.showGrid(x=True, y=True, alpha=0.3)
            layout.addWidget(self.signal_plot)
        else:
            placeholder = QLabel("PyQtGraph not available.\nInstall with: pip install pyqtgraph")
            placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
            layout.addWidget(placeholder)
            self.signal_plot = None

        return widget

    def _create_device_type_chart(self) -> QWidget:
        """Create device type distribution chart."""
        widget = QFrame()
        widget.setFrameStyle(QFrame.Shape.Box | QFrame.Shadow.Raised)
        widget.setMinimumSize(400, 300)
        layout = QVBoxLayout(widget)

        label = QLabel("Device Types")
        label.setStyleSheet("font-weight: bold;")
        layout.addWidget(label)

        if HAS_MATPLOTLIB:
            self.device_type_figure = Figure(figsize=(5, 4), dpi=100)
            self.device_type_canvas = FigureCanvas(self.device_type_figure)
            layout.addWidget(self.device_type_canvas)
        else:
            placeholder = QLabel("Matplotlib not available.\nInstall with: pip install matplotlib")
            placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
            layout.addWidget(placeholder)
            self.device_type_figure = None
            self.device_type_canvas = None

        return widget

    def _create_ssid_chart(self) -> QWidget:
        """Create top SSIDs chart."""
        widget = QFrame()
        widget.setFrameStyle(QFrame.Shape.Box | QFrame.Shadow.Raised)
        widget.setMinimumSize(400, 300)
        layout = QVBoxLayout(widget)

        label = QLabel("Top 10 SSIDs")
        label.setStyleSheet("font-weight: bold;")
        layout.addWidget(label)

        if HAS_PYQTGRAPH:
            self.ssid_plot = pg.PlotWidget()
            self.ssid_plot.setBackground('w')
            self.ssid_plot.setLabel('left', 'SSID')
            self.ssid_plot.setLabel('bottom', 'AP Count')
            layout.addWidget(self.ssid_plot)
        else:
            placeholder = QLabel("PyQtGraph not available")
            placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
            layout.addWidget(placeholder)
            self.ssid_plot = None

        return widget

    def _create_encryption_chart(self) -> QWidget:
        """Create encryption types chart."""
        widget = QFrame()
        widget.setFrameStyle(QFrame.Shape.Box | QFrame.Shadow.Raised)
        widget.setMinimumSize(400, 300)
        layout = QVBoxLayout(widget)

        label = QLabel("Encryption Types")
        label.setStyleSheet("font-weight: bold;")
        layout.addWidget(label)

        if HAS_MATPLOTLIB:
            self.encryption_figure = Figure(figsize=(5, 4), dpi=100)
            self.encryption_canvas = FigureCanvas(self.encryption_figure)
            layout.addWidget(self.encryption_canvas)
        else:
            placeholder = QLabel("Matplotlib not available")
            placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
            layout.addWidget(placeholder)
            self.encryption_figure = None
            self.encryption_canvas = None

        return widget

    def update_statistics(self, summary: dict, networks_df: pd.DataFrame = None,
                          signal_df: pd.DataFrame = None):
        """
        Update all statistics and charts.

        Args:
            summary: Dictionary from KismetDBReader.get_device_summary()
            networks_df: DataFrame of SSIDs from get_networks()
            signal_df: DataFrame of signal distribution
        """
        # Update summary cards
        self.total_devices_card.set_value(str(summary.get('total_devices', 0)))

        phy_counts = summary.get('by_phy_type', {})
        wifi_count = phy_counts.get('IEEE802.11', 0)
        bt_count = phy_counts.get('Bluetooth', 0) + phy_counts.get('BTLE', 0)

        self.wifi_aps_card.set_value(str(wifi_count))  # Will be refined with actual AP count
        self.wifi_clients_card.set_value("-")  # Needs separate query
        self.bluetooth_card.set_value(str(bt_count))

        geo_bounds = summary.get('geographic_bounds', {})
        self.gps_devices_card.set_value(str(geo_bounds.get('unique_devices_with_gps', 0)))

        # Update unique SSIDs
        if networks_df is not None and not networks_df.empty:
            self.unique_ssids_card.set_value(str(len(networks_df)))
        else:
            self.unique_ssids_card.set_value("-")

        # Update charts
        self._update_device_type_chart(phy_counts)
        self._update_signal_chart(signal_df)
        self._update_ssid_chart(networks_df)

    def _update_signal_chart(self, signal_df: pd.DataFrame):
        """Update the signal strength histogram."""
        if self.signal_plot is None or signal_df is None or signal_df.empty:
            return

        self.signal_plot.clear()

        x = signal_df['signal_bucket'].values
        y = signal_df['count'].values

        # Create bar chart
        bargraph = pg.BarGraphItem(x=x, height=y, width=8, brush='#3498db')
        self.signal_plot.addItem(bargraph)

    def _update_device_type_chart(self, phy_counts: dict):
        """Update the device type pie chart."""
        if self.device_type_figure is None or not phy_counts:
            return

        self.device_type_figure.clear()
        ax = self.device_type_figure.add_subplot(111)

        labels = []
        sizes = []
        colors = ['#3498db', '#2ecc71', '#9b59b6', '#e74c3c', '#f39c12', '#1abc9c']

        for phy_type, count in phy_counts.items():
            if count > 0:
                labels.append(phy_type)
                sizes.append(count)

        if sizes:
            ax.pie(sizes, labels=labels, colors=colors[:len(sizes)],
                   autopct='%1.1f%%', startangle=90)
            ax.axis('equal')

        self.device_type_canvas.draw()

    def _update_ssid_chart(self, networks_df: pd.DataFrame):
        """Update the top SSIDs bar chart."""
        if self.ssid_plot is None or networks_df is None or networks_df.empty:
            return

        self.ssid_plot.clear()

        # Get top 10 SSIDs
        top_ssids = networks_df.head(10)

        if top_ssids.empty:
            return

        y = list(range(len(top_ssids)))
        x = top_ssids['ap_count'].values

        # Create horizontal bar chart
        bargraph = pg.BarGraphItem(x0=0, y=y, height=0.6, width=x, brush='#2ecc71')
        self.ssid_plot.addItem(bargraph)

        # Add SSID labels
        ssid_names = top_ssids['ssid'].tolist()
        y_axis = self.ssid_plot.getAxis('left')
        y_axis.setTicks([[(i, ssid_names[i][:20]) for i in range(len(ssid_names))]])

    def _update_encryption_chart(self, encryption_counts: dict):
        """Update the encryption types pie chart."""
        if self.encryption_figure is None or not encryption_counts:
            return

        self.encryption_figure.clear()
        ax = self.encryption_figure.add_subplot(111)

        labels = list(encryption_counts.keys())
        sizes = list(encryption_counts.values())
        colors = ['#e74c3c', '#f39c12', '#3498db', '#2ecc71', '#9b59b6']

        if sizes:
            ax.pie(sizes, labels=labels, colors=colors[:len(sizes)],
                   autopct='%1.1f%%', startangle=90)
            ax.axis('equal')

        self.encryption_canvas.draw()

    def set_alert_count(self, count: int):
        """Set the alert count."""
        self.alerts_card.set_value(str(count))

    def set_data_source_count(self, count: int):
        """Set the data source count."""
        self.data_sources_card.set_value(str(count))

    def set_wifi_counts(self, ap_count: int, client_count: int):
        """Set the Wi-Fi AP and client counts."""
        self.wifi_aps_card.set_value(str(ap_count))
        self.wifi_clients_card.set_value(str(client_count))
