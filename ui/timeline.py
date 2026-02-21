"""Timeline view widget for temporal visualization of capture activity."""

from datetime import datetime
from typing import Optional, Tuple
import pandas as pd

from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QComboBox, QSlider, QFrame, QSplitter
)
from PyQt6.QtCore import Qt, pyqtSignal

try:
    import pyqtgraph as pg
    from pyqtgraph import DateAxisItem
    HAS_PYQTGRAPH = True
except ImportError:
    HAS_PYQTGRAPH = False

import numpy as np


class TimelineView(QWidget):
    """Temporal visualization of capture activity."""

    # Signals
    timeRangeSelected = pyqtSignal(datetime, datetime)  # Emits when user selects a time range

    def __init__(self, parent=None):
        super().__init__(parent)
        self._packet_data = None
        self._device_data = None
        self._selected_range = None
        self._setup_ui()

    def _setup_ui(self):
        """Set up the UI components."""
        layout = QVBoxLayout(self)

        if not HAS_PYQTGRAPH:
            placeholder = QLabel(
                "PyQtGraph is required for timeline visualization.\n\n"
                "Install with: pip install pyqtgraph"
            )
            placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
            placeholder.setStyleSheet("background-color: #2b2b2b; color: #ccc; padding: 50px;")
            layout.addWidget(placeholder)
            return

        # Toolbar
        toolbar = self._create_toolbar()
        layout.addWidget(toolbar)

        # Create splitter for multiple charts
        splitter = QSplitter(Qt.Orientation.Vertical)

        # Packet count timeline
        packet_frame = self._create_packet_timeline()
        splitter.addWidget(packet_frame)

        # Signal strength timeline
        signal_frame = self._create_signal_timeline()
        splitter.addWidget(signal_frame)

        # Device activity timeline
        device_frame = self._create_device_timeline()
        splitter.addWidget(device_frame)

        splitter.setSizes([200, 150, 150])
        layout.addWidget(splitter)

        # Time range info
        self.range_label = QLabel("Time Range: No data loaded")
        self.range_label.setStyleSheet("padding: 5px; background-color: #2b2b2b; color: #ccc;")
        layout.addWidget(self.range_label)

    def _create_toolbar(self) -> QWidget:
        """Create the toolbar."""
        toolbar = QFrame()
        layout = QHBoxLayout(toolbar)
        layout.setContentsMargins(5, 5, 5, 5)

        # Aggregation selector
        layout.addWidget(QLabel("Aggregate by:"))
        self.agg_combo = QComboBox()
        self.agg_combo.addItems(["Second", "Minute", "Hour", "Day"])
        self.agg_combo.setCurrentText("Minute")
        self.agg_combo.currentTextChanged.connect(self._on_aggregation_changed)
        layout.addWidget(self.agg_combo)

        layout.addSpacing(20)

        # Reset zoom button
        reset_btn = QPushButton("Reset Zoom")
        reset_btn.clicked.connect(self._reset_zoom)
        layout.addWidget(reset_btn)

        # Select range button
        self.select_btn = QPushButton("Select Range")
        self.select_btn.setCheckable(True)
        self.select_btn.toggled.connect(self._on_select_mode_toggled)
        layout.addWidget(self.select_btn)

        # Clear selection button
        clear_btn = QPushButton("Clear Selection")
        clear_btn.clicked.connect(self._clear_selection)
        layout.addWidget(clear_btn)

        layout.addStretch()

        return toolbar

    def _create_packet_timeline(self) -> QFrame:
        """Create the packet count timeline chart."""
        frame = QFrame()
        frame.setFrameStyle(QFrame.Shape.Box | QFrame.Shadow.Raised)
        layout = QVBoxLayout(frame)
        layout.setContentsMargins(5, 5, 5, 5)

        label = QLabel("Packet Activity Over Time")
        label.setStyleSheet("font-weight: bold;")
        layout.addWidget(label)

        # Create plot with date axis
        date_axis = DateAxisItem(orientation='bottom')
        self.packet_plot = pg.PlotWidget(axisItems={'bottom': date_axis})
        self.packet_plot.setBackground('#2b2b2b')
        self.packet_plot.setLabel('left', 'Packet Count')
        self.packet_plot.showGrid(x=True, y=True, alpha=0.3)
        self.packet_plot.setMouseEnabled(x=True, y=True)

        # Enable box zoom
        self.packet_plot.getViewBox().setMouseMode(pg.ViewBox.RectMode)

        # Selection region
        self.packet_region = pg.LinearRegionItem(brush=pg.mkBrush(100, 100, 255, 50))
        self.packet_region.setZValue(10)
        self.packet_region.hide()
        self.packet_plot.addItem(self.packet_region)
        self.packet_region.sigRegionChangeFinished.connect(self._on_region_changed)

        layout.addWidget(self.packet_plot)

        return frame

    def _create_signal_timeline(self) -> QFrame:
        """Create the signal strength timeline chart."""
        frame = QFrame()
        frame.setFrameStyle(QFrame.Shape.Box | QFrame.Shadow.Raised)
        layout = QVBoxLayout(frame)
        layout.setContentsMargins(5, 5, 5, 5)

        label = QLabel("Average Signal Strength Over Time")
        label.setStyleSheet("font-weight: bold;")
        layout.addWidget(label)

        date_axis = DateAxisItem(orientation='bottom')
        self.signal_plot = pg.PlotWidget(axisItems={'bottom': date_axis})
        self.signal_plot.setBackground('#2b2b2b')
        self.signal_plot.setLabel('left', 'Signal (dBm)')
        self.signal_plot.showGrid(x=True, y=True, alpha=0.3)
        self.signal_plot.setMouseEnabled(x=True, y=True)
        self.signal_plot.getViewBox().setMouseMode(pg.ViewBox.RectMode)

        # Link X axis with packet plot
        self.signal_plot.setXLink(self.packet_plot)

        layout.addWidget(self.signal_plot)

        return frame

    def _create_device_timeline(self) -> QFrame:
        """Create the device activity timeline chart."""
        frame = QFrame()
        frame.setFrameStyle(QFrame.Shape.Box | QFrame.Shadow.Raised)
        layout = QVBoxLayout(frame)
        layout.setContentsMargins(5, 5, 5, 5)

        label = QLabel("Unique Devices Over Time")
        label.setStyleSheet("font-weight: bold;")
        layout.addWidget(label)

        date_axis = DateAxisItem(orientation='bottom')
        self.device_plot = pg.PlotWidget(axisItems={'bottom': date_axis})
        self.device_plot.setBackground('#2b2b2b')
        self.device_plot.setLabel('left', 'Device Count')
        self.device_plot.showGrid(x=True, y=True, alpha=0.3)
        self.device_plot.setMouseEnabled(x=True, y=True)
        self.device_plot.getViewBox().setMouseMode(pg.ViewBox.RectMode)

        # Link X axis with packet plot
        self.device_plot.setXLink(self.packet_plot)

        layout.addWidget(self.device_plot)

        return frame

    def load_packet_data(self, df: pd.DataFrame):
        """
        Load packet timeline data.

        Args:
            df: DataFrame with ts_sec, packet_count, avg_signal columns
        """
        if not HAS_PYQTGRAPH or df is None or df.empty:
            return

        self._packet_data = df.copy()
        self._update_packet_plot()
        self._update_signal_plot()
        self._update_range_label()

    def load_device_data(self, df: pd.DataFrame):
        """
        Load device timeline data.

        Args:
            df: DataFrame with device activity over time
        """
        if not HAS_PYQTGRAPH or df is None or df.empty:
            return

        self._device_data = df.copy()
        self._update_device_plot()

    def _get_aggregation_seconds(self) -> int:
        """Get the aggregation period in seconds."""
        agg = self.agg_combo.currentText()
        if agg == "Second":
            return 1
        elif agg == "Minute":
            return 60
        elif agg == "Hour":
            return 3600
        elif agg == "Day":
            return 86400
        return 60

    def _aggregate_data(self, df: pd.DataFrame, value_col: str, agg_func: str = 'sum') -> pd.DataFrame:
        """Aggregate data by the selected time period."""
        if df is None or df.empty:
            return pd.DataFrame()

        period_seconds = self._get_aggregation_seconds()

        # Create time buckets
        df = df.copy()
        df['time_bucket'] = (df['ts_sec'] // period_seconds) * period_seconds

        if agg_func == 'sum':
            result = df.groupby('time_bucket')[value_col].sum().reset_index()
        elif agg_func == 'mean':
            result = df.groupby('time_bucket')[value_col].mean().reset_index()
        elif agg_func == 'count':
            result = df.groupby('time_bucket')[value_col].count().reset_index()
        else:
            result = df.groupby('time_bucket')[value_col].sum().reset_index()

        result.columns = ['timestamp', 'value']
        return result

    def _update_packet_plot(self):
        """Update the packet count plot."""
        if not HAS_PYQTGRAPH or self._packet_data is None:
            return

        self.packet_plot.clear()
        self.packet_plot.addItem(self.packet_region)

        if 'packet_count' not in self._packet_data.columns:
            return

        agg_data = self._aggregate_data(self._packet_data, 'packet_count', 'sum')

        if agg_data.empty:
            return

        x = agg_data['timestamp'].values
        y = agg_data['value'].values

        # Plot as bar graph for better visualization
        pen = pg.mkPen(color='#3498db', width=2)
        brush = pg.mkBrush('#3498db80')

        bargraph = pg.BarGraphItem(x=x, height=y, width=self._get_aggregation_seconds() * 0.8,
                                    brush=brush, pen=pen)
        self.packet_plot.addItem(bargraph)

        # Also add a line for trend
        self.packet_plot.plot(x, y, pen=pg.mkPen('#2980b9', width=1))

    def _update_signal_plot(self):
        """Update the signal strength plot."""
        if not HAS_PYQTGRAPH or self._packet_data is None:
            return

        self.signal_plot.clear()

        if 'avg_signal' not in self._packet_data.columns:
            return

        agg_data = self._aggregate_data(self._packet_data, 'avg_signal', 'mean')

        if agg_data.empty:
            return

        x = agg_data['timestamp'].values
        y = agg_data['value'].values

        # Plot as line with fill
        pen = pg.mkPen(color='#e74c3c', width=2)
        self.signal_plot.plot(x, y, pen=pen, fillLevel=-100,
                              fillBrush=pg.mkBrush('#e74c3c40'))

    def _update_device_plot(self):
        """Update the device activity plot."""
        if not HAS_PYQTGRAPH or self._device_data is None:
            return

        self.device_plot.clear()

        # For device data, we need to count unique devices per time period
        if 'devmac' not in self._device_data.columns:
            return

        period_seconds = self._get_aggregation_seconds()

        df = self._device_data.copy()

        # Use first_time for device appearance
        time_col = 'first_time' if 'first_time' in df.columns else 'ts_sec'
        if time_col not in df.columns:
            return

        # Convert datetime to timestamp if needed
        if df[time_col].dtype == 'datetime64[ns]':
            df['ts'] = df[time_col].astype(np.int64) // 10**9
        else:
            df['ts'] = df[time_col]

        df['time_bucket'] = (df['ts'] // period_seconds) * period_seconds

        device_counts = df.groupby('time_bucket')['devmac'].nunique().reset_index()
        device_counts.columns = ['timestamp', 'count']

        if device_counts.empty:
            return

        x = device_counts['timestamp'].values
        y = device_counts['count'].values

        pen = pg.mkPen(color='#2ecc71', width=2)
        brush = pg.mkBrush('#2ecc7180')

        bargraph = pg.BarGraphItem(x=x, height=y, width=period_seconds * 0.8,
                                    brush=brush, pen=pen)
        self.device_plot.addItem(bargraph)

    def _update_range_label(self):
        """Update the time range information label."""
        if self._packet_data is None or self._packet_data.empty:
            self.range_label.setText("Time Range: No data loaded")
            return

        min_ts = self._packet_data['ts_sec'].min()
        max_ts = self._packet_data['ts_sec'].max()

        start = datetime.fromtimestamp(min_ts)
        end = datetime.fromtimestamp(max_ts)
        duration = end - start

        self.range_label.setText(
            f"Time Range: {start.strftime('%Y-%m-%d %H:%M:%S')} to "
            f"{end.strftime('%Y-%m-%d %H:%M:%S')} (Duration: {duration})"
        )

    def _on_aggregation_changed(self, text: str):
        """Handle aggregation period change."""
        self._update_packet_plot()
        self._update_signal_plot()
        self._update_device_plot()

    def _reset_zoom(self):
        """Reset zoom to show all data."""
        if HAS_PYQTGRAPH:
            self.packet_plot.autoRange()
            self.signal_plot.autoRange()
            self.device_plot.autoRange()

    def _on_select_mode_toggled(self, enabled: bool):
        """Handle selection mode toggle."""
        if HAS_PYQTGRAPH:
            if enabled:
                self.packet_region.show()
                # Set initial region to middle of data
                if self._packet_data is not None and not self._packet_data.empty:
                    min_ts = self._packet_data['ts_sec'].min()
                    max_ts = self._packet_data['ts_sec'].max()
                    mid = (min_ts + max_ts) / 2
                    span = (max_ts - min_ts) * 0.1
                    self.packet_region.setRegion([mid - span, mid + span])
            else:
                self.packet_region.hide()

    def _on_region_changed(self):
        """Handle selection region change."""
        if HAS_PYQTGRAPH:
            region = self.packet_region.getRegion()
            start = datetime.fromtimestamp(region[0])
            end = datetime.fromtimestamp(region[1])
            self._selected_range = (start, end)
            self.timeRangeSelected.emit(start, end)

    def _clear_selection(self):
        """Clear the time range selection."""
        if HAS_PYQTGRAPH:
            self.packet_region.hide()
            self.select_btn.setChecked(False)
            self._selected_range = None

    def get_selected_range(self) -> Optional[Tuple[datetime, datetime]]:
        """Get the currently selected time range."""
        return self._selected_range

    def highlight_time_range(self, start: datetime, end: datetime):
        """
        Highlight a specific time range.

        Args:
            start: Start datetime
            end: End datetime
        """
        if HAS_PYQTGRAPH:
            self.packet_region.setRegion([start.timestamp(), end.timestamp()])
            self.packet_region.show()
            self._selected_range = (start, end)
