"""Filter panel widget for filtering device data."""

from datetime import datetime
from typing import Optional

from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGroupBox, QLabel,
    QSlider, QSpinBox, QCheckBox, QLineEdit, QPushButton,
    QDateTimeEdit, QComboBox, QScrollArea, QFrame
)
from PyQt6.QtCore import Qt, pyqtSignal, QDateTime


class FilterPanel(QWidget):
    """Panel with various filter options for device data."""

    # Signal emitted when filters change
    filtersChanged = pyqtSignal(dict)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._setup_ui()
        self._connect_signals()

    def _setup_ui(self):
        """Set up the UI components."""
        main_layout = QVBoxLayout(self)

        # Create scroll area for filters
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)

        scroll_content = QWidget()
        scroll_layout = QVBoxLayout(scroll_content)

        # Signal Strength Filter
        signal_group = self._create_signal_filter()
        scroll_layout.addWidget(signal_group)

        # Encryption Filter
        encryption_group = self._create_encryption_filter()
        scroll_layout.addWidget(encryption_group)

        # Device Type Filter
        device_type_group = self._create_device_type_filter()
        scroll_layout.addWidget(device_type_group)

        # Time Range Filter
        time_group = self._create_time_filter()
        scroll_layout.addWidget(time_group)

        # Manufacturer Filter
        manufacturer_group = self._create_manufacturer_filter()
        scroll_layout.addWidget(manufacturer_group)

        # Channel Filter
        channel_group = self._create_channel_filter()
        scroll_layout.addWidget(channel_group)

        scroll_layout.addStretch()
        scroll.setWidget(scroll_content)
        main_layout.addWidget(scroll)

        # Buttons
        button_layout = QHBoxLayout()

        self.apply_btn = QPushButton("Apply Filters")
        self.apply_btn.clicked.connect(self._apply_filters)
        button_layout.addWidget(self.apply_btn)

        self.reset_btn = QPushButton("Reset")
        self.reset_btn.clicked.connect(self.reset_filters)
        button_layout.addWidget(self.reset_btn)

        main_layout.addLayout(button_layout)

        # Auto-apply checkbox
        self.auto_apply_cb = QCheckBox("Auto-apply filters")
        self.auto_apply_cb.setChecked(False)
        main_layout.addWidget(self.auto_apply_cb)

    def _create_signal_filter(self) -> QGroupBox:
        """Create signal strength filter group."""
        group = QGroupBox("Signal Strength (dBm)")
        layout = QVBoxLayout(group)

        # Min signal
        min_layout = QHBoxLayout()
        min_layout.addWidget(QLabel("Min:"))

        self.min_signal_spin = QSpinBox()
        self.min_signal_spin.setRange(-100, 0)
        self.min_signal_spin.setValue(-100)
        self.min_signal_spin.setSuffix(" dBm")
        min_layout.addWidget(self.min_signal_spin)

        self.min_signal_slider = QSlider(Qt.Orientation.Horizontal)
        self.min_signal_slider.setRange(-100, 0)
        self.min_signal_slider.setValue(-100)
        min_layout.addWidget(self.min_signal_slider)

        layout.addLayout(min_layout)

        # Max signal
        max_layout = QHBoxLayout()
        max_layout.addWidget(QLabel("Max:"))

        self.max_signal_spin = QSpinBox()
        self.max_signal_spin.setRange(-100, 0)
        self.max_signal_spin.setValue(0)
        self.max_signal_spin.setSuffix(" dBm")
        max_layout.addWidget(self.max_signal_spin)

        self.max_signal_slider = QSlider(Qt.Orientation.Horizontal)
        self.max_signal_slider.setRange(-100, 0)
        self.max_signal_slider.setValue(0)
        max_layout.addWidget(self.max_signal_slider)

        layout.addLayout(max_layout)

        # Connect sliders and spinboxes
        self.min_signal_spin.valueChanged.connect(self.min_signal_slider.setValue)
        self.min_signal_slider.valueChanged.connect(self.min_signal_spin.setValue)
        self.max_signal_spin.valueChanged.connect(self.max_signal_slider.setValue)
        self.max_signal_slider.valueChanged.connect(self.max_signal_spin.setValue)

        return group

    def _create_encryption_filter(self) -> QGroupBox:
        """Create encryption type filter group."""
        group = QGroupBox("Encryption Type")
        layout = QVBoxLayout(group)

        self.encryption_checkboxes = {}
        encryption_types = ['Open', 'WEP', 'WPA', 'WPA2', 'WPA3', 'Unknown']

        for enc_type in encryption_types:
            cb = QCheckBox(enc_type)
            cb.setChecked(True)
            self.encryption_checkboxes[enc_type] = cb
            layout.addWidget(cb)

        # Select all / none buttons
        btn_layout = QHBoxLayout()
        select_all_btn = QPushButton("All")
        select_all_btn.clicked.connect(lambda: self._set_all_encryption(True))
        btn_layout.addWidget(select_all_btn)

        select_none_btn = QPushButton("None")
        select_none_btn.clicked.connect(lambda: self._set_all_encryption(False))
        btn_layout.addWidget(select_none_btn)

        layout.addLayout(btn_layout)

        return group

    def _create_device_type_filter(self) -> QGroupBox:
        """Create device type filter group."""
        group = QGroupBox("Device Type")
        layout = QVBoxLayout(group)

        self.device_type_checkboxes = {}
        device_types = ['Wi-Fi AP', 'Wi-Fi Client', 'Bluetooth', 'BTLE', 'Other']

        for dev_type in device_types:
            cb = QCheckBox(dev_type)
            cb.setChecked(True)
            self.device_type_checkboxes[dev_type] = cb
            layout.addWidget(cb)

        return group

    def _create_time_filter(self) -> QGroupBox:
        """Create time range filter group."""
        group = QGroupBox("Time Range")
        layout = QVBoxLayout(group)

        self.time_filter_enabled = QCheckBox("Enable time filter")
        self.time_filter_enabled.setChecked(False)
        layout.addWidget(self.time_filter_enabled)

        # Start time
        start_layout = QHBoxLayout()
        start_layout.addWidget(QLabel("From:"))
        self.start_time = QDateTimeEdit()
        self.start_time.setCalendarPopup(True)
        self.start_time.setDateTime(QDateTime.currentDateTime().addDays(-7))
        self.start_time.setEnabled(False)
        start_layout.addWidget(self.start_time)
        layout.addLayout(start_layout)

        # End time
        end_layout = QHBoxLayout()
        end_layout.addWidget(QLabel("To:"))
        self.end_time = QDateTimeEdit()
        self.end_time.setCalendarPopup(True)
        self.end_time.setDateTime(QDateTime.currentDateTime())
        self.end_time.setEnabled(False)
        end_layout.addWidget(self.end_time)
        layout.addLayout(end_layout)

        # Connect enable checkbox
        self.time_filter_enabled.toggled.connect(self.start_time.setEnabled)
        self.time_filter_enabled.toggled.connect(self.end_time.setEnabled)

        return group

    def _create_manufacturer_filter(self) -> QGroupBox:
        """Create manufacturer filter group."""
        group = QGroupBox("Manufacturer")
        layout = QVBoxLayout(group)

        self.manufacturer_input = QLineEdit()
        self.manufacturer_input.setPlaceholderText("Enter manufacturer name...")
        layout.addWidget(self.manufacturer_input)

        return group

    def _create_channel_filter(self) -> QGroupBox:
        """Create channel filter group."""
        group = QGroupBox("Channel")
        layout = QVBoxLayout(group)

        # 2.4 GHz channels
        label_24 = QLabel("2.4 GHz:")
        layout.addWidget(label_24)

        self.channel_24_checkboxes = {}
        channel_layout_24 = QHBoxLayout()
        for ch in range(1, 15):
            cb = QCheckBox(str(ch))
            cb.setChecked(True)
            self.channel_24_checkboxes[ch] = cb
            channel_layout_24.addWidget(cb)
        layout.addLayout(channel_layout_24)

        # 5 GHz channels (common ones)
        label_5 = QLabel("5 GHz:")
        layout.addWidget(label_5)

        self.channel_5_checkboxes = {}
        channels_5ghz = [36, 40, 44, 48, 52, 56, 60, 64, 100, 104, 108, 112,
                         116, 120, 124, 128, 132, 136, 140, 144, 149, 153, 157, 161, 165]

        # Split into rows of 8
        for i in range(0, len(channels_5ghz), 8):
            row_layout = QHBoxLayout()
            for ch in channels_5ghz[i:i+8]:
                cb = QCheckBox(str(ch))
                cb.setChecked(True)
                self.channel_5_checkboxes[ch] = cb
                row_layout.addWidget(cb)
            layout.addLayout(row_layout)

        # Select all / none buttons
        btn_layout = QHBoxLayout()
        select_all_btn = QPushButton("All Channels")
        select_all_btn.clicked.connect(lambda: self._set_all_channels(True))
        btn_layout.addWidget(select_all_btn)

        select_none_btn = QPushButton("None")
        select_none_btn.clicked.connect(lambda: self._set_all_channels(False))
        btn_layout.addWidget(select_none_btn)

        layout.addLayout(btn_layout)

        return group

    def _connect_signals(self):
        """Connect signals for auto-apply."""
        # When auto-apply is enabled, emit changes immediately
        self.min_signal_spin.valueChanged.connect(self._check_auto_apply)
        self.max_signal_spin.valueChanged.connect(self._check_auto_apply)
        self.manufacturer_input.textChanged.connect(self._check_auto_apply)

        for cb in self.encryption_checkboxes.values():
            cb.toggled.connect(self._check_auto_apply)

        for cb in self.device_type_checkboxes.values():
            cb.toggled.connect(self._check_auto_apply)

    def _check_auto_apply(self):
        """Check if auto-apply is enabled and apply filters."""
        if self.auto_apply_cb.isChecked():
            self._apply_filters()

    def _set_all_encryption(self, checked: bool):
        """Set all encryption checkboxes to the given state."""
        for cb in self.encryption_checkboxes.values():
            cb.setChecked(checked)

    def _set_all_channels(self, checked: bool):
        """Set all channel checkboxes to the given state."""
        for cb in self.channel_24_checkboxes.values():
            cb.setChecked(checked)
        for cb in self.channel_5_checkboxes.values():
            cb.setChecked(checked)

    def _apply_filters(self):
        """Apply current filter settings and emit signal."""
        filters = self.get_filters()
        self.filtersChanged.emit(filters)

    def get_filters(self) -> dict:
        """Get current filter values as a dictionary."""
        filters = {
            'min_signal': self.min_signal_spin.value(),
            'max_signal': self.max_signal_spin.value(),
            'manufacturer': self.manufacturer_input.text().strip() or None,
        }

        # Encryption types
        filters['encryption'] = [
            enc_type for enc_type, cb in self.encryption_checkboxes.items()
            if cb.isChecked()
        ]

        # Device types
        filters['device_types'] = [
            dev_type for dev_type, cb in self.device_type_checkboxes.items()
            if cb.isChecked()
        ]

        # Time range
        if self.time_filter_enabled.isChecked():
            filters['start_time'] = self.start_time.dateTime().toPyDateTime()
            filters['end_time'] = self.end_time.dateTime().toPyDateTime()
        else:
            filters['start_time'] = None
            filters['end_time'] = None

        # Channels
        selected_channels = []
        for ch, cb in self.channel_24_checkboxes.items():
            if cb.isChecked():
                selected_channels.append(ch)
        for ch, cb in self.channel_5_checkboxes.items():
            if cb.isChecked():
                selected_channels.append(ch)
        filters['channels'] = selected_channels if selected_channels else None

        return filters

    def set_filters(self, filters: dict):
        """Set filter values from a dictionary."""
        if 'min_signal' in filters:
            self.min_signal_spin.setValue(filters['min_signal'])

        if 'max_signal' in filters:
            self.max_signal_spin.setValue(filters['max_signal'])

        if 'manufacturer' in filters and filters['manufacturer']:
            self.manufacturer_input.setText(filters['manufacturer'])

        if 'start_time' in filters and filters['start_time']:
            self.time_filter_enabled.setChecked(True)
            self.start_time.setDateTime(QDateTime(filters['start_time']))

        if 'end_time' in filters and filters['end_time']:
            self.end_time.setDateTime(QDateTime(filters['end_time']))

    def reset_filters(self):
        """Reset all filters to default values."""
        self.min_signal_spin.setValue(-100)
        self.max_signal_spin.setValue(0)
        self.manufacturer_input.clear()
        self.time_filter_enabled.setChecked(False)

        self._set_all_encryption(True)

        for cb in self.device_type_checkboxes.values():
            cb.setChecked(True)

        self._set_all_channels(True)

        self._apply_filters()

    def set_time_range(self, start: datetime, end: datetime):
        """Set the time range from database bounds."""
        if start:
            self.start_time.setDateTime(QDateTime(start))
        if end:
            self.end_time.setDateTime(QDateTime(end))
