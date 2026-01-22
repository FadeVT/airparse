"""Export dialog for selecting export options."""

from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QGroupBox, QRadioButton,
    QCheckBox, QLabel, QLineEdit, QPushButton, QFileDialog,
    QComboBox, QSpinBox, QTabWidget, QWidget, QButtonGroup,
    QMessageBox
)
from PyQt6.QtCore import Qt


class ExportDialog(QDialog):
    """Dialog for configuring export options."""

    def __init__(self, parent=None, has_gps_data: bool = True):
        super().__init__(parent)
        self.has_gps_data = has_gps_data
        self.export_format = 'csv'
        self.export_options = {}
        self._setup_ui()

    def _setup_ui(self):
        """Set up the dialog UI."""
        self.setWindowTitle("Export Data")
        self.setMinimumWidth(450)

        layout = QVBoxLayout(self)

        # Format selection
        format_group = QGroupBox("Export Format")
        format_layout = QVBoxLayout(format_group)

        self.format_group = QButtonGroup(self)

        self.csv_radio = QRadioButton("CSV (Comma-separated values)")
        self.csv_radio.setChecked(True)
        self.format_group.addButton(self.csv_radio)
        format_layout.addWidget(self.csv_radio)

        self.json_radio = QRadioButton("JSON (JavaScript Object Notation)")
        self.format_group.addButton(self.json_radio)
        format_layout.addWidget(self.json_radio)

        self.kml_radio = QRadioButton("KML (Google Earth)")
        self.kml_radio.setEnabled(self.has_gps_data)
        self.format_group.addButton(self.kml_radio)
        format_layout.addWidget(self.kml_radio)
        if not self.has_gps_data:
            format_layout.addWidget(QLabel("  <i>(No GPS data available)</i>"))

        self.pdf_radio = QRadioButton("PDF Report")
        self.format_group.addButton(self.pdf_radio)
        format_layout.addWidget(self.pdf_radio)

        layout.addWidget(format_group)

        # Data selection
        data_group = QGroupBox("Data to Export")
        data_layout = QVBoxLayout(data_group)

        self.export_all_radio = QRadioButton("All devices")
        self.export_all_radio.setChecked(True)
        data_layout.addWidget(self.export_all_radio)

        self.export_ap_radio = QRadioButton("Access Points only")
        data_layout.addWidget(self.export_ap_radio)

        self.export_clients_radio = QRadioButton("Clients only")
        data_layout.addWidget(self.export_clients_radio)

        self.export_bluetooth_radio = QRadioButton("Bluetooth devices only")
        data_layout.addWidget(self.export_bluetooth_radio)

        self.export_current_radio = QRadioButton("Current view / selection")
        data_layout.addWidget(self.export_current_radio)

        self.data_group = QButtonGroup(self)
        self.data_group.addButton(self.export_all_radio)
        self.data_group.addButton(self.export_ap_radio)
        self.data_group.addButton(self.export_clients_radio)
        self.data_group.addButton(self.export_bluetooth_radio)
        self.data_group.addButton(self.export_current_radio)

        layout.addWidget(data_group)

        # Options (format-specific)
        self.options_group = QGroupBox("Options")
        self.options_layout = QVBoxLayout(self.options_group)

        # CSV options
        self.csv_options = QWidget()
        csv_layout = QVBoxLayout(self.csv_options)
        self.csv_headers_cb = QCheckBox("Include column headers")
        self.csv_headers_cb.setChecked(True)
        csv_layout.addWidget(self.csv_headers_cb)
        self.options_layout.addWidget(self.csv_options)

        # JSON options
        self.json_options = QWidget()
        json_layout = QVBoxLayout(self.json_options)
        self.json_pretty_cb = QCheckBox("Pretty print (formatted)")
        self.json_pretty_cb.setChecked(True)
        json_layout.addWidget(self.json_pretty_cb)
        self.json_metadata_cb = QCheckBox("Include metadata")
        self.json_metadata_cb.setChecked(True)
        json_layout.addWidget(self.json_metadata_cb)
        self.json_options.hide()
        self.options_layout.addWidget(self.json_options)

        # KML options
        self.kml_options = QWidget()
        kml_layout = QVBoxLayout(self.kml_options)
        self.kml_track_cb = QCheckBox("Include GPS track")
        self.kml_track_cb.setChecked(True)
        self.kml_track_cb.setEnabled(self.has_gps_data)
        kml_layout.addWidget(self.kml_track_cb)
        self.kml_details_cb = QCheckBox("Include device details in popups")
        self.kml_details_cb.setChecked(True)
        kml_layout.addWidget(self.kml_details_cb)
        self.kml_options.hide()
        self.options_layout.addWidget(self.kml_options)

        # PDF options
        self.pdf_options = QWidget()
        pdf_layout = QVBoxLayout(self.pdf_options)
        self.pdf_devices_cb = QCheckBox("Include device tables")
        self.pdf_devices_cb.setChecked(True)
        pdf_layout.addWidget(self.pdf_devices_cb)
        self.pdf_networks_cb = QCheckBox("Include networks table")
        self.pdf_networks_cb.setChecked(True)
        pdf_layout.addWidget(self.pdf_networks_cb)
        self.pdf_alerts_cb = QCheckBox("Include alerts")
        self.pdf_alerts_cb.setChecked(True)
        pdf_layout.addWidget(self.pdf_alerts_cb)

        max_rows_layout = QHBoxLayout()
        max_rows_layout.addWidget(QLabel("Max rows per table:"))
        self.pdf_max_rows = QSpinBox()
        self.pdf_max_rows.setRange(10, 500)
        self.pdf_max_rows.setValue(50)
        max_rows_layout.addWidget(self.pdf_max_rows)
        max_rows_layout.addStretch()
        pdf_layout.addLayout(max_rows_layout)

        self.pdf_options.hide()
        self.options_layout.addWidget(self.pdf_options)

        layout.addWidget(self.options_group)

        # Output file
        file_group = QGroupBox("Output File")
        file_layout = QHBoxLayout(file_group)

        self.file_path_edit = QLineEdit()
        self.file_path_edit.setPlaceholderText("Select output file...")
        file_layout.addWidget(self.file_path_edit)

        browse_btn = QPushButton("Browse...")
        browse_btn.clicked.connect(self._browse_file)
        file_layout.addWidget(browse_btn)

        layout.addWidget(file_group)

        # Buttons
        button_layout = QHBoxLayout()
        button_layout.addStretch()

        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)
        button_layout.addWidget(cancel_btn)

        export_btn = QPushButton("Export")
        export_btn.setDefault(True)
        export_btn.clicked.connect(self._do_export)
        button_layout.addWidget(export_btn)

        layout.addLayout(button_layout)

        # Connect format radio buttons
        self.csv_radio.toggled.connect(self._on_format_changed)
        self.json_radio.toggled.connect(self._on_format_changed)
        self.kml_radio.toggled.connect(self._on_format_changed)
        self.pdf_radio.toggled.connect(self._on_format_changed)

    def _on_format_changed(self):
        """Handle format selection change."""
        # Hide all option widgets
        self.csv_options.hide()
        self.json_options.hide()
        self.kml_options.hide()
        self.pdf_options.hide()

        # Show relevant options
        if self.csv_radio.isChecked():
            self.csv_options.show()
            self.export_format = 'csv'
        elif self.json_radio.isChecked():
            self.json_options.show()
            self.export_format = 'json'
        elif self.kml_radio.isChecked():
            self.kml_options.show()
            self.export_format = 'kml'
        elif self.pdf_radio.isChecked():
            self.pdf_options.show()
            self.export_format = 'pdf'

        # Update file extension in path
        current_path = self.file_path_edit.text()
        if current_path:
            base = current_path.rsplit('.', 1)[0]
            self.file_path_edit.setText(f"{base}.{self.export_format}")

    def _browse_file(self):
        """Open file browser dialog."""
        extensions = {
            'csv': "CSV Files (*.csv)",
            'json': "JSON Files (*.json)",
            'kml': "KML Files (*.kml)",
            'pdf': "PDF Files (*.pdf)"
        }

        file_filter = extensions.get(self.export_format, "All Files (*.*)")

        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "Save Export File",
            f"kismet_export.{self.export_format}",
            file_filter
        )

        if file_path:
            self.file_path_edit.setText(file_path)

    def _do_export(self):
        """Validate and accept the dialog."""
        if not self.file_path_edit.text():
            QMessageBox.warning(self, "Missing File", "Please specify an output file.")
            return

        # Collect options
        self.export_options = {
            'format': self.export_format,
            'file_path': self.file_path_edit.text(),
            'data_type': self._get_data_type()
        }

        # Format-specific options
        if self.export_format == 'csv':
            self.export_options['include_headers'] = self.csv_headers_cb.isChecked()
        elif self.export_format == 'json':
            self.export_options['pretty'] = self.json_pretty_cb.isChecked()
            self.export_options['include_metadata'] = self.json_metadata_cb.isChecked()
        elif self.export_format == 'kml':
            self.export_options['include_track'] = self.kml_track_cb.isChecked()
            self.export_options['include_details'] = self.kml_details_cb.isChecked()
        elif self.export_format == 'pdf':
            self.export_options['include_devices'] = self.pdf_devices_cb.isChecked()
            self.export_options['include_networks'] = self.pdf_networks_cb.isChecked()
            self.export_options['include_alerts'] = self.pdf_alerts_cb.isChecked()
            self.export_options['max_rows'] = self.pdf_max_rows.value()

        self.accept()

    def _get_data_type(self) -> str:
        """Get the selected data type."""
        if self.export_all_radio.isChecked():
            return 'all'
        elif self.export_ap_radio.isChecked():
            return 'ap'
        elif self.export_clients_radio.isChecked():
            return 'clients'
        elif self.export_bluetooth_radio.isChecked():
            return 'bluetooth'
        elif self.export_current_radio.isChecked():
            return 'current'
        return 'all'

    def get_options(self) -> dict:
        """Get the configured export options."""
        return self.export_options


def show_export_dialog(parent=None, has_gps_data: bool = True) -> dict:
    """
    Show the export dialog and return options.

    Args:
        parent: Parent widget
        has_gps_data: Whether GPS data is available

    Returns:
        Dictionary of export options, or empty dict if cancelled
    """
    dialog = ExportDialog(parent, has_gps_data)
    if dialog.exec() == QDialog.DialogCode.Accepted:
        return dialog.get_options()
    return {}
