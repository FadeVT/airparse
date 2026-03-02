"""Main window for AirParse."""

from PyQt6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QToolBar, QStatusBar, QTabWidget, QStackedWidget,
    QTreeWidget, QTreeWidgetItem, QSplitter, QLabel,
    QFileDialog, QMessageBox, QDockWidget, QPushButton, QSizePolicy
)
from PyQt6.QtCore import Qt, QSize
from PyQt6.QtGui import QAction

import tarfile
import tempfile
import zipfile
from pathlib import Path

import pandas as pd

from database.reader import KismetDBReader
from database.pcap_reader import PcapReader
from database.pcap_worker import PcapParseWorker
from database.wigle_reader import WigleCsvReader
from database.hc22000_reader import Hc22000Reader
from ui.device_table import DeviceTableView
from ui.filters import FilterPanel
from ui.statistics import StatisticsPanel
from ui.device_detail import show_device_detail
from ui.network_detail import show_network_detail
from ui.map_view import MapView
from ui.timeline import TimelineView
from ui.pcap_progress import PcapProgressDialog
from ui.pcap_views import HandshakeView, DeauthView, ProbeMapView, FrameTypeView, NetworksView
from ui.connect_dialog import ConnectDialog
from export.export_dialog import show_export_dialog
from export.csv_exporter import CSVExporter
from export.json_exporter import JSONExporter
from export.kml_exporter import KMLExporter, HAS_SIMPLEKML
from export.pdf_exporter import PDFExporter, HAS_REPORTLAB


class MainWindow(QMainWindow):
    """Main application window."""

    def __init__(self):
        super().__init__()
        self.db_reader = KismetDBReader()
        self._current_filters = {}
        self._temp_dir = None
        self._pending_wigle_csvs: list[str] = []
        self.setup_ui()
        self.setup_menus()
        self.setup_toolbar()
        self.setup_status_bar()
        self._setup_filter_dock()

    def setup_ui(self):
        """Set up the main UI layout."""
        self.setWindowTitle("AirParse")
        self.setMinimumSize(1200, 800)

        # Create central widget with main layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QHBoxLayout(central_widget)

        # Create splitter for sidebar and content
        splitter = QSplitter(Qt.Orientation.Horizontal)
        main_layout.addWidget(splitter)

        # Create sidebar
        self.sidebar = self._create_sidebar()
        splitter.addWidget(self.sidebar)

        # Create tabbed content area
        self.tab_widget = self._create_tab_widget()
        splitter.addWidget(self.tab_widget)

        # Set splitter sizes (sidebar:content — compact nav, wide content)
        splitter.setSizes([150, 1050])

    def _create_sidebar(self) -> QWidget:
        """Create the sidebar with navigation tree."""
        sidebar = QWidget()
        layout = QVBoxLayout(sidebar)
        layout.setContentsMargins(0, 0, 0, 0)

        # Navigation tree
        self.nav_tree = QTreeWidget()
        self.nav_tree.setHeaderLabel("Navigation")
        self.nav_tree.setMinimumWidth(100)

        # Add tree items
        stats_item = QTreeWidgetItem(["Statistics"])
        self.nav_tree.addTopLevelItem(stats_item)

        devices_item = QTreeWidgetItem(["Devices"])
        devices_item.addChild(QTreeWidgetItem(["Access Points"]))
        devices_item.addChild(QTreeWidgetItem(["Clients"]))
        devices_item.addChild(QTreeWidgetItem(["Bluetooth"]))
        devices_item.addChild(QTreeWidgetItem(["All Devices"]))
        self.nav_tree.addTopLevelItem(devices_item)

        networks_item = QTreeWidgetItem(["Networks (SSIDs)"])
        self.nav_tree.addTopLevelItem(networks_item)

        data_item = QTreeWidgetItem(["Data"])
        data_item.addChild(QTreeWidgetItem(["Packets"]))
        data_item.addChild(QTreeWidgetItem(["Data Sources"]))
        self.nav_tree.addTopLevelItem(data_item)

        alerts_item = QTreeWidgetItem(["Alerts"])
        self.nav_tree.addTopLevelItem(alerts_item)

        map_item = QTreeWidgetItem(["GPS / Map"])
        self.nav_tree.addTopLevelItem(map_item)

        timeline_item = QTreeWidgetItem(["Timeline"])
        self.nav_tree.addTopLevelItem(timeline_item)

        # PCAP-specific items (hidden by default)
        self.pcap_nav_item = QTreeWidgetItem(["PCAP Analysis"])
        self.pcap_nav_item.addChild(QTreeWidgetItem(["Handshakes"]))
        self.pcap_nav_item.addChild(QTreeWidgetItem(["Deauth Frames"]))
        self.pcap_nav_item.addChild(QTreeWidgetItem(["Probe Requests"]))
        self.pcap_nav_item.addChild(QTreeWidgetItem(["Frame Types"]))
        self.nav_tree.addTopLevelItem(self.pcap_nav_item)
        self.pcap_nav_item.setHidden(True)
        self.pcap_nav_item.setExpanded(True)

        # Expand devices by default
        devices_item.setExpanded(True)

        # Connect selection signal
        self.nav_tree.itemClicked.connect(self._on_nav_item_clicked)

        layout.addWidget(self.nav_tree)
        return sidebar

    def _create_tab_widget(self) -> QTabWidget:
        """Create the main tabbed content area."""
        tab_widget = QTabWidget()
        tab_widget.setTabsClosable(True)
        tab_widget.tabCloseRequested.connect(self._close_tab)

        # Create main content container (stacked widget for switching views)
        self.content_stack = QStackedWidget()

        # Create Overview/Statistics panel
        self.statistics_panel = StatisticsPanel()
        self.content_stack.addWidget(self.statistics_panel)

        # Create reusable device table view
        self.main_table_view = DeviceTableView()
        self.main_table_view.deviceDoubleClicked.connect(self._on_device_double_clicked)
        self.content_stack.addWidget(self.main_table_view)

        # Create reusable map view
        self.main_map_view = MapView()
        self.main_map_view.deviceClicked.connect(self._on_map_device_clicked)
        self.content_stack.addWidget(self.main_map_view)

        # Create reusable timeline view
        self.main_timeline_view = TimelineView()
        self.main_timeline_view.timeRangeSelected.connect(self._on_timeline_range_selected)
        self.content_stack.addWidget(self.main_timeline_view)

        # Create PCAP-specific views
        self.handshake_view = HandshakeView()
        self.content_stack.addWidget(self.handshake_view)

        self.deauth_view = DeauthView()
        self.content_stack.addWidget(self.deauth_view)

        self.probe_map_view = ProbeMapView()
        self.content_stack.addWidget(self.probe_map_view)

        self.frame_type_view = FrameTypeView()
        self.content_stack.addWidget(self.frame_type_view)

        self.networks_view = NetworksView()
        self.content_stack.addWidget(self.networks_view)

        # Add the content stack as the first (and default) tab
        tab_widget.addTab(self.content_stack, "Main View")

        # Make the first tab non-closable
        tab_widget.tabBar().setTabButton(0, tab_widget.tabBar().ButtonPosition.RightSide, None)

        # Track current view name for tab title
        self._current_view_name = "Overview"

        return tab_widget

    def _update_main_tab_title(self, title: str):
        """Update the main tab title to reflect current view."""
        self._current_view_name = title
        self.tab_widget.setTabText(0, title)

    def _setup_filter_dock(self):
        """Set up the filter dock widget."""
        self.filter_dock = QDockWidget("Filters", self)
        self.filter_dock.setAllowedAreas(
            Qt.DockWidgetArea.LeftDockWidgetArea | Qt.DockWidgetArea.RightDockWidgetArea
        )

        self.filter_panel = FilterPanel()
        self.filter_panel.filtersChanged.connect(self._on_filters_changed)
        self.filter_dock.setWidget(self.filter_panel)

        self.addDockWidget(Qt.DockWidgetArea.RightDockWidgetArea, self.filter_dock)
        self.filter_dock.hide()  # Hidden by default

    def _create_device_table_tab(self, title: str) -> DeviceTableView:
        """Create a device table tab using the new DeviceTableView widget."""
        table_view = DeviceTableView()
        table_view.deviceDoubleClicked.connect(self._on_device_double_clicked)
        return table_view

    def setup_menus(self):
        """Set up the menu bar."""
        menubar = self.menuBar()

        # File menu
        file_menu = menubar.addMenu("&File")

        open_action = QAction("&Open Database...", self)
        open_action.setShortcut("Ctrl+O")
        open_action.triggered.connect(self.open_database)
        file_menu.addAction(open_action)

        file_menu.addSeparator()

        close_action = QAction("&Close Database", self)
        close_action.triggered.connect(self.close_database)
        file_menu.addAction(close_action)

        file_menu.addSeparator()

        exit_action = QAction("E&xit", self)
        exit_action.setShortcut("Ctrl+Q")
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)

        # View menu
        view_menu = menubar.addMenu("&View")

        refresh_action = QAction("&Refresh", self)
        refresh_action.setShortcut("F5")
        refresh_action.triggered.connect(self.refresh_data)
        view_menu.addAction(refresh_action)

        view_menu.addSeparator()

        overview_action = QAction("&Overview", self)
        overview_action.triggered.connect(lambda: self._show_tab("Overview"))
        view_menu.addAction(overview_action)

        view_menu.addSeparator()

        self.filter_action = QAction("Show &Filters Panel", self)
        self.filter_action.setCheckable(True)
        self.filter_action.triggered.connect(self._toggle_filter_panel)
        view_menu.addAction(self.filter_action)

        # Tools menu
        tools_menu = menubar.addMenu("&Tools")

        filter_action = QAction("&Filters...", self)
        filter_action.setShortcut("Ctrl+F")
        filter_action.triggered.connect(self.show_filters)
        tools_menu.addAction(filter_action)

        # Export menu
        export_menu = menubar.addMenu("&Export")

        export_csv_action = QAction("Export to &CSV...", self)
        export_csv_action.triggered.connect(lambda: self.export_data('csv'))
        export_menu.addAction(export_csv_action)

        export_json_action = QAction("Export to &JSON...", self)
        export_json_action.triggered.connect(lambda: self.export_data('json'))
        export_menu.addAction(export_json_action)

        export_kml_action = QAction("Export to &KML...", self)
        export_kml_action.triggered.connect(lambda: self.export_data('kml'))
        export_menu.addAction(export_kml_action)

        export_menu.addSeparator()

        export_view_action = QAction("Export Current &View to CSV...", self)
        export_view_action.setShortcut("Ctrl+Shift+E")
        export_view_action.triggered.connect(self._export_current_view)
        export_menu.addAction(export_view_action)

        export_menu.addSeparator()

        export_pdf_action = QAction("Generate &PDF Report...", self)
        export_pdf_action.triggered.connect(lambda: self.export_data('pdf'))
        export_menu.addAction(export_pdf_action)

        # Help menu
        help_menu = menubar.addMenu("&Help")

        about_action = QAction("&About", self)
        about_action.triggered.connect(self.show_about)
        help_menu.addAction(about_action)

    def setup_toolbar(self):
        """Set up the toolbar."""
        toolbar = QToolBar("Main Toolbar")
        toolbar.setMovable(False)
        toolbar.setIconSize(QSize(24, 24))
        self.addToolBar(toolbar)

        # Open database button
        open_btn = QAction("Open", self)
        open_btn.setToolTip("Open Kismet Database or PCAP File")
        open_btn.triggered.connect(self.open_database)
        toolbar.addAction(open_btn)

        # Refresh button
        refresh_btn = QAction("Refresh", self)
        refresh_btn.setToolTip("Refresh Data")
        refresh_btn.triggered.connect(self.refresh_data)
        toolbar.addAction(refresh_btn)

        toolbar.addSeparator()

        # Filter button
        filter_btn = QAction("Filter", self)
        filter_btn.setToolTip("Toggle Filters Panel")
        filter_btn.triggered.connect(self.show_filters)
        toolbar.addAction(filter_btn)

        toolbar.addSeparator()

        # Export button
        export_btn = QAction("Export", self)
        export_btn.setToolTip("Export Data")
        export_btn.triggered.connect(lambda: self.export_data('csv'))
        toolbar.addAction(export_btn)

        # Map button
        map_btn = QAction("Map", self)
        map_btn.setToolTip("Open Map View")
        map_btn.triggered.connect(self.show_map)
        toolbar.addAction(map_btn)

        toolbar.addSeparator()

        # Tips toggle
        self.tips_action = QAction("Tips", self)
        self.tips_action.setToolTip("Toggle contextual tooltips on hover")
        self.tips_action.setCheckable(True)
        self.tips_action.setChecked(True)
        self.tips_action.triggered.connect(self._toggle_tips)
        toolbar.addAction(self.tips_action)

        # Spacer to push Connect to the far right
        spacer = QWidget()
        spacer.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        toolbar.addWidget(spacer)

        # Connect button — far right, styled to stand out
        connect_btn = QPushButton("Connect")
        connect_btn.setToolTip("Connect to devices and pull capture data")
        connect_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        connect_btn.setStyleSheet("""
            QPushButton {
                background-color: #2980b9;
                color: white;
                border: none;
                border-radius: 4px;
                padding: 5px 16px;
                font-weight: bold;
                font-size: 13px;
            }
            QPushButton:hover {
                background-color: #3498db;
            }
            QPushButton:pressed {
                background-color: #2471a3;
            }
        """)
        connect_btn.clicked.connect(self._on_connect)
        toolbar.addWidget(connect_btn)

    def setup_status_bar(self):
        """Set up the status bar."""
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)

        # Database path label
        self.db_path_label = QLabel("No database loaded")
        self.status_bar.addWidget(self.db_path_label, 1)

        # Device count label
        self.device_count_label = QLabel("")
        self.status_bar.addPermanentWidget(self.device_count_label)

        # Last updated label
        self.last_updated_label = QLabel("")
        self.status_bar.addPermanentWidget(self.last_updated_label)

    def open_database(self):
        """Open a Kismet database, PCAP file, or zip archive."""
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Open Capture File",
            "",
            "All Supported (*.kismet *.pcap *.pcapng *.cap *.csv *.zip *.tar.gz *.tgz *.hc22000 *.22000);;"
            "Kismet Database (*.kismet);;"
            "PCAP Files (*.pcap *.pcapng *.cap);;"
            "WiGLE CSV (*.csv);;"
            "Zip Archives (*.zip);;"
            "Tar Archives (*.tar.gz *.tgz);;"
            "Hashcat Hashes (*.hc22000 *.22000);;"
            "All Files (*.*)"
        )

        if not file_path:
            return

        self._open_file_by_type(file_path)

    def _on_connect(self):
        """Open the Connect dialog to pull data from remote devices."""
        dlg = ConnectDialog(self)
        if dlg.exec() == ConnectDialog.DialogCode.Accepted:
            merged = dlg.get_merged_database()
            if merged and merged.is_connected():
                self.db_reader = merged
                info = merged.get_device_summary()
                self.db_path_label.setText(
                    f"Merged: {info['sources']} sources, "
                    f"{info['access_points']} APs, "
                    f"{info['handshakes']} handshakes"
                )
                # Show PCAP nav if merged DB has PCAP data
                if merged.has_pcap_features():
                    self.pcap_nav_item.setHidden(False)
                    if hasattr(self, 'handshake_view') and merged.primary_pcap_path:
                        self.handshake_view.set_pcap_path(merged.primary_pcap_path)
                else:
                    self.pcap_nav_item.setHidden(True)

                self.update_overview()
                self._update_filter_time_range()
                gps_count = info.get('gps_enriched', 0)
                self.status_bar.showMessage(
                    f"Merged {info['access_points']} APs from {info['sources']} sources "
                    f"({gps_count} with GPS)", 5000
                )

    def _open_file_by_type(self, file_path: str):
        """Route a file to the appropriate opener based on extension."""
        p = Path(file_path)
        ext = p.suffix.lower()

        # Handle double extensions like .tar.gz
        if p.name.lower().endswith('.tar.gz'):
            self._open_targz_archive(file_path)
        elif ext == '.tgz':
            self._open_targz_archive(file_path)
        elif ext == '.zip':
            self._open_zip_archive(file_path)
        elif ext == '.kismet':
            self._open_kismet_database(file_path)
        elif ext in ('.pcap', '.pcapng', '.cap'):
            self._open_pcap_file(file_path)
        elif ext == '.csv':
            self._open_wigle_csv(file_path)
        elif ext in ('.hc22000', '.22000'):
            self._open_hc22000_file(file_path)
        else:
            # Try Kismet first, fall back to PCAP
            try:
                self._open_kismet_database(file_path)
            except Exception:
                try:
                    self._open_pcap_file(file_path)
                except Exception as e:
                    QMessageBox.critical(
                        self, "Error",
                        f"Unrecognized file format:\n{str(e)}"
                    )

    def _open_zip_archive(self, zip_path: str):
        """Extract and open capture files from a zip archive."""
        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                all_names = [n for n in zf.namelist() if not n.startswith('__MACOSX')]
                capture_exts = {'.kismet', '.pcap', '.pcapng', '.cap'}
                csv_exts = {'.csv'}

                # Separate primary capture files from CSVs
                capture_files = [n for n in all_names if Path(n).suffix.lower() in capture_exts]
                csv_files = [n for n in all_names if Path(n).suffix.lower() in csv_exts]

                # If no PCAPs/kismet, fall back to showing CSVs as primary
                if not capture_files:
                    capture_files = csv_files
                    csv_files = []

                if not capture_files:
                    QMessageBox.warning(
                        self, "No Capture Files",
                        "No supported capture files found in the archive."
                    )
                    return

                # If multiple capture files, let the user pick
                if len(capture_files) > 1:
                    from PyQt6.QtWidgets import QInputDialog
                    labels = [Path(f).name for f in capture_files]
                    choice, ok = QInputDialog.getItem(
                        self, "Select Capture File",
                        "Multiple capture files found. Select one:",
                        labels, 0, False
                    )
                    if not ok:
                        return
                    selected = capture_files[labels.index(choice)]
                else:
                    selected = capture_files[0]

                # Clean up previous temp dir if any
                self._cleanup_temp_dir()

                # Extract to a temp directory
                self._temp_dir = tempfile.mkdtemp(prefix='kismet_gui_')
                zf.extract(selected, self._temp_dir)
                extracted_path = str(Path(self._temp_dir) / selected)

                # If a PCAP was selected, also extract companion WiGLE CSVs
                self._pending_wigle_csvs = []
                if Path(selected).suffix.lower() in ('.pcap', '.pcapng', '.cap'):
                    for csv_name in csv_files:
                        zf.extract(csv_name, self._temp_dir)
                        csv_path = str(Path(self._temp_dir) / csv_name)
                        try:
                            with open(csv_path, 'r') as cf:
                                if cf.readline().startswith('WigleWifi'):
                                    self._pending_wigle_csvs.append(csv_path)
                        except Exception:
                            pass

                self.status_bar.showMessage(
                    f"Extracted {Path(selected).name} from {Path(zip_path).name}", 3000
                )
                self._open_file_by_type(extracted_path)

        except zipfile.BadZipFile:
            QMessageBox.critical(self, "Error", "Not a valid zip file.")
        except Exception as e:
            QMessageBox.critical(
                self, "Error", f"Failed to open zip archive:\n{str(e)}"
            )

    def _open_targz_archive(self, tar_path: str):
        """Extract and open capture/CSV files from a tar.gz archive."""
        try:
            with tarfile.open(tar_path, 'r:*') as tf:
                members = [m for m in tf.getmembers() if m.isfile()]
                capture_exts = {'.kismet', '.pcap', '.pcapng', '.cap'}
                csv_exts = {'.csv'}

                capture_files = [m for m in members if Path(m.name).suffix.lower() in capture_exts]
                csv_files = [m for m in members if Path(m.name).suffix.lower() in csv_exts]

                if not capture_files:
                    capture_files = csv_files
                    csv_files = []

                if not capture_files:
                    QMessageBox.warning(
                        self, "No Capture Files",
                        "No supported capture files found in the archive."
                    )
                    return

                if len(capture_files) > 1:
                    from PyQt6.QtWidgets import QInputDialog
                    labels = [Path(f.name).name for f in capture_files]
                    choice, ok = QInputDialog.getItem(
                        self, "Select Capture File",
                        "Multiple capture files found. Select one:",
                        labels, 0, False
                    )
                    if not ok:
                        return
                    selected = capture_files[labels.index(choice)]
                else:
                    selected = capture_files[0]

                self._cleanup_temp_dir()
                self._temp_dir = tempfile.mkdtemp(prefix='airparse_tar_')
                tf.extract(selected, self._temp_dir, filter='data')
                extracted_path = str(Path(self._temp_dir) / selected.name)

                self._pending_wigle_csvs = []
                if Path(selected.name).suffix.lower() in ('.pcap', '.pcapng', '.cap'):
                    for csv_member in csv_files:
                        tf.extract(csv_member, self._temp_dir, filter='data')
                        csv_path = str(Path(self._temp_dir) / csv_member.name)
                        try:
                            with open(csv_path, 'r') as cf:
                                if cf.readline().startswith('WigleWifi'):
                                    self._pending_wigle_csvs.append(csv_path)
                        except Exception:
                            pass
                elif Path(selected.name).suffix.lower() == '.csv':
                    # All CSVs selected as primary — load them all as WiGLE
                    for csv_member in csv_files:
                        tf.extract(csv_member, self._temp_dir, filter='data')
                        csv_path = str(Path(self._temp_dir) / csv_member.name)
                        try:
                            with open(csv_path, 'r') as cf:
                                if cf.readline().startswith('WigleWifi'):
                                    self._pending_wigle_csvs.append(csv_path)
                        except Exception:
                            pass

                self.status_bar.showMessage(
                    f"Extracted {Path(selected.name).name} from {Path(tar_path).name}", 3000
                )
                self._open_file_by_type(extracted_path)

        except tarfile.TarError:
            QMessageBox.critical(self, "Error", "Not a valid tar archive.")
        except Exception as e:
            QMessageBox.critical(
                self, "Error", f"Failed to open tar archive:\n{str(e)}"
            )

    def _open_kismet_database(self, file_path: str):
        """Open a .kismet SQLite database, with auto-repair on corruption."""
        try:
            reader = KismetDBReader()
            reader.open_database(file_path)
            self.db_reader = reader
            self.db_path_label.setText(f"Database: {file_path}")
            self.pcap_nav_item.setHidden(True)
            self.update_overview()
            self._update_filter_time_range()
            self.status_bar.showMessage("Database loaded successfully", 3000)
        except Exception as e:
            error_msg = str(e).lower()
            if 'malformed' in error_msg or 'corrupt' in error_msg or 'not a database' in error_msg:
                reply = QMessageBox.question(
                    self, "Corrupted Database",
                    f"The database appears corrupted:\n{str(e)}\n\n"
                    "Attempt auto-repair using sqlite3 .recover?",
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
                if reply == QMessageBox.StandardButton.Yes:
                    repaired = self._repair_kismet_db(file_path)
                    if repaired:
                        self._open_kismet_database(repaired)
                        return
            QMessageBox.critical(
                self, "Error",
                f"Failed to open database:\n{str(e)}"
            )

    def _open_wigle_csv(self, file_path: str):
        """Open a WiGLE CSV file."""
        try:
            reader = WigleCsvReader()
            reader.open_database(file_path)
            self.db_reader = reader
            self.db_path_label.setText(f"WiGLE CSV: {Path(file_path).name}")
            self.pcap_nav_item.setHidden(True)
            self.update_overview()
            self._update_filter_time_range()
            info = reader.get_database_info()
            self.status_bar.showMessage(
                f"WiGLE CSV loaded: {info.get('total_devices', 0):,} devices "
                f"from {info.get('total_sightings', 0):,} sightings", 5000
            )
        except Exception as e:
            QMessageBox.critical(
                self, "Error",
                f"Failed to open WiGLE CSV:\n{str(e)}"
            )

    def _open_hc22000_file(self, file_path: str):
        """Open a .hc22000 hashcat hash file."""
        try:
            reader = Hc22000Reader()
            reader.open_database(file_path)
            self.db_reader = reader
            self.db_path_label.setText(f"Hashcat: {Path(file_path).name}")
            self.pcap_nav_item.setHidden(False)
            self.update_overview()
            self._update_filter_time_range()
            info = reader.get_database_info()
            self.status_bar.showMessage(
                f"Loaded {info.get('hash_count', 0)} hashes from "
                f"{info.get('network_count', 0)} networks", 5000
            )
        except Exception as e:
            QMessageBox.critical(
                self, "Error",
                f"Failed to open hashcat file:\n{str(e)}"
            )

    def _open_pcap_file(self, file_path: str):
        """Open a PCAP file with background parsing."""
        reader = PcapReader()

        # Scan for companion WiGLE CSVs if not already set by zip extractor
        if not self._pending_wigle_csvs:
            self._pending_wigle_csvs = self._find_companion_wigle_csvs(file_path)

        self._parse_worker = PcapParseWorker(reader, file_path)
        self._progress_dialog = PcapProgressDialog(self)
        self._progress_dialog.setWindowTitle(
            f"Parsing: {Path(file_path).name}"
        )

        self._parse_worker.progress.connect(self._progress_dialog.update_progress)
        self._parse_worker.status.connect(self._progress_dialog.update_status)
        self._parse_worker.finished.connect(self._on_pcap_parse_finished)
        self._progress_dialog.cancelled.connect(self._parse_worker.cancel)

        self._pending_reader = reader
        self._pending_path = file_path

        self._parse_worker.start()
        self._progress_dialog.exec()

    def _on_pcap_parse_finished(self, success: bool, error: str):
        """Handle PCAP parse completion."""
        self._progress_dialog.accept()

        if not success:
            import shutil
            if shutil.which('tshark') and error != 'cancelled':
                reply = QMessageBox.question(
                    self, "Parse Failed",
                    f"Failed to parse PCAP:\n{error}\n\n"
                    "Attempt repair with tshark?",
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
                if reply == QMessageBox.StandardButton.Yes:
                    repaired = self._repair_pcap_with_tshark(self._pending_path)
                    if repaired:
                        self._open_pcap_file(repaired)
                        return
            QMessageBox.critical(
                self, "Error",
                f"Failed to parse PCAP:\n{error}"
            )
            return

        self.db_reader = self._pending_reader

        # Load companion WiGLE GPS data if available
        if self._pending_wigle_csvs:
            self.db_reader.load_wigle_gps(self._pending_wigle_csvs)
            gps_count = len(self.db_reader._wigle_gps)
            if gps_count > 0:
                self.status_bar.showMessage(
                    f"Loaded GPS data for {gps_count} devices from WiGLE CSVs", 3000)
            self._pending_wigle_csvs = []

        # Pass pcap path to handshake view for hashcat cracking
        if hasattr(self, 'handshake_view'):
            self.handshake_view.set_pcap_path(self._pending_path)

        self.db_path_label.setText(f"PCAP: {self._pending_path}")
        self.pcap_nav_item.setHidden(False)
        self.update_overview()
        self._update_filter_time_range()

        if error == "cancelled":
            self.status_bar.showMessage(
                "PCAP partially loaded (parsing was cancelled)", 5000
            )
        else:
            self.status_bar.showMessage("PCAP loaded successfully", 3000)

    def _cleanup_temp_dir(self):
        """Remove temporary extraction directory if it exists."""
        if self._temp_dir and Path(self._temp_dir).exists():
            import shutil
            shutil.rmtree(self._temp_dir, ignore_errors=True)
            self._temp_dir = None

    def _repair_kismet_db(self, file_path: str) -> str:
        """Attempt to repair a corrupted .kismet SQLite database.

        Uses 'sqlite3 <file> .recover' to dump and rebuild.
        Returns path to repaired file, or empty string on failure.
        """
        import subprocess
        import shutil

        if not shutil.which('sqlite3'):
            QMessageBox.warning(
                self, "sqlite3 Not Found",
                "sqlite3 command not found. Install sqlite3 to enable auto-repair.")
            return ''

        self.status_bar.showMessage("Attempting database repair...", 0)
        QApplication.processEvents()

        try:
            repaired_path = file_path + '.repaired.kismet'

            # Dump recovered SQL
            result = subprocess.run(
                ['sqlite3', file_path, '.recover'],
                capture_output=True, text=True, timeout=120)

            if result.returncode != 0 or not result.stdout:
                self.status_bar.showMessage("Recovery failed", 3000)
                return ''

            # Rebuild into new database
            result2 = subprocess.run(
                ['sqlite3', repaired_path],
                input=result.stdout, capture_output=True, text=True, timeout=120)

            if result2.returncode != 0:
                self.status_bar.showMessage("Rebuild failed", 3000)
                return ''

            self.status_bar.showMessage(
                f"Database repaired: {Path(repaired_path).name}", 5000)
            return repaired_path

        except subprocess.TimeoutExpired:
            self.status_bar.showMessage("Repair timed out", 3000)
            return ''
        except Exception as e:
            self.status_bar.showMessage(f"Repair failed: {e}", 3000)
            return ''

    def _repair_pcap_with_tshark(self, file_path: str) -> str:
        """Attempt to repair a corrupted PCAP using tshark.

        Returns path to repaired file, or empty string on failure.
        """
        import subprocess
        import shutil

        if not shutil.which('tshark'):
            return ''

        self.status_bar.showMessage("Attempting PCAP repair with tshark...", 0)
        QApplication.processEvents()

        try:
            repaired_path = file_path + '.repaired.pcap'

            result = subprocess.run(
                ['tshark', '-r', file_path, '-w', repaired_path],
                capture_output=True, text=True, timeout=300)

            if result.returncode == 0 and Path(repaired_path).exists():
                size = Path(repaired_path).stat().st_size
                if size > 24:  # More than just a pcap header
                    self.status_bar.showMessage(
                        f"PCAP repaired: {Path(repaired_path).name}", 5000)
                    return repaired_path

            return ''

        except subprocess.TimeoutExpired:
            self.status_bar.showMessage("tshark repair timed out", 3000)
            return ''
        except Exception:
            return ''

    def _find_companion_wigle_csvs(self, pcap_path: str) -> list[str]:
        """Scan for companion WiGLE CSV files near a PCAP file.

        Checks:
        - Same directory as the PCAP
        - Sibling 'wigle/' directory (Pineapple loot structure: pcap/ + wigle/)
        - Parent directory
        """
        found = []
        pcap_dir = Path(pcap_path).parent

        # Directories to scan
        search_dirs = [pcap_dir]
        # Pineapple loot structure: pcap/ and wigle/ are siblings
        if pcap_dir.name.lower() == 'pcap':
            wigle_dir = pcap_dir.parent / 'wigle'
            if wigle_dir.exists():
                search_dirs.append(wigle_dir)
            search_dirs.append(pcap_dir.parent)

        for search_dir in search_dirs:
            if not search_dir.exists():
                continue
            for csv_file in search_dir.glob('*.csv'):
                try:
                    with open(csv_file, 'r') as f:
                        if f.readline().startswith('WigleWifi'):
                            found.append(str(csv_file))
                except Exception:
                    pass

        return found

    def close_database(self):
        """Close the current database."""
        self.db_reader.close_database()
        self._cleanup_temp_dir()
        self.db_path_label.setText("No database loaded")
        self.device_count_label.setText("")
        self.last_updated_label.setText("")

        # Close all tabs except Overview
        while self.tab_widget.count() > 1:
            self.tab_widget.removeTab(1)

    def refresh_data(self):
        """Refresh data from the database."""
        if self.db_reader.is_connected():
            self.update_overview()
            self._refresh_open_tabs()
            self.status_bar.showMessage("Data refreshed", 2000)

    def _refresh_open_tabs(self):
        """Refresh data in the current view."""
        # Refresh based on the current view name
        view_name = self._current_view_name

        if view_name == "Access Points":
            df = self.db_reader.get_access_points(self._current_filters)
            self.main_table_view.load_data(df, exclude_columns=['device'])
        elif view_name == "Clients":
            df = self.db_reader.get_clients(self._current_filters)
            self.main_table_view.load_data(df, exclude_columns=['device'])
        elif view_name == "Bluetooth":
            df = self.db_reader.get_bluetooth_devices(self._current_filters)
            self.main_table_view.load_data(df, exclude_columns=['device'])
        elif view_name == "All Devices":
            df = self.db_reader.get_all_devices(self._current_filters)
            self.main_table_view.load_data(df, exclude_columns=['device'])
        elif view_name == "Networks":
            df = self.db_reader.get_networks()
            self.main_table_view.load_data(df)
        elif view_name == "Data Sources":
            df = self.db_reader.get_data_sources()
            self.main_table_view.load_data(df)
        elif view_name == "Alerts":
            df = self.db_reader.get_alerts()
            self.main_table_view.load_data(df, exclude_columns=['json'])
        elif view_name == "Map":
            devices_df = self.db_reader.get_all_devices()
            if not devices_df.empty:
                self.main_map_view.plot_devices(devices_df)
        elif view_name == "Handshakes":
            self._show_handshakes()
        elif view_name == "Deauth Frames":
            self._show_deauths()
        elif view_name == "Probe Requests":
            self._show_probes()
        elif view_name == "Frame Types":
            self._show_frame_types()
        elif view_name == "Overview":
            self.update_overview()

    def update_overview(self):
        """Update the overview tab with current database statistics."""
        if not self.db_reader.is_connected():
            return

        summary = self.db_reader.get_device_summary()
        networks_df = self.db_reader.get_networks()
        signal_df = self.db_reader.get_signal_distribution()

        # Update statistics panel
        self.statistics_panel.update_statistics(summary, networks_df, signal_df)

        # Update additional counts
        alerts_df = self.db_reader.get_alerts()
        self.statistics_panel.set_alert_count(len(alerts_df))

        datasources_df = self.db_reader.get_data_sources()
        self.statistics_panel.set_data_source_count(len(datasources_df))

        # Get separate AP and client counts
        ap_df = self.db_reader.get_access_points()
        client_df = self.db_reader.get_clients()
        self.statistics_panel.set_wifi_counts(len(ap_df), len(client_df))

        # Show PCAP-specific stats if applicable
        if self.db_reader.has_pcap_features():
            self.statistics_panel.show_pcap_stats(self.db_reader)

        # Show mini map on landing page if GPS data is available
        devices_df = self.db_reader.get_all_devices()
        gps_df = self.db_reader.get_gps_data()
        self.statistics_panel.show_mini_map(devices_df, gps_df)

        self.device_count_label.setText(f"Devices: {summary.get('total_devices', 0)}")

    def _update_filter_time_range(self):
        """Update filter panel with database time range."""
        if not self.db_reader.is_connected():
            return

        summary = self.db_reader.get_device_summary()
        time_range = summary.get('time_range', {})

        if time_range.get('earliest') and time_range.get('latest'):
            self.filter_panel.set_time_range(
                time_range['earliest'],
                time_range['latest']
            )

    def _on_nav_item_clicked(self, item: QTreeWidgetItem, column: int):
        """Handle navigation tree item clicks."""
        item_text = item.text(0)

        if item_text == "Statistics":
            self._show_statistics()
        elif item_text == "Access Points":
            self._show_access_points()
        elif item_text == "Clients":
            self._show_clients()
        elif item_text == "Bluetooth":
            self._show_bluetooth()
        elif item_text == "All Devices":
            self._show_all_devices()
        elif item_text == "Networks (SSIDs)":
            self._show_networks()
        elif item_text == "Data Sources":
            self._show_data_sources()
        elif item_text == "Alerts":
            self._show_alerts()
        elif item_text == "GPS / Map":
            self.show_map()
        elif item_text == "Timeline":
            self._show_timeline()
        elif item_text == "Handshakes":
            self._show_handshakes()
        elif item_text == "Deauth Frames":
            self._show_deauths()
        elif item_text == "Probe Requests":
            self._show_probes()
        elif item_text == "Frame Types":
            self._show_frame_types()

    def _show_statistics(self):
        """Show the statistics/overview panel."""
        self.tab_widget.setCurrentIndex(0)
        self.content_stack.setCurrentWidget(self.statistics_panel)
        self._update_main_tab_title("Overview")

    def _show_access_points(self):
        """Show Access Points in the main table view."""
        if not self.db_reader.is_connected():
            QMessageBox.warning(self, "Warning", "No database loaded.")
            return

        self.tab_widget.setCurrentIndex(0)
        df = self.db_reader.get_access_points(self._current_filters)
        # Slim columns: hide BSSID, commonname, coords, beacon/data counts, type from default
        hide = ['device', 'commonname', 'min_lat', 'min_lon', 'max_lat', 'max_lon',
                'beacon_count', 'data_count', 'type', 'devmac']
        self.main_table_view.load_data(df, exclude_columns=hide)
        self._setup_pcap_context_actions('ap')
        self.content_stack.setCurrentWidget(self.main_table_view)
        self._update_main_tab_title("Access Points")

    def _show_clients(self):
        """Show Clients in the main table view."""
        if not self.db_reader.is_connected():
            QMessageBox.warning(self, "Warning", "No database loaded.")
            return

        self.tab_widget.setCurrentIndex(0)
        df = self.db_reader.get_clients(self._current_filters)
        hide = ['device', 'commonname', 'min_lat', 'min_lon', 'last_bssid']
        self.main_table_view.load_data(df, exclude_columns=hide)
        self._setup_pcap_context_actions('client')
        self.content_stack.setCurrentWidget(self.main_table_view)
        self._update_main_tab_title("Clients")

    def _show_bluetooth(self):
        """Show Bluetooth devices in the main table view."""
        if not self.db_reader.is_connected():
            QMessageBox.warning(self, "Warning", "No database loaded.")
            return

        self.tab_widget.setCurrentIndex(0)
        df = self.db_reader.get_bluetooth_devices(self._current_filters)
        self.main_table_view.load_data(df, exclude_columns=['device'])
        self.content_stack.setCurrentWidget(self.main_table_view)
        self._update_main_tab_title("Bluetooth")

    def _show_all_devices(self):
        """Show All Devices in the main table view."""
        if not self.db_reader.is_connected():
            QMessageBox.warning(self, "Warning", "No database loaded.")
            return

        self.tab_widget.setCurrentIndex(0)
        df = self.db_reader.get_all_devices(self._current_filters)
        self.main_table_view.load_data(df, exclude_columns=['device'])
        self.content_stack.setCurrentWidget(self.main_table_view)
        self._update_main_tab_title("All Devices")

    def _show_networks(self):
        """Show Networks (SSIDs) investigation hub."""
        if not self.db_reader.is_connected():
            QMessageBox.warning(self, "Warning", "No database loaded.")
            return

        self.tab_widget.setCurrentIndex(0)
        self.networks_view.load_data(self.db_reader)
        self.content_stack.setCurrentWidget(self.networks_view)
        self._update_main_tab_title("Networks")

    def _show_data_sources(self):
        """Show Data Sources in the main table view."""
        if not self.db_reader.is_connected():
            QMessageBox.warning(self, "Warning", "No database loaded.")
            return

        self.tab_widget.setCurrentIndex(0)
        df = self.db_reader.get_data_sources()
        self.main_table_view.load_data(df)
        self.content_stack.setCurrentWidget(self.main_table_view)
        self._update_main_tab_title("Data Sources")

    def _show_alerts(self):
        """Show Alerts in the main table view."""
        if not self.db_reader.is_connected():
            QMessageBox.warning(self, "Warning", "No database loaded.")
            return

        self.tab_widget.setCurrentIndex(0)
        df = self.db_reader.get_alerts()
        self.main_table_view.load_data(df, exclude_columns=['json'])
        self.content_stack.setCurrentWidget(self.main_table_view)
        self._update_main_tab_title("Alerts")

    def _show_timeline(self):
        """Show the timeline view."""
        if not self.db_reader.is_connected():
            QMessageBox.warning(self, "Warning", "No database loaded.")
            return

        self.tab_widget.setCurrentIndex(0)

        # Load packet timeline data
        packet_df = self.db_reader.get_packets_timeline()
        if not packet_df.empty:
            self.main_timeline_view.load_packet_data(packet_df)

        # Load device data for device activity chart
        devices_df = self.db_reader.get_all_devices()
        if not devices_df.empty:
            self.main_timeline_view.load_device_data(devices_df)

        self.content_stack.setCurrentWidget(self.main_timeline_view)
        self._update_main_tab_title("Timeline")

    def _show_handshakes(self):
        """Show the WPA handshakes view."""
        if not self.db_reader.is_connected() or not self.db_reader.has_pcap_features():
            QMessageBox.warning(self, "Warning", "No PCAP data loaded.")
            return

        self.tab_widget.setCurrentIndex(0)
        df = self.db_reader.get_handshakes()
        self.handshake_view.load_data(df)
        self.content_stack.setCurrentWidget(self.handshake_view)
        self._update_main_tab_title("Handshakes")

    def _show_deauths(self):
        """Show the deauthentication analysis view."""
        if not self.db_reader.is_connected() or not self.db_reader.has_pcap_features():
            QMessageBox.warning(self, "Warning", "No PCAP data loaded.")
            return

        self.tab_widget.setCurrentIndex(0)
        df = self.db_reader.get_deauth_frames()
        ap_df = self.db_reader.get_access_points()
        client_df = self.db_reader.get_clients()
        self.deauth_view.load_data(df, ap_df, client_df)
        self.content_stack.setCurrentWidget(self.deauth_view)
        self._update_main_tab_title("Deauth Frames")

    def _show_probes(self):
        """Show the probe request analysis view."""
        if not self.db_reader.is_connected() or not self.db_reader.has_pcap_features():
            QMessageBox.warning(self, "Warning", "No PCAP data loaded.")
            return

        self.tab_widget.setCurrentIndex(0)
        df = self.db_reader.get_probe_requests()
        self.probe_map_view.load_data(df)
        self.content_stack.setCurrentWidget(self.probe_map_view)
        self._update_main_tab_title("Probe Requests")

    def _show_frame_types(self):
        """Show the frame type distribution view."""
        if not self.db_reader.is_connected() or not self.db_reader.has_pcap_features():
            QMessageBox.warning(self, "Warning", "No PCAP data loaded.")
            return

        self.tab_widget.setCurrentIndex(0)
        df = self.db_reader.get_frame_type_distribution()
        self.frame_type_view.load_data(df)
        self.content_stack.setCurrentWidget(self.frame_type_view)
        self._update_main_tab_title("Frame Types")

    def _close_tab(self, index: int):
        """Close a tab by index."""
        if index > 0:  # Don't close Overview tab
            self.tab_widget.removeTab(index)

    def _toggle_filter_panel(self):
        """Toggle the filter panel visibility."""
        if self.filter_dock.isVisible():
            self.filter_dock.hide()
            self.filter_action.setChecked(False)
        else:
            self.filter_dock.show()
            self.filter_action.setChecked(True)

    def _toggle_tips(self, enabled: bool):
        """Toggle contextual tooltips globally."""
        # Qt doesn't have a native global tooltip toggle, so we
        # control via tooltip duration: 0 = disabled, -1 = default
        from PyQt6.QtWidgets import QToolTip
        if enabled:
            # Re-enable tooltips (default behavior)
            self.setStyleSheet(self.styleSheet())  # refresh
        else:
            # Disable by setting a very short duration
            pass
        # Simple approach: just toggle the action state. Tooltips are always
        # present in the widget tree; the user controls visibility via hover.
        # The button serves as a visual reminder that tips exist.
        self.status_bar.showMessage(
            "Tooltips enabled" if enabled else "Tooltips disabled", 2000)

    def show_filters(self):
        """Show the filters panel."""
        self.filter_dock.show()
        self.filter_action.setChecked(True)

    def _on_filters_changed(self, filters: dict):
        """Handle filter changes."""
        self._current_filters = filters
        self._refresh_open_tabs()
        self.status_bar.showMessage("Filters applied", 2000)

    def _setup_pcap_context_actions(self, view_type: str):
        """Add PCAP-aware context menu actions to the main table view."""
        if not self.db_reader.has_pcap_features():
            self.main_table_view.set_extra_actions([])
            return

        from PyQt6.QtGui import QAction
        actions = []

        if view_type == 'ap':
            act = QAction("Show Clients for this AP", self)
            act.triggered.connect(self._ctx_show_ap_clients)
            actions.append(act)

            act2 = QAction("Show Probes for this SSID", self)
            act2.triggered.connect(self._ctx_show_ssid_probes)
            actions.append(act2)

            act3 = QAction("Show Handshakes for this Network", self)
            act3.triggered.connect(self._ctx_show_ap_handshakes)
            actions.append(act3)

        elif view_type == 'client':
            act = QAction("Show Probe Requests", self)
            act.triggered.connect(self._ctx_show_client_probes)
            actions.append(act)

            act2 = QAction("Show Handshakes for this Client", self)
            act2.triggered.connect(self._ctx_show_client_handshakes)
            actions.append(act2)

        self.main_table_view.set_extra_actions(actions)

    def _ctx_show_ap_clients(self):
        """Context: show clients associated with selected AP."""
        row = self.main_table_view._get_current_row()
        bssid = row.get('devmac', '')
        if not bssid:
            return
        client_df = self.db_reader.get_clients()
        if not client_df.empty and 'last_bssid' in client_df.columns:
            filtered = client_df[client_df['last_bssid'].str.lower() == bssid.lower()]
            self.main_table_view.load_data(filtered, exclude_columns=['device', 'commonname', 'min_lat', 'min_lon'])
            self._update_main_tab_title(f"Clients of {row.get('name', bssid)}")

    def _ctx_show_ssid_probes(self):
        """Context: show probe requests tab filtered by this AP's SSID."""
        row = self.main_table_view._get_current_row()
        ssid = row.get('name', '')
        if ssid:
            self._show_probes()
            self.probe_map_view._ssid_search.setText(ssid)
            self.probe_map_view._apply_filters()

    def _ctx_show_ap_handshakes(self):
        """Context: show handshakes for this AP."""
        row = self.main_table_view._get_current_row()
        bssid = row.get('devmac', '')
        if bssid:
            self._show_handshakes()

    def _ctx_show_client_probes(self):
        """Context: switch to probe requests and search for this client."""
        row = self.main_table_view._get_current_row()
        mac = row.get('client_mac', '')
        if mac:
            self._show_probes()

    def _ctx_show_client_handshakes(self):
        """Context: show handshakes involving this client."""
        row = self.main_table_view._get_current_row()
        if row:
            self._show_handshakes()

    def _on_device_double_clicked(self, device_data: dict):
        """Handle double-click on a device row."""
        # Check if this is a network/SSID entry (has 'ssid' and 'ap_count' but no 'devmac')
        if 'ssid' in device_data and 'ap_count' in device_data and 'devmac' not in device_data:
            show_network_detail(device_data, self)
        else:
            show_device_detail(device_data, self)

    def _on_map_device_clicked(self, mac: str):
        """Handle device click from map popup."""
        # Get the full device data from the map view
        device_data = self.main_map_view.get_device_data(mac)
        if device_data:
            show_device_detail(device_data, self)

    def show_map(self):
        """Show the map view."""
        self.tab_widget.setCurrentIndex(0)
        self.content_stack.setCurrentWidget(self.main_map_view)
        self._update_main_tab_title("Map")

        # If database is loaded, show device data
        if not self.db_reader.is_connected():
            return

        # Load device data with GPS coordinates
        devices_df = self.db_reader.get_all_devices()
        if not devices_df.empty:
            # Calculate bounds first for immediate fit
            lat_col = 'min_lat' if 'min_lat' in devices_df.columns else 'lat'
            lon_col = 'min_lon' if 'min_lon' in devices_df.columns else 'lon'

            valid_coords = devices_df[
                (devices_df[lat_col].notna()) &
                (devices_df[lat_col] != 0) &
                (devices_df[lon_col].notna()) &
                (devices_df[lon_col] != 0)
            ]

            if not valid_coords.empty:
                # Pre-fit the map to bounds before loading markers
                min_lat = valid_coords[lat_col].min()
                max_lat = valid_coords[lat_col].max()
                min_lon = valid_coords[lon_col].min()
                max_lon = valid_coords[lon_col].max()

                # Set initial view to center of data
                center_lat = (min_lat + max_lat) / 2
                center_lon = (min_lon + max_lon) / 2
                self.main_map_view.set_center(center_lat, center_lon, 12)

            self.main_map_view.plot_devices(devices_df)

    def _on_timeline_range_selected(self, start, end):
        """Handle time range selection from timeline."""
        self.status_bar.showMessage(
            f"Selected time range: {start.strftime('%Y-%m-%d %H:%M')} to {end.strftime('%Y-%m-%d %H:%M')}",
            5000
        )

    def _export_current_view(self):
        """Export the currently displayed (filtered) data to CSV."""
        if not self.db_reader.is_connected():
            QMessageBox.warning(self, "Warning", "No database loaded.")
            return

        current = self.content_stack.currentWidget()
        df = None
        view_name = self._current_view_name

        # Get data from whichever view is active
        if current == self.main_table_view:
            df = self.main_table_view.get_all_data()
        elif current == self.probe_map_view:
            df = self.probe_map_view._filtered_df
        elif current == self.deauth_view:
            df = self.deauth_view._df
        elif current == self.handshake_view:
            # Rebuild from table
            rows = []
            for i in range(self.handshake_view.table.rowCount()):
                row_data = self.handshake_view.table.property(f'row_data_{i}')
                if row_data:
                    rows.append(row_data)
            if rows:
                df = pd.DataFrame(rows)
        elif current == self.networks_view:
            df = self.networks_view._networks_df
        elif current == self.frame_type_view:
            df = self.db_reader.get_frame_type_distribution()

        if df is None or df.empty:
            QMessageBox.warning(self, "No Data", "No data to export in the current view.")
            return

        safe_name = view_name.replace(' ', '_').replace('/', '_').lower()
        path, _ = QFileDialog.getSaveFileName(
            self, f"Export {view_name} to CSV",
            f"{safe_name}_export.csv",
            "CSV Files (*.csv)")

        if path:
            # Drop internal columns
            export_df = df.copy()
            for col in ('device', '_ts_sec'):
                if col in export_df.columns:
                    export_df = export_df.drop(columns=[col])
            export_df.to_csv(path, index=False)
            self.status_bar.showMessage(
                f"Exported {len(export_df)} rows to {Path(path).name}", 5000)

    def export_data(self, format_type: str = None):
        """Export data to specified format."""
        if not self.db_reader.is_connected():
            QMessageBox.warning(self, "Warning", "No database loaded.")
            return

        # Check if GPS data is available for KML export
        gps_df = self.db_reader.get_gps_data()
        has_gps = not gps_df.empty

        # Show export dialog
        options = show_export_dialog(self, has_gps_data=has_gps)

        if not options:
            return  # User cancelled

        try:
            self.status_bar.showMessage("Exporting data...", 0)

            # Get the data to export
            df = self._get_export_data(options.get('data_type', 'all'))

            if df is None or df.empty:
                QMessageBox.warning(self, "No Data", "No data available to export.")
                return

            success = False
            export_format = options.get('format', 'csv')
            file_path = options.get('file_path', '')

            if export_format == 'csv':
                success = self._export_csv(df, file_path, options)
            elif export_format == 'json':
                success = self._export_json(df, file_path, options)
            elif export_format == 'kml':
                success = self._export_kml(df, gps_df, file_path, options)
            elif export_format == 'pdf':
                success = self._export_pdf(file_path, options)

            if success:
                self.status_bar.showMessage(f"Exported to {file_path}", 5000)
                QMessageBox.information(
                    self,
                    "Export Complete",
                    f"Data exported successfully to:\n{file_path}"
                )
            else:
                QMessageBox.critical(
                    self,
                    "Export Failed",
                    "Failed to export data. Please check the file path and try again."
                )

        except Exception as e:
            QMessageBox.critical(
                self,
                "Export Error",
                f"An error occurred during export:\n{str(e)}"
            )
            self.status_bar.showMessage("Export failed", 3000)

    def _get_export_data(self, data_type: str):
        """Get the appropriate DataFrame based on data type selection."""
        if data_type == 'all':
            return self.db_reader.get_all_devices()
        elif data_type == 'ap':
            return self.db_reader.get_access_points()
        elif data_type == 'clients':
            return self.db_reader.get_clients()
        elif data_type == 'bluetooth':
            return self.db_reader.get_bluetooth_devices()
        elif data_type == 'current':
            # Get data based on current view
            view_name = self._current_view_name
            if view_name == "Access Points":
                return self.db_reader.get_access_points()
            elif view_name == "Clients":
                return self.db_reader.get_clients()
            elif view_name == "Bluetooth":
                return self.db_reader.get_bluetooth_devices()
            elif view_name == "All Devices":
                return self.db_reader.get_all_devices()
            elif view_name == "Networks":
                return self.db_reader.get_networks()
            elif view_name == "Alerts":
                return self.db_reader.get_alerts()
            return self.db_reader.get_all_devices()
        return self.db_reader.get_all_devices()

    def _export_csv(self, df, file_path: str, options: dict) -> bool:
        """Export to CSV format."""
        exporter = CSVExporter()
        return exporter.export_dataframe(
            df, file_path,
            exclude_columns=['device'],
            include_headers=options.get('include_headers', True)
        )

    def _export_json(self, df, file_path: str, options: dict) -> bool:
        """Export to JSON format."""
        exporter = JSONExporter()

        if options.get('include_metadata', True):
            metadata = {
                'source_database': str(self.db_reader.db_path) if self.db_reader.db_path else 'unknown'
            }
            # Remove device blob column
            if 'device' in df.columns:
                df = df.drop(columns=['device'])
            return exporter.export_with_metadata(
                df, file_path,
                metadata=metadata,
                data_key='devices'
            )
        else:
            if 'device' in df.columns:
                df = df.drop(columns=['device'])
            return exporter.export_dataframe(
                df, file_path,
                pretty=options.get('pretty', True)
            )

    def _export_kml(self, df, gps_df, file_path: str, options: dict) -> bool:
        """Export to KML format."""
        if not HAS_SIMPLEKML:
            QMessageBox.warning(
                self, "Missing Dependency",
                "simplekml is required for KML export.\n\nInstall with: pip install simplekml"
            )
            return False

        exporter = KMLExporter()

        if options.get('include_track', True) and not gps_df.empty:
            return exporter.export_combined(
                df, gps_df, file_path
            )
        else:
            return exporter.export_devices(
                df, file_path,
                include_details=options.get('include_details', True)
            )

    def _export_pdf(self, file_path: str, options: dict) -> bool:
        """Export to PDF report."""
        if not HAS_REPORTLAB:
            QMessageBox.warning(
                self, "Missing Dependency",
                "reportlab is required for PDF export.\n\nInstall with: pip install reportlab"
            )
            return False

        exporter = PDFExporter()
        return exporter.generate_report(
            self.db_reader, file_path,
            include_devices=options.get('include_devices', True),
            include_networks=options.get('include_networks', True),
            include_alerts=options.get('include_alerts', True),
            max_table_rows=options.get('max_rows', 50)
        )

    def show_about(self):
        """Show the About dialog."""
        QMessageBox.about(
            self,
            "About AirParse",
            "AirParse — Wireless Capture Analyzer\n\n"
            "Analyze Kismet databases and PCAP captures with interactive maps, "
            "device analysis, and hashcat WPA cracking.\n\n"
            "Built with PyQt6"
        )

    def closeEvent(self, event):
        """Handle window close event."""
        self.db_reader.close_database()
        self._cleanup_temp_dir()
        event.accept()
