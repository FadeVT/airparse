"""Main window for Kismet GUI Reader."""

from PyQt6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QToolBar, QStatusBar, QTabWidget, QStackedWidget,
    QTreeWidget, QTreeWidgetItem, QSplitter, QLabel,
    QFileDialog, QMessageBox, QDockWidget
)
from PyQt6.QtCore import Qt, QSize
from PyQt6.QtGui import QAction

from database.reader import KismetDBReader
from ui.device_table import DeviceTableView
from ui.filters import FilterPanel
from ui.statistics import StatisticsPanel
from ui.device_detail import show_device_detail
from ui.network_detail import show_network_detail
from ui.map_view import MapView
from ui.timeline import TimelineView
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
        self.setup_ui()
        self.setup_menus()
        self.setup_toolbar()
        self.setup_status_bar()
        self._setup_filter_dock()

    def setup_ui(self):
        """Set up the main UI layout."""
        self.setWindowTitle("Kismet Database Reader")
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

        # Set splitter sizes (sidebar:content = 1:4)
        splitter.setSizes([250, 950])

    def _create_sidebar(self) -> QWidget:
        """Create the sidebar with navigation tree."""
        sidebar = QWidget()
        layout = QVBoxLayout(sidebar)
        layout.setContentsMargins(0, 0, 0, 0)

        # Navigation tree
        self.nav_tree = QTreeWidget()
        self.nav_tree.setHeaderLabel("Navigation")
        self.nav_tree.setMinimumWidth(200)

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
        open_btn = QAction("Open DB", self)
        open_btn.setToolTip("Open Kismet Database")
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
        """Open a Kismet database file."""
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Open Kismet Database",
            "",
            "Kismet Database (*.kismet);;All Files (*.*)"
        )

        if file_path:
            try:
                self.db_reader.open_database(file_path)
                self.db_path_label.setText(f"Database: {file_path}")
                self.update_overview()
                self._update_filter_time_range()
                self.status_bar.showMessage("Database loaded successfully", 3000)
            except Exception as e:
                QMessageBox.critical(
                    self,
                    "Error",
                    f"Failed to open database:\n{str(e)}"
                )

    def close_database(self):
        """Close the current database."""
        self.db_reader.close_database()
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
        self.main_table_view.load_data(df, exclude_columns=['device'])
        self.content_stack.setCurrentWidget(self.main_table_view)
        self._update_main_tab_title("Access Points")

    def _show_clients(self):
        """Show Clients in the main table view."""
        if not self.db_reader.is_connected():
            QMessageBox.warning(self, "Warning", "No database loaded.")
            return

        self.tab_widget.setCurrentIndex(0)
        df = self.db_reader.get_clients(self._current_filters)
        self.main_table_view.load_data(df, exclude_columns=['device'])
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
        """Show Networks (SSIDs) in the main table view."""
        if not self.db_reader.is_connected():
            QMessageBox.warning(self, "Warning", "No database loaded.")
            return

        self.tab_widget.setCurrentIndex(0)
        df = self.db_reader.get_networks()
        self.main_table_view.load_data(df)
        self.content_stack.setCurrentWidget(self.main_table_view)
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

    def show_filters(self):
        """Show the filters panel."""
        self.filter_dock.show()
        self.filter_action.setChecked(True)

    def _on_filters_changed(self, filters: dict):
        """Handle filter changes."""
        self._current_filters = filters
        self._refresh_open_tabs()
        self.status_bar.showMessage("Filters applied", 2000)

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

        # Load GPS track data
        gps_df = self.db_reader.get_gps_data()
        if not gps_df.empty:
            self.main_map_view.plot_gps_track(gps_df)

    def _on_timeline_range_selected(self, start, end):
        """Handle time range selection from timeline."""
        self.status_bar.showMessage(
            f"Selected time range: {start.strftime('%Y-%m-%d %H:%M')} to {end.strftime('%Y-%m-%d %H:%M')}",
            5000
        )

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
            "About Kismet Database Reader",
            "Kismet Database GUI Reader\n\n"
            "A cross-platform desktop application for reading and visualizing "
            "Kismet wireless network capture data.\n\n"
            "Built with PyQt6"
        )

    def closeEvent(self, event):
        """Handle window close event."""
        self.db_reader.close_database()
        event.accept()
