"""Reusable device table view widget."""

from typing import Optional, Callable
import pandas as pd

from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QTableView, QLineEdit,
    QLabel, QPushButton, QMenu, QHeaderView, QAbstractItemView,
    QMessageBox, QApplication, QComboBox
)
from PyQt6.QtCore import Qt, QSortFilterProxyModel, QAbstractTableModel, QModelIndex, pyqtSignal
from PyQt6.QtGui import QAction, QColor, QBrush


class PandasTableModel(QAbstractTableModel):
    """Table model for displaying pandas DataFrame."""

    def __init__(self, df: pd.DataFrame = None, parent=None):
        super().__init__(parent)
        self._df = df if df is not None else pd.DataFrame()
        self._original_df = self._df.copy()

    def rowCount(self, parent=QModelIndex()):
        if parent.isValid():
            return 0
        return len(self._df)

    def columnCount(self, parent=QModelIndex()):
        if parent.isValid():
            return 0
        return len(self._df.columns)

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid():
            return None

        row = index.row()
        col = index.column()

        if row >= len(self._df) or col >= len(self._df.columns):
            return None

        value = self._df.iloc[row, col]

        if role == Qt.ItemDataRole.DisplayRole:
            if pd.isna(value):
                return ""
            if hasattr(value, 'strftime'):
                return value.strftime("%Y-%m-%d %H:%M:%S")
            return str(value)

        elif role == Qt.ItemDataRole.BackgroundRole:
            # Color code by signal strength if column exists
            col_name = self._df.columns[col]
            if col_name == 'strongest_signal' and not pd.isna(value):
                try:
                    signal = int(value)
                    if signal >= -50:
                        return QBrush(QColor(144, 238, 144))  # Light green - excellent
                    elif signal >= -60:
                        return QBrush(QColor(173, 255, 47))   # Green yellow - good
                    elif signal >= -70:
                        return QBrush(QColor(255, 255, 150))  # Light yellow - fair
                    elif signal >= -80:
                        return QBrush(QColor(255, 200, 150))  # Light orange - weak
                    else:
                        return QBrush(QColor(255, 182, 193))  # Light pink - poor
                except (ValueError, TypeError):
                    pass

        elif role == Qt.ItemDataRole.ToolTipRole:
            if pd.isna(value):
                return "No data"
            return str(value)

        return None

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if role == Qt.ItemDataRole.DisplayRole:
            if orientation == Qt.Orientation.Horizontal:
                if section < len(self._df.columns):
                    col_name = str(self._df.columns[section])
                    # Make column names more readable
                    return col_name.replace('_', ' ').title()
            else:
                return str(section + 1)
        return None

    def setDataFrame(self, df: pd.DataFrame, hidden_columns: list = None):
        """Set the DataFrame to display."""
        self.beginResetModel()
        # Store full data for row lookups
        self._full_df = df if df is not None else pd.DataFrame()
        self._original_df = self._full_df.copy()

        # Create display DataFrame without hidden columns
        self._hidden_columns = hidden_columns or []
        if hidden_columns:
            display_cols = [col for col in self._full_df.columns if col not in hidden_columns]
            self._df = self._full_df[display_cols]
        else:
            self._df = self._full_df
        self.endResetModel()

    def getDataFrame(self) -> pd.DataFrame:
        """Get the current DataFrame (display version)."""
        return self._df

    def getFullDataFrame(self) -> pd.DataFrame:
        """Get the full DataFrame including hidden columns."""
        return self._full_df if hasattr(self, '_full_df') else self._df

    def getRowData(self, row: int) -> dict:
        """Get FULL data for a specific row as dictionary (including hidden columns)."""
        full_df = self._full_df if hasattr(self, '_full_df') else self._df
        if row < 0 or row >= len(full_df):
            return {}
        return full_df.iloc[row].to_dict()

    def getColumnName(self, col: int) -> str:
        """Get column name by index."""
        if col < 0 or col >= len(self._df.columns):
            return ""
        return str(self._df.columns[col])


class DeviceTableView(QWidget):
    """Reusable table view for device data with filtering and context menu."""

    # Signals
    deviceSelected = pyqtSignal(dict)  # Emitted when a device row is selected
    deviceDoubleClicked = pyqtSignal(dict)  # Emitted on double-click

    def __init__(self, parent=None):
        super().__init__(parent)
        self._setup_ui()
        self._setup_context_menu()

    def _setup_ui(self):
        """Set up the UI components."""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        # Search/filter bar
        filter_layout = QHBoxLayout()

        self.search_label = QLabel("Search:")
        filter_layout.addWidget(self.search_label)

        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Type to filter...")
        self.search_input.textChanged.connect(self._on_search_changed)
        filter_layout.addWidget(self.search_input)

        self.column_combo = QComboBox()
        self.column_combo.addItem("All Columns")
        self.column_combo.currentIndexChanged.connect(self._on_search_changed)
        filter_layout.addWidget(self.column_combo)

        self.clear_btn = QPushButton("Clear")
        self.clear_btn.clicked.connect(self._clear_search)
        filter_layout.addWidget(self.clear_btn)

        # Row count label
        self.row_count_label = QLabel("0 rows")
        filter_layout.addWidget(self.row_count_label)

        layout.addLayout(filter_layout)

        # Table view
        self.table_view = QTableView()
        self.table_view.setAlternatingRowColors(True)
        self.table_view.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table_view.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.table_view.setSortingEnabled(True)
        self.table_view.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.table_view.customContextMenuRequested.connect(self._show_context_menu)
        self.table_view.doubleClicked.connect(self._on_double_click)

        # Set up header
        header = self.table_view.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        header.setStretchLastSection(True)
        header.setSortIndicatorShown(True)

        # Set up models
        self._source_model = PandasTableModel()
        self._proxy_model = QSortFilterProxyModel()
        self._proxy_model.setSourceModel(self._source_model)
        self._proxy_model.setFilterCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        self.table_view.setModel(self._proxy_model)

        layout.addWidget(self.table_view)

    def _setup_context_menu(self):
        """Set up the right-click context menu."""
        self.context_menu = QMenu(self)

        self.copy_mac_action = QAction("Copy MAC Address", self)
        self.copy_mac_action.triggered.connect(self._copy_mac_address)
        self.context_menu.addAction(self.copy_mac_action)

        self.copy_cell_action = QAction("Copy Cell Value", self)
        self.copy_cell_action.triggered.connect(self._copy_cell_value)
        self.context_menu.addAction(self.copy_cell_action)

        self.copy_row_action = QAction("Copy Row", self)
        self.copy_row_action.triggered.connect(self._copy_row)
        self.context_menu.addAction(self.copy_row_action)

        self.context_menu.addSeparator()

        self.show_details_action = QAction("Show Details...", self)
        self.show_details_action.triggered.connect(self._show_details)
        self.context_menu.addAction(self.show_details_action)

        self.show_on_map_action = QAction("Show on Map", self)
        self.show_on_map_action.triggered.connect(self._show_on_map)
        self.context_menu.addAction(self.show_on_map_action)

        self.context_menu.addSeparator()

        self.export_selected_action = QAction("Export Selected...", self)
        self.export_selected_action.triggered.connect(self._export_selected)
        self.context_menu.addAction(self.export_selected_action)

    def load_data(self, df: pd.DataFrame, exclude_columns: list = None):
        """
        Load data into the table.

        Args:
            df: DataFrame to display
            exclude_columns: List of column names to exclude from display (but keep for data access)
        """
        if df is None:
            df = pd.DataFrame()

        # Pass hidden columns to model - data is kept but not displayed
        self._source_model.setDataFrame(df, hidden_columns=exclude_columns)
        self._update_column_combo()
        self._update_row_count()

        # Auto-resize columns to content
        self.table_view.resizeColumnsToContents()

    def _update_column_combo(self):
        """Update the column filter combo box."""
        self.column_combo.clear()
        self.column_combo.addItem("All Columns")
        df = self._source_model.getDataFrame()
        for col in df.columns:
            self.column_combo.addItem(str(col).replace('_', ' ').title())

    def _update_row_count(self):
        """Update the row count label."""
        total = self._source_model.rowCount()
        filtered = self._proxy_model.rowCount()
        if total == filtered:
            self.row_count_label.setText(f"{total} rows")
        else:
            self.row_count_label.setText(f"{filtered} of {total} rows")

    def _on_search_changed(self):
        """Handle search text or column change."""
        search_text = self.search_input.text()
        column_index = self.column_combo.currentIndex()

        if column_index == 0:
            # Search all columns
            self._proxy_model.setFilterKeyColumn(-1)
        else:
            # Search specific column (subtract 1 for "All Columns" item)
            self._proxy_model.setFilterKeyColumn(column_index - 1)

        self._proxy_model.setFilterRegularExpression(search_text)
        self._update_row_count()

    def _clear_search(self):
        """Clear the search filter."""
        self.search_input.clear()
        self.column_combo.setCurrentIndex(0)

    def _show_context_menu(self, position):
        """Show the context menu at the given position."""
        index = self.table_view.indexAt(position)
        if index.isValid():
            self.context_menu.exec(self.table_view.viewport().mapToGlobal(position))

    def _get_selected_rows(self) -> list:
        """Get list of selected row data."""
        selection = self.table_view.selectionModel().selectedRows()
        rows = []
        for proxy_index in selection:
            source_index = self._proxy_model.mapToSource(proxy_index)
            row_data = self._source_model.getRowData(source_index.row())
            rows.append(row_data)
        return rows

    def _get_current_row(self) -> dict:
        """Get the current row data."""
        index = self.table_view.currentIndex()
        if index.isValid():
            source_index = self._proxy_model.mapToSource(index)
            return self._source_model.getRowData(source_index.row())
        return {}

    def _copy_mac_address(self):
        """Copy the MAC address of the selected device."""
        row_data = self._get_current_row()
        mac = row_data.get('devmac') or row_data.get('client_mac', '')
        if mac:
            clipboard = QApplication.clipboard()
            clipboard.setText(str(mac))

    def _copy_cell_value(self):
        """Copy the value of the current cell."""
        index = self.table_view.currentIndex()
        if index.isValid():
            value = self._proxy_model.data(index, Qt.ItemDataRole.DisplayRole)
            if value:
                clipboard = QApplication.clipboard()
                clipboard.setText(str(value))

    def _copy_row(self):
        """Copy the entire row as tab-separated values."""
        row_data = self._get_current_row()
        if row_data:
            values = [str(v) for v in row_data.values()]
            clipboard = QApplication.clipboard()
            clipboard.setText('\t'.join(values))

    def _show_details(self):
        """Show detailed view of the selected device."""
        row_data = self._get_current_row()
        if row_data:
            self.deviceDoubleClicked.emit(row_data)

    def _show_on_map(self):
        """Show the selected device on the map."""
        row_data = self._get_current_row()
        lat = row_data.get('min_lat') or row_data.get('lat')
        lon = row_data.get('min_lon') or row_data.get('lon')
        if lat and lon and lat != 0 and lon != 0:
            # This will be handled by the main window
            QMessageBox.information(
                self,
                "Show on Map",
                f"Location: {lat}, {lon}\n\nMap view will be implemented in Phase 3."
            )
        else:
            QMessageBox.warning(self, "No GPS Data", "This device has no GPS coordinates.")

    def _export_selected(self):
        """Export selected rows."""
        rows = self._get_selected_rows()
        if not rows:
            QMessageBox.warning(self, "No Selection", "Please select rows to export.")
            return

        # This will be handled by the main window in Phase 4
        QMessageBox.information(
            self,
            "Export",
            f"Export {len(rows)} rows will be implemented in Phase 4."
        )

    def _on_double_click(self, index):
        """Handle double-click on a row."""
        if index.isValid():
            source_index = self._proxy_model.mapToSource(index)
            row_data = self._source_model.getRowData(source_index.row())
            self.deviceDoubleClicked.emit(row_data)

    def get_selected_data(self) -> pd.DataFrame:
        """Get DataFrame of selected rows."""
        rows = self._get_selected_rows()
        if rows:
            return pd.DataFrame(rows)
        return pd.DataFrame()

    def get_all_data(self) -> pd.DataFrame:
        """Get the full DataFrame."""
        return self._source_model.getDataFrame()

    def apply_signal_filter(self, min_signal: int, max_signal: int):
        """Apply signal strength filter."""
        # This is a simple implementation - for more complex filtering,
        # we'd need a custom proxy model
        df = self._source_model._original_df.copy()
        if 'strongest_signal' in df.columns:
            mask = (df['strongest_signal'] >= min_signal) & (df['strongest_signal'] <= max_signal)
            df = df[mask]
        self._source_model.setDataFrame(df)
        self._update_row_count()
