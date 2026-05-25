"""Mapping tab — QGIS export pipeline.

Lifted out of WigleView. The QGIS page widget is constructed by
WigleView.create_qgis_page() and re-parented here; backing workers and
state live on WigleView (it owns the WiGLE API client and KML cache).
"""

from PyQt6.QtWidgets import QStackedWidget, QVBoxLayout, QWidget


class MappingView(QWidget):
    PAGE_QGIS = 0

    def __init__(self, qgis_page: QWidget, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        self._stack = QStackedWidget()
        self._stack.addWidget(qgis_page)
        layout.addWidget(self._stack)

    def show_page(self, index: int):
        self._stack.setCurrentIndex(index)
