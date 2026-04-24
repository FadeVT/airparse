"""Top-level Cell tab — dedicated to cell-tower coverage analysis.

Stays completely separate from the WiFi/WiGLE tab. Own data (cells.db),
own map, own filters. Reads the same `~/AirParse/Wigle/*.kml` directory
the WiFi side reads, but strictly read-only.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QFont
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QFrame, QGroupBox, QStackedWidget, QCheckBox, QSizePolicy,
    QMessageBox, QProgressBar, QScrollArea, QSplitter,
    QButtonGroup,
)

try:
    from PyQt6.QtWebEngineWidgets import QWebEngineView
    HAS_WEBENGINE = True
except ImportError:
    HAS_WEBENGINE = False

from cell import db, reader, enrich, wigle_api, bands as cbands

log = logging.getLogger(__name__)

_LABEL_STYLE = "color: #e0e0e0; border: none; background: transparent;"
_DIM_STYLE = "color: #999; border: none; background: transparent; font-size: 11px;"
_GREEN = "color: #2ecc71; border: none; background: transparent;"
_YELLOW = "color: #f39c12; border: none; background: transparent;"

_GROUP_STYLE = """
    QGroupBox { color: #e0e0e0; border: 1px solid #444; border-radius: 6px;
                margin-top: 8px; padding-top: 16px; }
    QGroupBox::title { subcontrol-origin: margin; padding: 0 6px; }
"""


def _btn(text, color="#3c3f41", text_color="#e0e0e0", bold=False) -> QPushButton:
    btn = QPushButton(text)
    weight = "bold" if bold else "normal"
    btn.setStyleSheet(f"""
        QPushButton {{
            background-color: {color}; color: {text_color};
            border: 1px solid #555; border-radius: 4px;
            padding: 6px 16px; font-weight: {weight};
        }}
        QPushButton:hover {{ opacity: 0.85; }}
        QPushButton:disabled {{ background-color: #333; color: #666; }}
    """)
    btn.setCursor(Qt.CursorShape.PointingHandCursor)
    return btn


class _ImportWorker(QThread):
    progress = pyqtSignal(str)
    finished_ok = pyqtSignal(object)  # ImportReport
    failed = pyqtSignal(str)

    def __init__(self, force: bool = False):
        super().__init__()
        self._force = force

    def run(self):
        try:
            rep = reader.import_all(
                progress_cb=self.progress.emit,
                force=self._force,
            )
            self.finished_ok.emit(rep)
        except Exception as e:
            log.exception("Cell import failed")
            self.failed.emit(str(e))


class _EnrichWorker(QThread):
    progress = pyqtSignal(str)
    finished_ok = pyqtSignal(object)  # EnrichReport
    failed = pyqtSignal(str)

    def __init__(self, bbox: tuple[float, float, float, float]):
        super().__init__()
        self._bbox = bbox

    def run(self):
        try:
            rep = enrich.enrich_bbox(*self._bbox, progress_cb=self.progress.emit)
            if rep.error:
                self.failed.emit(rep.error)
            else:
                self.finished_ok.emit(rep)
        except Exception as e:
            log.exception("Cell enrich failed")
            self.failed.emit(str(e))


class _EnrichAllWorker(QThread):
    """Walks every tile containing unenriched cells. Cooperative cancel via
    the _cancel flag — polled between tiles, so an in-flight API call still
    completes before we tear down."""
    progress = pyqtSignal(str, int, int)  # (msg, done, total)
    finished_ok = pyqtSignal(object)      # BulkEnrichReport
    failed = pyqtSignal(str)

    def __init__(self, tile_size_deg: float = 1.0):
        super().__init__()
        self._tile_size = tile_size_deg
        self._cancel = False

    def cancel(self):
        self._cancel = True

    def run(self):
        try:
            rep = enrich.enrich_all_unenriched(
                tile_size_deg=self._tile_size,
                progress_cb=lambda msg, done, total:
                    self.progress.emit(msg, done, total),
                is_cancelled=lambda: self._cancel,
            )
            if rep.error:
                self.failed.emit(rep.error)
            else:
                self.finished_ok.emit(rep)
        except Exception as e:
            log.exception("Bulk cell enrich failed")
            self.failed.emit(str(e))


class CellTab(QWidget):
    """The whole Cell tab. Internally a QStackedWidget so future sub-pages
    (Search, Settings) can slot in cleanly without restructuring."""

    PAGE_MAP = 0

    def __init__(self, parent=None):
        super().__init__(parent)
        self._current_carriers: set[str] = set()
        self._current_radio_types: set[str] = set()
        self._current_bands: set[str] = set()
        self._map_ready = False
        self._import_worker: _ImportWorker | None = None
        self._enrich_worker: _EnrichWorker | None = None

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        self._stack = QStackedWidget()
        self._stack.addWidget(self._build_map_page())
        layout.addWidget(self._stack)

    def show_page(self, index: int):
        self._stack.setCurrentIndex(index)

    # ─── Map page ────────────────────────────────────────────────────

    def _build_map_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setSpacing(4)
        layout.setContentsMargins(8, 8, 8, 8)

        # ── Stats strip ──
        stats_frame = QFrame()
        stats_frame.setStyleSheet(
            "QFrame { background-color: #2a2a2a; border: 1px solid #444; border-radius: 4px; }")
        stats_frame.setMaximumHeight(60)
        stats_layout = QHBoxLayout(stats_frame)
        stats_layout.setContentsMargins(12, 4, 12, 4)
        stats_layout.setSpacing(24)

        self._stats_total = QLabel("--")
        self._stats_total.setFont(QFont('', 14, QFont.Weight.Bold))
        self._stats_total.setStyleSheet(_GREEN)
        stats_layout.addWidget(QLabel("Cells:"))
        stats_layout.addWidget(self._stats_total)

        self._stats_detail = QLabel("")
        self._stats_detail.setStyleSheet(_DIM_STYLE)
        stats_layout.addWidget(self._stats_detail)

        stats_layout.addStretch()

        self._import_btn = _btn("Import Cells from WiGLE KMLs", "#2980b9", "white", bold=True)
        self._import_btn.setMaximumHeight(28)
        self._import_btn.clicked.connect(self._trigger_import)
        stats_layout.addWidget(self._import_btn)

        self._enrich_btn = _btn("Enrich Bands in This View", "#27ae60", "white", bold=True)
        self._enrich_btn.setMaximumHeight(28)
        self._enrich_btn.setToolTip(
            "Pull WiGLE's /cell/search for the current map viewport to fill in EARFCN + band. "
            "Adds any cells WiGLE has that you don't."
        )
        self._enrich_btn.clicked.connect(self._trigger_enrich)
        stats_layout.addWidget(self._enrich_btn)

        self._enrich_all_btn = _btn("Enrich All Unenriched", "#8e44ad", "white", bold=True)
        self._enrich_all_btn.setMaximumHeight(28)
        self._enrich_all_btn.clicked.connect(self._trigger_enrich_all)
        stats_layout.addWidget(self._enrich_all_btn)

        layout.addWidget(stats_frame)

        # ── Horizontal split: filter column | map (QSplitter — user can drag) ──
        splitter = QSplitter(Qt.Orientation.Horizontal)
        splitter.setChildrenCollapsible(False)
        splitter.setHandleWidth(4)
        splitter.setStyleSheet(
            "QSplitter::handle { background: #333; } "
            "QSplitter::handle:hover { background: #555; }"
        )

        # Filter column
        filter_col = QVBoxLayout()
        filter_col.setContentsMargins(0, 0, 0, 0)
        filter_col.setSpacing(8)

        self._carrier_group = self._build_checkbox_group(
            "Carriers", on_change=self._apply_filters)
        filter_col.addWidget(self._carrier_group, 1)

        self._radio_group = self._build_checkbox_group(
            "Radio Type", on_change=self._apply_filters)
        filter_col.addWidget(self._radio_group)

        # Band filter — populated as cells gain band info via API enrichment.
        self._band_group = self._build_checkbox_group(
            "Band",
            on_change=self._apply_filters)
        filter_col.addWidget(self._band_group, 1)

        filter_wrap = QWidget()
        filter_wrap.setLayout(filter_col)
        filter_wrap.setMinimumWidth(280)
        splitter.addWidget(filter_wrap)

        # Map column (wraps mode toggle + map)
        map_col = QVBoxLayout()
        map_col.setContentsMargins(0, 0, 0, 0)
        map_col.setSpacing(4)

        mode_row = QHBoxLayout()
        mode_row.setContentsMargins(4, 0, 4, 0)
        self._mode_btn_group = QButtonGroup(self)
        self._mode_btn_group.setExclusive(True)
        self._mode = "dots"
        for key, label in (("dots", "Dots"), ("heatmap", "Heatmap"), ("towers", "Towers")):
            b = QPushButton(label)
            b.setCheckable(True)
            b.setCursor(Qt.CursorShape.PointingHandCursor)
            b.setStyleSheet(
                "QPushButton { background:#3c3f41; color:#e0e0e0; "
                "border:1px solid #555; border-radius:4px; padding:4px 14px; } "
                "QPushButton:checked { background:#2980b9; color:white; }"
            )
            b.clicked.connect(lambda _=False, k=key: self._set_mode(k))
            self._mode_btn_group.addButton(b)
            b._mode_key = key
            mode_row.addWidget(b)
            if key == "dots":
                b.setChecked(True)
        mode_row.addStretch(1)

        mode_wrap = QWidget()
        mode_wrap.setLayout(mode_row)
        mode_wrap.setStyleSheet("background:transparent;")
        map_col.addWidget(mode_wrap)

        if HAS_WEBENGINE:
            try:
                self._map_view = QWebEngineView()
                self._map_view.setSizePolicy(
                    QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
                self._map_view.loadFinished.connect(self._on_map_loaded)
                self._map_view.setHtml(self._map_html())
                map_col.addWidget(self._map_view, 1)
            except Exception:
                self._map_view = None
                map_col.addWidget(
                    QLabel("Map unavailable — QWebEngine init failed"), 1)
        else:
            self._map_view = None
            map_col.addWidget(
                QLabel("Map unavailable — install PyQt6-WebEngine"), 1)

        map_wrap = QWidget()
        map_wrap.setLayout(map_col)
        splitter.addWidget(map_wrap)

        # Default split: ~420px for filters, rest for map. User can drag.
        splitter.setSizes([420, 900])
        splitter.setStretchFactor(0, 0)
        splitter.setStretchFactor(1, 1)

        layout.addWidget(splitter, 1)
        return page

    def _build_checkbox_group(self, title: str, on_change) -> QGroupBox:
        group = QGroupBox(title)
        group.setStyleSheet(_GROUP_STYLE)
        group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        group.setMinimumHeight(180)
        inner = QVBoxLayout(group)
        inner.setContentsMargins(8, 8, 8, 8)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        scroll.setStyleSheet(
            "QScrollArea { border: none; background: transparent; } "
            "QScrollBar:vertical { width: 10px; background: #222; } "
            "QScrollBar::handle:vertical { background: #555; border-radius: 4px; }"
        )

        content = QWidget()
        content.setStyleSheet("background: transparent;")
        cl = QVBoxLayout(content)
        cl.setContentsMargins(0, 0, 0, 0)
        cl.setSpacing(4)
        cl.addStretch(1)
        scroll.setWidget(content)
        inner.addWidget(scroll, 1)

        # Stash the content layout so we can populate checkboxes later.
        group._content_layout = cl
        group._on_change = on_change
        group._checkboxes = {}
        return group

    def _populate_checkboxes(
        self,
        group: QGroupBox,
        items,
        default_checked: bool = True,
    ) -> None:
        """Rebuild the checkbox list, preserving current check state.

        `items` is either a list of strings (key == display text) or a list of
        `(key, display_text, enabled)` tuples. Keys are what gets used for
        filtering — display text is only what the user sees."""
        old_state = {k: cb.isChecked() for k, cb in group._checkboxes.items()}
        while group._content_layout.count() > 1:
            w = group._content_layout.takeAt(0).widget()
            if w is not None:
                w.setParent(None)
        group._checkboxes.clear()

        for item in items:
            if isinstance(item, tuple):
                key, display, enabled = item
            else:
                key, display, enabled = item, item, True
            cb = QCheckBox(display)
            cb.setStyleSheet(_LABEL_STYLE if enabled else _DIM_STYLE)
            cb.setEnabled(enabled)
            # Previously-set state wins; otherwise default_checked — but
            # disabled entries always start unchecked regardless.
            cb.setChecked(enabled and old_state.get(key, default_checked))
            cb.toggled.connect(group._on_change)
            group._checkboxes[key] = cb
            group._content_layout.insertWidget(group._content_layout.count() - 1, cb)

    # ─── Lifecycle ───────────────────────────────────────────────────

    def showEvent(self, event):
        super().showEvent(event)
        self._refresh_filters_and_stats()

    def _refresh_filters_and_stats(self):
        s = db.stats()
        self._stats_total.setText(f"{s['total_cells']:,}")
        parts = []
        for radio, n in list(s["by_radio"].items())[:3]:
            parts.append(f"{radio} {n:,}")
        unenriched = reader.unenriched_operator_count()
        if unenriched:
            parts.append(f"{unenriched:,} towers lack band info")
        self._stats_detail.setText("  •  ".join(parts) if parts else
                                   "No data yet — click Import.")

        self._populate_checkboxes(self._carrier_group, reader.distinct_carriers())
        self._populate_checkboxes(self._radio_group, reader.distinct_radio_types())
        self._populate_band_filter()

        # Enable the Enrich-All button only when there's unenriched data to chew on
        if hasattr(self, "_enrich_all_btn"):
            self._enrich_all_btn.setEnabled(unenriched > 0)
            self._enrich_all_btn.setToolTip(
                f"{unenriched:,} towers have no band info. Walks a grid of WiGLE "
                f"/cell/search calls to fill them in. Respects rate-limit; can cancel."
                if unenriched else
                "All towers in this DB already have band info. Nothing to do."
            )

        self._apply_filters()

    def _populate_band_filter(self):
        """Populate the band checkboxes with ALL standard US LTE + NR bands,
        annotated with the number of resolved towers per band. Bands with no
        data yet stay visible (dimmed) so the user sees the full vocabulary
        and knows what enrichment could reveal."""
        counts = reader.band_counts()
        items = []
        for label in cbands.all_band_labels():
            n = counts.get(label, 0)
            common = cbands.common_name_for(label) or ""
            if n:
                display = f"{label} — {common} ({n:,})" if common else f"{label} ({n:,})"
            else:
                display = f"{label} — {common}" if common else label
            items.append((label, display, n > 0))
        self._populate_checkboxes(self._band_group, items, default_checked=True)

    # ─── Filters → map ───────────────────────────────────────────────

    def _apply_filters(self):
        carriers = {k for k, cb in self._carrier_group._checkboxes.items()
                    if cb.isChecked()}
        radio = {k for k, cb in self._radio_group._checkboxes.items()
                 if cb.isChecked()}
        bands = {k for k, cb in self._band_group._checkboxes.items()
                 if cb.isChecked()}
        cells = reader.query_cells(
            carriers=list(carriers) or None,
            radio_types=list(radio) or None,
            bands=list(bands) or None,
            limit=50000,  # safety cap so the browser doesn't melt
        )
        self._send_points_to_map(cells)

    def _send_points_to_map(self, cells: list[dict]):
        if not self._map_view or not self._map_ready:
            self._pending = cells
            return
        data = json.dumps([
            {
                "lat": c["lat"],
                "lon": c["lon"],
                "carrier": c["carrier"],
                "type": c["radio_type"],
                "signal": c.get("signal_dbm"),
                "cid": c["cid"],
                "op": c["operator_key"],
                "band": c.get("band_label") or "",
            }
            for c in cells
        ])
        self._map_view.page().runJavaScript(
            f"setMode({json.dumps(self._mode)}); setPoints({data});"
        )

    def _set_mode(self, key: str):
        if key == self._mode:
            return
        self._mode = key
        if self._map_view and self._map_ready:
            self._map_view.page().runJavaScript(
                f"setMode({json.dumps(self._mode)});"
            )

    def _on_map_loaded(self, ok: bool):
        if not ok:
            return
        self._map_ready = True
        if getattr(self, "_pending", None):
            self._send_points_to_map(self._pending)
            self._pending = None

    # ─── Import ──────────────────────────────────────────────────────

    def _trigger_import(self):
        if self._import_worker and self._import_worker.isRunning():
            return
        self._import_btn.setEnabled(False)
        self._import_btn.setText("Importing…")
        self._import_worker = _ImportWorker(force=False)
        self._import_worker.progress.connect(
            lambda s: self._stats_detail.setText(s))
        self._import_worker.finished_ok.connect(self._on_import_done)
        self._import_worker.failed.connect(self._on_import_failed)
        self._import_worker.start()

    def _on_import_done(self, rep):
        self._import_btn.setEnabled(True)
        self._import_btn.setText("Import Cells from WiGLE KMLs")
        QMessageBox.information(
            self, "Cell import complete",
            f"Scanned {rep.transids_scanned} KML(s).\n"
            f"Imported {rep.transids_imported} new, skipped {rep.transids_skipped} "
            f"already on record.\n"
            f"Added {rep.cells_inserted:,} cell observations.\n"
            f"{rep.files_without_cell_layer} KML(s) had no Cellular Networks layer."
        )
        self._refresh_filters_and_stats()

    def _on_import_failed(self, msg: str):
        self._import_btn.setEnabled(True)
        self._import_btn.setText("Import Cells from WiGLE KMLs")
        QMessageBox.warning(self, "Cell import failed", msg)

    # ─── Enrich (Slice 2) ───────────────────────────────────────────

    def _trigger_enrich(self):
        if self._enrich_worker and self._enrich_worker.isRunning():
            return
        if not wigle_api.has_credentials():
            QMessageBox.information(
                self, "WiGLE credentials needed",
                "Set your WiGLE API Name + Token in Settings → WiGLE API first. "
                "The Cell tab reuses the same credentials as the WiFi side."
            )
            return
        if not self._map_view or not self._map_ready:
            QMessageBox.information(
                self, "Map not ready",
                "Wait for the map to finish loading, then try again."
            )
            return
        # Ask Leaflet for the current viewport bounds, then kick off the worker.
        self._map_view.page().runJavaScript(
            "(function(){var b=map.getBounds();return ["
            "b.getSouth(),b.getNorth(),b.getWest(),b.getEast()];})()",
            self._on_viewport_bounds,
        )

    def _on_viewport_bounds(self, bounds):
        if not bounds or len(bounds) != 4:
            QMessageBox.warning(
                self, "Couldn't read viewport",
                "Leaflet didn't return a bounding box — try panning or zooming once and retry."
            )
            return
        south, north, west, east = [float(v) for v in bounds]
        self._enrich_btn.setEnabled(False)
        self._enrich_btn.setText("Enriching…")
        self._enrich_worker = _EnrichWorker((south, north, west, east))
        self._enrich_worker.progress.connect(
            lambda s: self._stats_detail.setText(s))
        self._enrich_worker.finished_ok.connect(self._on_enrich_done)
        self._enrich_worker.failed.connect(self._on_enrich_failed)
        self._enrich_worker.start()

    def _on_enrich_done(self, rep):
        self._enrich_btn.setEnabled(True)
        self._enrich_btn.setText("Enrich Bands in This View")
        QMessageBox.information(
            self, "Enrich complete",
            f"Pulled {rep.cells_fetched:,} cell record(s) from WiGLE.\n"
            f"Enriched {rep.rows_enriched:,} existing row(s) with band info.\n"
            f"Added {rep.rows_inserted:,} new cell(s) from WiGLE's coverage.\n"
            f"{rep.rows_skipped_no_band:,} row(s) had no channel data, skipped."
        )
        self._refresh_filters_and_stats()

    def _on_enrich_failed(self, msg: str):
        self._enrich_btn.setEnabled(True)
        self._enrich_btn.setText("Enrich Bands in This View")
        QMessageBox.warning(self, "Enrich failed", msg)

    # ─── Bulk Enrich (all unenriched towers) ─────────────────────────

    def _trigger_enrich_all(self):
        # If an enrich-all is already running, clicking again cancels it.
        if getattr(self, "_enrich_all_worker", None) and self._enrich_all_worker.isRunning():
            self._enrich_all_worker.cancel()
            self._enrich_all_btn.setText("Cancelling…")
            self._enrich_all_btn.setEnabled(False)
            return

        if not wigle_api.has_credentials():
            QMessageBox.information(
                self, "WiGLE credentials needed",
                "Set your WiGLE API Name + Token in Settings → WiGLE API first."
            )
            return

        bbox = reader.unenriched_bbox()
        unenriched = reader.unenriched_operator_count()
        if bbox is None or unenriched == 0:
            QMessageBox.information(
                self, "Nothing to enrich",
                "Every tower in your DB already has band info. Nothing to do."
            )
            return

        # Confirm — this can be a long-running, API-heavy operation.
        south, north, west, east = bbox
        deg_w = max(abs(north - south), 0.5)
        deg_h = max(abs(east - west), 0.5)
        approx_tiles = max(1, int(deg_w * deg_h))  # rough
        reply = QMessageBox.question(
            self, "Enrich all unenriched towers?",
            f"{unenriched:,} towers have no band info.\n"
            f"Bounding region: {deg_w:.1f}° × {deg_h:.1f}° — "
            f"~{approx_tiles:,} tile(s) at 1°×1°.\n\n"
            "Each tile is one WiGLE /cell/search call (rate-limited to ~2s/call, "
            "paginated to 1000/call). The run is incremental — you can cancel "
            "mid-run and pick up where you left off next time.\n\n"
            "Proceed?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return

        self._enrich_all_worker = _EnrichAllWorker(tile_size_deg=1.0)
        self._enrich_all_worker.progress.connect(self._on_enrich_all_progress)
        self._enrich_all_worker.finished_ok.connect(self._on_enrich_all_done)
        self._enrich_all_worker.failed.connect(self._on_enrich_all_failed)
        self._enrich_all_btn.setText("Cancel Bulk Enrich")
        self._enrich_all_btn.setEnabled(True)
        self._enrich_btn.setEnabled(False)
        self._enrich_all_worker.start()

    def _on_enrich_all_progress(self, msg: str, done: int, total: int):
        self._stats_detail.setText(
            f"{msg}  •  {done}/{total}" if total else msg
        )

    def _on_enrich_all_done(self, rep):
        self._reset_enrich_all_ui()
        suffix = "Cancelled partway." if rep.cancelled else "All tiles complete."
        QMessageBox.information(
            self, "Bulk enrich finished",
            f"{suffix}\n\n"
            f"Tiles: {rep.tiles_done}/{rep.tiles_total} "
            f"(+{rep.tiles_skipped_empty} empty tiles skipped).\n"
            f"Fetched {rep.cells_fetched:,} cell record(s) from WiGLE.\n"
            f"Enriched {rep.rows_enriched:,} existing row(s).\n"
            f"Inserted {rep.rows_inserted:,} new cell(s).\n"
            f"{rep.rows_skipped_no_band:,} returned cells had no channel data."
        )
        self._refresh_filters_and_stats()

    def _on_enrich_all_failed(self, msg: str):
        self._reset_enrich_all_ui()
        QMessageBox.warning(self, "Bulk enrich failed", msg)

    def _reset_enrich_all_ui(self):
        self._enrich_all_btn.setText("Enrich All Unenriched")
        self._enrich_all_btn.setEnabled(True)
        self._enrich_btn.setEnabled(True)

    # ─── Map HTML ────────────────────────────────────────────────────

    def _map_html(self) -> str:
        return r"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
  <style>
    * { margin: 0; padding: 0; }
    html, body, #map { width: 100%; height: 100%; background: #1a1a2e; }
    .leaflet-control-attribution { background-color: rgba(30,30,30,0.7) !important; color: #999 !important; }
    .leaflet-popup-content-wrapper { background-color: #2b2b2b !important; color: #e0e0e0 !important; border-radius: 6px !important; }
    .leaflet-popup-tip { background-color: #2b2b2b !important; }
    .cell-popup { font-family: monospace; font-size: 12px; line-height: 1.5; }
    .cell-popup b { color: #2ecc71; }
  </style>
</head>
<body>
  <div id="map"></div>
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script src="https://unpkg.com/leaflet.heat@0.2.0/dist/leaflet-heat.js"></script>
  <script>
    var map = L.map('map', { preferCanvas: true }).setView([43.6, -72.7], 9);
    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
      attribution: '&copy; OpenStreetMap &copy; CARTO',
      subdomains: 'abcd', maxZoom: 20
    }).addTo(map);

    // Color by carrier. Pick a stable hash-ish palette.
    var carrierColors = {
      'T-Mobile': '#E20074',
      'T-Mobile (ex-Sprint)': '#FFCC00',
      'AT&T': '#00A8E0',
      'Verizon': '#CD040B',
      'Dish': '#EC1C24',
      'Dish (Boost)': '#EC1C24',
      'US Cellular': '#0066B3',
      'VTel Wireless': '#5CB85C',
      'Rogers': '#DA291C',
      'Telus': '#66CC00',
      'Bell': '#4B92DB',
    };
    function colorFor(name) {
      if (carrierColors[name]) return carrierColors[name];
      // Stable pseudo-hash fallback
      var h = 0; for (var i = 0; i < (name||'').length; i++) h = (h*31 + name.charCodeAt(i)) & 0xffffffff;
      return 'hsl(' + (Math.abs(h) % 360) + ', 65%, 55%)';
    }

    // Current state, shared across render modes.
    var currentPoints = [];
    var currentMode = 'dots';
    var layer = null;

    function clearLayer() {
      if (layer) {
        if (layer.remove) layer.remove();
        else map.removeLayer(layer);
        layer = null;
      }
    }

    function renderDots(points) {
      var markers = points.map(function(p) {
        return L.circleMarker([p.lat, p.lon], {
          radius: 4,
          color: colorFor(p.carrier),
          weight: 1,
          fillColor: colorFor(p.carrier),
          fillOpacity: 0.7,
        }).bindPopup(
          '<div class="cell-popup">' +
          '<b>' + (p.carrier || 'Unknown') + '</b><br/>' +
          'Type: ' + (p.type || '-') + '<br/>' +
          'Signal: ' + (p.signal !== null && p.signal !== undefined ? p.signal + ' dBm' : '-') + '<br/>' +
          (p.band ? 'Band: ' + p.band + '<br/>' : '') +
          'Key: ' + (p.op || '-') + '<br/>' +
          'CID: ' + (p.cid || '-') +
          '</div>'
        );
      });
      layer = L.layerGroup(markers).addTo(map);
      if (points.length && points.length < 5000) {
        try { map.fitBounds(new L.featureGroup(markers).getBounds().pad(0.05)); } catch (e) {}
      }
    }

    function renderHeatmap(points) {
      // Weight by inverse signal strength. Stronger (less negative dBm) → higher.
      // Floor at 0.1 so every point still contributes a baseline.
      var heatPoints = points.map(function(p) {
        var s = p.signal;
        var intensity = 0.5;
        if (s !== null && s !== undefined) {
          // -50 dBm → 1.0, -120 dBm → ~0.1
          intensity = Math.max(0.1, Math.min(1.0, (s + 120) / 70));
        }
        return [p.lat, p.lon, intensity];
      });
      layer = L.heatLayer(heatPoints, {
        radius: 20,
        blur: 18,
        maxZoom: 14,
        gradient: { 0.2: 'blue', 0.4: 'cyan', 0.6: 'lime', 0.8: 'yellow', 1.0: 'red' },
      }).addTo(map);
    }

    function renderTowers(points) {
      // Aggregate by operator_key.
      var agg = {};
      points.forEach(function(p) {
        var key = p.op || 'unknown';
        var bucket = agg[key];
        if (!bucket) {
          agg[key] = {
            count: 0, carrier: p.carrier, type: p.type, band: p.band,
            latSum: 0, lonSum: 0, op: p.op, cid: p.cid,
            bestSignal: -999,
          };
          bucket = agg[key];
        }
        bucket.count++;
        bucket.latSum += p.lat;
        bucket.lonSum += p.lon;
        if (p.signal !== null && p.signal !== undefined && p.signal > bucket.bestSignal) {
          bucket.bestSignal = p.signal;
        }
      });
      var markers = Object.keys(agg).map(function(key) {
        var b = agg[key];
        var lat = b.latSum / b.count;
        var lon = b.lonSum / b.count;
        // Log-scale the radius: 1 obs = 4px, 10 = 7px, 100 = 10px, 1000 = 13px.
        var radius = Math.max(4, 4 + Math.log10(b.count) * 3);
        return L.circleMarker([lat, lon], {
          radius: radius,
          color: colorFor(b.carrier),
          weight: 1,
          fillColor: colorFor(b.carrier),
          fillOpacity: 0.55,
        }).bindPopup(
          '<div class="cell-popup">' +
          '<b>' + (b.carrier || 'Unknown') + '</b>  (1 tower)<br/>' +
          'Observations: ' + b.count + '<br/>' +
          'Type: ' + (b.type || '-') + '<br/>' +
          (b.band ? 'Band: ' + b.band + '<br/>' : '') +
          'Best signal: ' + (b.bestSignal > -999 ? b.bestSignal + ' dBm' : '-') + '<br/>' +
          'Key: ' + (b.op || '-')  +
          '</div>'
        );
      });
      layer = L.layerGroup(markers).addTo(map);
    }

    function renderCurrent() {
      clearLayer();
      if (!currentPoints.length) return;
      if (currentMode === 'heatmap') renderHeatmap(currentPoints);
      else if (currentMode === 'towers') renderTowers(currentPoints);
      else renderDots(currentPoints);
    }

    function setPoints(points) {
      currentPoints = points || [];
      renderCurrent();
    }

    function setMode(mode) {
      currentMode = mode || 'dots';
      renderCurrent();
    }
  </script>
</body>
</html>
"""
