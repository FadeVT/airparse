"""Map view widget for GPS visualization using Leaflet.js."""

import json
from typing import List, Optional
import pandas as pd

from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QToolBar, QComboBox,
    QCheckBox, QLabel, QPushButton, QSlider, QSpinBox
)
from PyQt6.QtCore import Qt, pyqtSignal, QTimer
from PyQt6.QtGui import QAction

try:
    from PyQt6.QtWebEngineWidgets import QWebEngineView
    HAS_WEBENGINE = True
except ImportError:
    HAS_WEBENGINE = False

from utils.geo_utils import (
    get_center_point, get_bounding_box, calculate_zoom_level,
    signal_to_color, device_type_to_color
)


class MapView(QWidget):
    """GPS-based map visualization widget."""

    # Signals
    deviceClicked = pyqtSignal(str)  # Emits device MAC when marker clicked

    def __init__(self, parent=None):
        super().__init__(parent)
        self._devices = []
        self._devices_by_mac = {}  # Index devices by MAC for quick lookup
        self._gps_track = []
        self._show_heatmap = False
        self._show_clusters = True
        self._color_by = 'type'  # 'type' or 'signal'
        self._map_initialized = False  # Track if map HTML has been set
        self._setup_ui()

    def _setup_ui(self):
        """Set up the UI components."""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        # Toolbar
        toolbar = self._create_toolbar()
        layout.addWidget(toolbar)

        # Map view
        if HAS_WEBENGINE:
            try:
                self.web_view = QWebEngineView()
                self.web_view.setMinimumSize(400, 300)
                self.web_view.setSizePolicy(
                    self.web_view.sizePolicy().horizontalPolicy(),
                    self.web_view.sizePolicy().verticalPolicy()
                )
                layout.addWidget(self.web_view, 1)

                # Track if map is ready
                self._map_ready = False
                self._pending_devices = None
                self._pending_track = None
                self._pending_center = None

                # Set up timer to poll for device clicks from JavaScript
                self._click_poll_timer = QTimer(self)
                self._click_poll_timer.timeout.connect(self._poll_for_device_click)
                self._click_poll_timer.start(200)

                # Defer map initialization until widget is shown
                self._first_show = True
            except Exception:
                self.web_view = None
                placeholder = QLabel(
                    "Map view failed to initialize.\n\n"
                    "WebEngine may not be compatible with your display server.\n"
                    "Try setting: QTWEBENGINE_CHROMIUM_FLAGS=--disable-gpu"
                )
                placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
                placeholder.setStyleSheet("background-color: #2b2b2b; color: #ccc; padding: 50px;")
                layout.addWidget(placeholder)
        else:
            placeholder = QLabel(
                "PyQt6-WebEngine is required for map view.\n\n"
                "Install with: pip install PyQt6-WebEngine"
            )
            placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
            placeholder.setStyleSheet("background-color: #2b2b2b; color: #ccc; padding: 50px;")
            layout.addWidget(placeholder)
            self.web_view = None

    def _create_toolbar(self) -> QToolBar:
        """Create the map toolbar."""
        toolbar = QToolBar()

        # Layer selector
        toolbar.addWidget(QLabel("Layer: "))
        self.layer_combo = QComboBox()
        self.layer_combo.addItems(["Dark Matter", "Dark (No Labels)", "Satellite", "Terrain"])
        self.layer_combo.currentTextChanged.connect(self._on_layer_changed)
        toolbar.addWidget(self.layer_combo)

        toolbar.addSeparator()

        # Color by selector
        toolbar.addWidget(QLabel("Color by: "))
        self.color_combo = QComboBox()
        self.color_combo.addItems(["Device Type", "Signal Strength"])
        self.color_combo.currentTextChanged.connect(self._on_color_changed)
        toolbar.addWidget(self.color_combo)

        toolbar.addSeparator()

        # Clustering checkbox
        self.cluster_cb = QCheckBox("Clustering")
        self.cluster_cb.setChecked(True)
        self.cluster_cb.toggled.connect(self._on_cluster_toggled)
        toolbar.addWidget(self.cluster_cb)

        # Heatmap checkbox
        self.heatmap_cb = QCheckBox("Heatmap")
        self.heatmap_cb.setChecked(False)
        self.heatmap_cb.toggled.connect(self._on_heatmap_toggled)
        toolbar.addWidget(self.heatmap_cb)

        # GPS track checkbox
        self.track_cb = QCheckBox("GPS Track")
        self.track_cb.setChecked(False)
        self.track_cb.toggled.connect(self._on_track_toggled)
        toolbar.addWidget(self.track_cb)

        toolbar.addSeparator()

        # Fit to data button
        fit_btn = QPushButton("Fit to Data")
        fit_btn.clicked.connect(self._fit_to_data)
        toolbar.addWidget(fit_btn)

        # Refresh button
        refresh_btn = QPushButton("Refresh")
        refresh_btn.clicked.connect(self._refresh_map)
        toolbar.addWidget(refresh_btn)

        return toolbar

    def _init_map(self):
        """Initialize the Leaflet map."""
        if not self.web_view:
            return

        # Only connect signal once
        if not self._map_initialized:
            self.web_view.loadFinished.connect(self._on_map_loaded)
            self._map_initialized = True

        html = self._generate_map_html()
        self.web_view.setHtml(html)

    def showEvent(self, event):
        """Handle widget becoming visible - initialize or resize map."""
        super().showEvent(event)
        if not self.web_view:
            return

        # Initialize map on first show (when widget has proper dimensions)
        if self._first_show:
            self._first_show = False
            self._init_map()
        elif self._map_ready:
            # When shown again, Leaflet needs to recalculate its size
            QTimer.singleShot(100, lambda: self.web_view.page().runJavaScript("if(typeof map !== 'undefined') map.invalidateSize();"))

    def _on_map_loaded(self, ok: bool):
        """Handle map page load completion."""
        if not ok:
            # Page failed to load — CDN resources might be unavailable
            self._map_ready = False
            return

        self._map_ready = True

        try:
            # Apply pending center first
            if self._pending_center is not None:
                lat, lon, zoom = self._pending_center
                self.web_view.page().runJavaScript(f"setView({lat}, {lon}, {zoom});")
                self._pending_center = None

            # Send any pending data
            if self._pending_devices is not None:
                self._send_devices_to_map(self._pending_devices)
                self._pending_devices = None
            if self._pending_track is not None:
                self._send_track_to_map(self._pending_track)
                self._pending_track = None
            # Fit to data after loading
            if self._devices or self._gps_track:
                self._fit_to_data()
        except Exception:
            pass  # Swallow JS errors to prevent crash

    def _generate_map_html(self, center_lat: float = 39.8283, center_lon: float = -98.5795,
                           zoom: int = 4) -> str:
        """Generate the HTML for the Leaflet map."""
        return f'''<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.4.1/dist/MarkerCluster.css" />
    <link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.4.1/dist/MarkerCluster.Default.css" />
    <style>
        * {{ margin: 0; padding: 0; }}
        html, body {{ width: 100vw; height: 100vh; overflow: hidden; }}
        #map {{ width: 100vw; height: 100vh; background: #1a1a2e; }}

        /* --- Dark theme for Leaflet controls --- */
        .leaflet-control-zoom a,
        .leaflet-control-layers-toggle {{
            background-color: #2b2b2b !important;
            color: #e0e0e0 !important;
            border-color: #444 !important;
        }}
        .leaflet-control-zoom a:hover,
        .leaflet-control-layers-toggle:hover {{
            background-color: #3c3f41 !important;
        }}
        .leaflet-control-layers {{
            background-color: #2b2b2b !important;
            color: #e0e0e0 !important;
            border-color: #444 !important;
        }}
        .leaflet-control-layers label {{
            color: #e0e0e0 !important;
        }}
        .leaflet-control-layers-separator {{
            border-top-color: #444 !important;
        }}
        .leaflet-control-attribution {{
            background-color: rgba(30, 30, 30, 0.7) !important;
            color: #999 !important;
        }}
        .leaflet-control-attribution a {{
            color: #aaa !important;
        }}

        /* --- Dark popups --- */
        .leaflet-popup-content-wrapper {{
            background-color: #2b2b2b !important;
            color: #e0e0e0 !important;
            border-radius: 6px !important;
        }}
        .leaflet-popup-tip {{
            background-color: #2b2b2b !important;
        }}
        .leaflet-popup-close-button {{
            color: #aaa !important;
        }}
        .leaflet-popup-close-button:hover {{
            color: #fff !important;
        }}

        /* --- Device popup content --- */
        .device-popup {{ min-width: 200px; color: #e0e0e0; }}
        .device-popup h4 {{ margin: 0 0 8px 0; font-family: monospace; color: #e0e0e0; }}
        .device-popup table {{ width: 100%; font-size: 12px; }}
        .device-popup td {{ padding: 2px 4px; color: #ccc; }}
        .device-popup td b {{ color: #e0e0e0; }}
        .device-popup .view-details {{
            display: block; margin-top: 10px; padding: 5px 10px;
            background: #3498db; color: white; text-align: center;
            text-decoration: none; border-radius: 4px; cursor: pointer;
        }}
        .device-popup .view-details:hover {{ background: #2980b9; }}

        /* --- Cluster markers --- */
        .marker-cluster div {{
            background-color: rgba(52, 152, 219, 0.6) !important;
            color: #fff !important;
        }}
        .marker-cluster {{
            background-color: rgba(52, 152, 219, 0.3) !important;
        }}
    </style>
</head>
<body>
    <div id="map"></div>
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <script src="https://unpkg.com/leaflet.markercluster@1.4.1/dist/leaflet.markercluster.js"></script>
    <script src="https://unpkg.com/leaflet.heat@0.2.0/dist/leaflet-heat.js"></script>
    <script>
    // Global map reference for external calls
    var map = L.map('map').setView([{center_lat}, {center_lon}], {zoom});

    // Tile layers
    var darkMatterLayer = L.tileLayer('https://{{s}}.basemaps.cartocdn.com/dark_all/{{z}}/{{x}}/{{y}}{{r}}.png', {{
        attribution: '© OpenStreetMap contributors © CARTO',
        subdomains: 'abcd',
        maxZoom: 20
    }}).addTo(map);

    var darkNoLabelsLayer = L.tileLayer('https://{{s}}.basemaps.cartocdn.com/dark_nolabels/{{z}}/{{x}}/{{y}}{{r}}.png', {{
        attribution: '© OpenStreetMap contributors © CARTO',
        subdomains: 'abcd',
        maxZoom: 20
    }});

    var satelliteLayer = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{{z}}/{{y}}/{{x}}', {{
        attribution: '© Esri',
        maxZoom: 19
    }});

    var terrainLayer = L.tileLayer('https://{{s}}.tile.opentopomap.org/{{z}}/{{x}}/{{y}}.png', {{
        attribution: '© OpenTopoMap',
        maxZoom: 17
    }});

    var currentLayer = darkMatterLayer;
    var deviceData = [];
    var useClustering = true;
    var clusterGroup = null;
    var regularMarkers = L.layerGroup();
    var trackLayer = L.layerGroup().addTo(map);
    var heatLayer = null;
    window.pendingDeviceClick = null;

    // Create cluster group if available
    if (typeof L.markerClusterGroup === 'function') {{
        clusterGroup = L.markerClusterGroup({{
            disableClusteringAtZoom: 18,
            showCoverageOnHover: false,
            maxClusterRadius: 50
        }});
    }}

    function openDeviceDetails(mac) {{ window.pendingDeviceClick = mac; }}
    function getPendingClick() {{ var m = window.pendingDeviceClick; window.pendingDeviceClick = null; return m; }}

    function setLayer(name) {{
        map.removeLayer(currentLayer);
        if (name === 'Satellite') currentLayer = satelliteLayer;
        else if (name === 'Terrain') currentLayer = terrainLayer;
        else if (name === 'Dark (No Labels)') currentLayer = darkNoLabelsLayer;
        else currentLayer = darkMatterLayer;
        currentLayer.addTo(map);
    }}

    function createMarker(d) {{
        var icon = L.divIcon({{
            className: 'device-marker',
            html: '<div style="background:' + (d.color || '#3498db') + ';width:12px;height:12px;border-radius:50%;border:2px solid white;box-shadow:0 0 3px rgba(0,0,0,0.4);"></div>',
            iconSize: [16, 16],
            iconAnchor: [8, 8]
        }});
        var popup = '<div class="device-popup"><h4>' + (d.mac || 'Unknown') + '</h4>' +
            '<table><tr><td><b>Type:</b></td><td>' + (d.type || '-') + '</td></tr>' +
            '<tr><td><b>Name:</b></td><td>' + (d.name || '-') + '</td></tr>' +
            '<tr><td><b>SSID:</b></td><td>' + (d.ssid || '-') + '</td></tr>' +
            '<tr><td><b>Signal:</b></td><td>' + (d.signal || '-') + ' dBm</td></tr></table>' +
            '<a class="view-details" onclick="openDeviceDetails(\\'' + d.mac + '\\')">View Details</a></div>';
        return L.marker([d.lat, d.lon], {{icon: icon}}).bindPopup(popup);
    }}

    function refreshMarkers() {{
        if (clusterGroup) clusterGroup.clearLayers();
        regularMarkers.clearLayers();

        deviceData.forEach(function(d) {{
            var marker = createMarker(d);
            if (useClustering && clusterGroup) {{
                clusterGroup.addLayer(marker);
            }} else {{
                regularMarkers.addLayer(marker);
            }}
        }});

        if (useClustering && clusterGroup) {{
            if (map.hasLayer(regularMarkers)) map.removeLayer(regularMarkers);
            if (!map.hasLayer(clusterGroup)) map.addLayer(clusterGroup);
        }} else {{
            if (clusterGroup && map.hasLayer(clusterGroup)) map.removeLayer(clusterGroup);
            if (!map.hasLayer(regularMarkers)) map.addLayer(regularMarkers);
        }}
    }}

    function setDevices(devices) {{
        deviceData = devices;
        refreshMarkers();
    }}

    function setClustering(enabled) {{
        useClustering = enabled;
        refreshMarkers();
    }}

    function setHeatmap(enabled) {{
        if (enabled && deviceData.length > 0 && typeof L.heatLayer === 'function') {{
            var heatData = deviceData.map(function(d) {{
                return [d.lat, d.lon, Math.abs(d.signal || -80) / 100];
            }});
            if (heatLayer) map.removeLayer(heatLayer);
            heatLayer = L.heatLayer(heatData, {{radius: 25, blur: 15, maxZoom: 17}}).addTo(map);
        }} else if (heatLayer) {{
            map.removeLayer(heatLayer);
            heatLayer = null;
        }}
    }}

    function setGpsTrack(track) {{
        trackLayer.clearLayers();
        if (track && track.length > 1) {{
            var latlngs = track.map(function(p) {{ return [p.lat, p.lon]; }});
            L.polyline(latlngs, {{color: '#e74c3c', weight: 3}}).addTo(trackLayer);
        }}
    }}

    function fitBounds(minLat, minLon, maxLat, maxLon) {{
        map.fitBounds([[minLat, minLon], [maxLat, maxLon]], {{padding: [50, 50]}});
    }}

    function setView(lat, lon, z) {{ map.setView([lat, lon], z); }}
    function setTrackVisible(visible) {{ if(visible) map.addLayer(trackLayer); else map.removeLayer(trackLayer); }}

    // Fix map size on load and resize
    window.addEventListener('resize', function() {{ map.invalidateSize(); }});
    new ResizeObserver(function() {{ map.invalidateSize(); }}).observe(document.getElementById('map'));
    setTimeout(function() {{ map.invalidateSize(); }}, 100);
    setTimeout(function() {{ map.invalidateSize(); }}, 300);
    setTimeout(function() {{ map.invalidateSize(); }}, 1000);
    </script>
</body>
</html>'''

    def plot_devices(self, devices_df: pd.DataFrame, color_by: str = 'type'):
        """
        Plot devices on the map.

        Args:
            devices_df: DataFrame with device data including lat/lon columns
            color_by: 'type' for device type colors, 'signal' for signal strength colors
        """
        if not self.web_view or devices_df is None or devices_df.empty:
            return

        self._devices = []
        self._devices_by_mac = {}  # Store full device data by MAC for lookup

        # Filter for valid GPS coordinates
        lat_col = 'min_lat' if 'min_lat' in devices_df.columns else 'lat'
        lon_col = 'min_lon' if 'min_lon' in devices_df.columns else 'lon'

        if lat_col not in devices_df.columns or lon_col not in devices_df.columns:
            return

        for _, row in devices_df.iterrows():
            lat = row.get(lat_col, 0)
            lon = row.get(lon_col, 0)

            if lat == 0 or lon == 0 or pd.isna(lat) or pd.isna(lon):
                continue

            signal = row.get('strongest_signal', -80)
            device_type = row.get('phyname', row.get('type', 'Unknown'))

            if color_by == 'signal':
                color = signal_to_color(int(signal) if signal else -80)
            else:
                color = device_type_to_color(device_type)

            mac = str(row.get('devmac', row.get('client_mac', '')))
            device = {
                'lat': float(lat),
                'lon': float(lon),
                'mac': mac,
                'type': str(device_type),
                'name': str(row.get('name', '')),
                'ssid': str(row.get('ssid', row.get('name', ''))),
                'signal': int(signal) if signal and not pd.isna(signal) else None,
                'manufacturer': str(row.get('manufacturer', '')),
                'color': color
            }
            self._devices.append(device)

            # Store full row data for lookup when clicked
            self._devices_by_mac[mac] = row.to_dict()

        # Send devices to map (or queue if not ready)
        if self._map_ready:
            self._send_devices_to_map(self._devices)
        else:
            self._pending_devices = self._devices

    def _send_devices_to_map(self, devices: list):
        """Send devices data to the JavaScript map."""
        if self.web_view and devices:
            devices_json = json.dumps(devices)
            self.web_view.page().runJavaScript(f"setDevices({devices_json});")

    def plot_gps_track(self, track_df: pd.DataFrame):
        """
        Plot GPS track on the map.

        Args:
            track_df: DataFrame with GPS track data (lat, lon, timestamp)
        """
        if not self.web_view or track_df is None or track_df.empty:
            return

        self._gps_track = []

        for _, row in track_df.iterrows():
            lat = row.get('lat', 0)
            lon = row.get('lon', 0)

            if lat == 0 or lon == 0 or pd.isna(lat) or pd.isna(lon):
                continue

            point = {
                'lat': float(lat),
                'lon': float(lon),
                'timestamp': str(row.get('timestamp', ''))
            }
            self._gps_track.append(point)

        # Send track to map (or queue if not ready)
        if self._map_ready:
            self._send_track_to_map(self._gps_track)
        else:
            self._pending_track = self._gps_track

    def _send_track_to_map(self, track: list):
        """Send GPS track data to the JavaScript map."""
        if self.web_view and track:
            track_json = json.dumps(track)
            self.web_view.page().runJavaScript(f"setGpsTrack({track_json});")

    def _fit_to_data(self):
        """Fit map view to show all data."""
        if not self.web_view:
            return

        points = [(d['lat'], d['lon']) for d in self._devices]
        points.extend([(p['lat'], p['lon']) for p in self._gps_track])

        if not points:
            return

        bounds = get_bounding_box(points)
        self.web_view.page().runJavaScript(
            f"fitBounds({bounds['min_lat']}, {bounds['min_lon']}, "
            f"{bounds['max_lat']}, {bounds['max_lon']});"
        )

    def _refresh_map(self):
        """Refresh the map display."""
        if not self.web_view:
            return

        devices_json = json.dumps(self._devices)
        self.web_view.page().runJavaScript(f"setDevices({devices_json});")

        track_json = json.dumps(self._gps_track)
        self.web_view.page().runJavaScript(f"setGpsTrack({track_json});")

    def _on_layer_changed(self, layer_name: str):
        """Handle layer selection change."""
        if self.web_view:
            self.web_view.page().runJavaScript(f"setLayer('{layer_name}');")

    def _on_color_changed(self, color_option: str):
        """Handle color-by selection change."""
        self._color_by = 'signal' if 'Signal' in color_option else 'type'
        # Re-plot with new colors
        if self._devices:
            for device in self._devices:
                if self._color_by == 'signal':
                    device['color'] = signal_to_color(device.get('signal', -80) or -80)
                else:
                    device['color'] = device_type_to_color(device.get('type', 'Unknown'))

            devices_json = json.dumps(self._devices)
            self.web_view.page().runJavaScript(f"setDevices({devices_json});")

    def _on_cluster_toggled(self, enabled: bool):
        """Handle clustering toggle."""
        self._show_clusters = enabled
        if self.web_view:
            self.web_view.page().runJavaScript(f"setClustering({'true' if enabled else 'false'});")

    def _on_heatmap_toggled(self, enabled: bool):
        """Handle heatmap toggle."""
        self._show_heatmap = enabled
        if self.web_view:
            self.web_view.page().runJavaScript(f"setHeatmap({'true' if enabled else 'false'});")

    def _on_track_toggled(self, enabled: bool):
        """Handle GPS track visibility toggle."""
        if self.web_view:
            self.web_view.page().runJavaScript(f"setTrackVisible({'true' if enabled else 'false'});")

    def set_center(self, lat: float, lon: float, zoom: int = 15):
        """Set the map center and zoom level."""
        if self.web_view:
            if self._map_ready:
                self.web_view.page().runJavaScript(f"setView({lat}, {lon}, {zoom});")
            else:
                # Queue for when map is ready
                self._pending_center = (lat, lon, zoom)

    def clear(self):
        """Clear all data from the map."""
        self._devices = []
        self._devices_by_mac = {}
        self._gps_track = []
        if self.web_view:
            self.web_view.page().runJavaScript("setDevices([]);")
            self.web_view.page().runJavaScript("setGpsTrack([]);")

    def _poll_for_device_click(self):
        """Poll JavaScript for pending device clicks."""
        if not self.web_view or not self._map_ready:
            return

        def handle_result(mac):
            if mac:
                self.deviceClicked.emit(mac)

        self.web_view.page().runJavaScript("getPendingClick();", handle_result)

    def get_device_data(self, mac: str) -> dict:
        """Get the full device data for a MAC address."""
        return self._devices_by_mac.get(mac, {})
