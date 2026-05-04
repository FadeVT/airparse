# AirParse

AirParse is an all-in-one database management and visualization tool for wireless captures, built around WiGLE and QGIS integration. It ingests data from a range of sources — Kismet `.kismet` databases, Hak5 Pager loot, raw PCAP / PCAPNG files, and WiGLE KML downloads — and gives you a unified, searchable database that you can upload back to WiGLE, merge into your QGIS-styled maps, or browse device-by-device. Hashcat handles WPA cracking when you need it; tshark assists with PCAP repair when installed.

## Features

### WiGLE database management (WiGLE tab)
- **Dashboard:** trophy bar, highlights, hero map, dedicated WiFi DB streaming for large datasets
- **Upload:** auto-discovers staged wiglecsv files, post-upload move to `uploaded/`, optional pre-upload blocklist filtering
- **Downloads:** WiGLE transactions browser with multi-select, right-click context menu (download, re-download, merge to QGIS KML), persistent KML scan manifest, empty-vs-error distinction in auto-ignore
- **WiGLE Database:** aggregate stats across every downloaded KML (count, size, WiFi/Cell/BT observations, date range), sortable per-KML table, right-click re-import / delete
- Full WiGLE REST API integration: BSSID lookup, user stats, transactions, KML download, network search

### QGIS integration (WiGLE → QGIS sub-page)
- Merge new WiGLE KMLs into per-year GeoPackages while preserving QGIS layer styles
- Insert-only and schema-stable: never alters the target layer's schema, never touches `layer_styles`. Style bindings survive every re-import
- Per-KML transaction wrapping (~100× faster than per-feature autocommit on large imports)
- Idempotent state tracking via internal `_airparse_merge_state` table — re-running Export skips everything already recorded
- Layer auto-detect plus configurable per-year layer name pattern
- Per-year KML export to `~/Downloads/{year}.kml` available as a secondary action for ad-hoc drag-into-fresh-project workflows

### Remote capture devices (Control tab)
- SSH pull from Kismet RPi5, Hak5 Pager, and Pwnagotchi
- Multi-source merge — combine captures across devices into one analysis session
- Per-device file management: scan, multi-select, delete remote captures via SFTP
- Local Storage browser: pulled files by source, WiGLE upload staging (staged vs uploaded), KML files
- Auto-discovery of `.wiglecsv` and `.csv` files alongside captures
- MAC Filters: blocklist (strip travel-kit devices from uploads), watchlist (highlight devices of interest), full MAC + OUI prefix matching, sync blocklist to remote Kismet `kismet_site.conf`

### Capture analysis (View tab)
- Loads Kismet `.kismet` databases and PCAP / PCAPNG files
- Auto-loads companion WiGLE CSV files for GPS data
- Interactive Leaflet.js map (CartoDB Dark Matter) with clustering, heatmaps, and GPS tracks
- Networks, devices, probe requests with filtering and search
- Deauth analysis with smart 3-category classification (band steering, client departures, attacks)
- Signal-weighted GPS centroid for PCAP-derived locations
- Handshake tracking with EAPOL completeness progress bars; right-click to crack with hashcat (mode 22000) when needed

### Cell tab
- Cellular networks parser for WiGLE's `Cellular Networks` KML layer, written to its own `cells.db` SQLite
- MCC/MNC → carrier resolver (T-Mobile incl. ex-Sprint, AT&T, Verizon, Dish, US Cellular, VTel Wireless, common Canadian carriers)
- 3GPP 36.101 EARFCN → LTE band and 38.101-1 NR-ARFCN → 5G band lookup (B1/B2/B4/B5/B7/B8/B12/B13/B14/B17/B25/B26/B29/B30/B41/B46/B48/B66/B71 + common NR FR1)
- Map view with carrier / radio / band multi-select filters and Dots / Heatmap / Towers render-mode toggle
- WiGLE `/api/v2/cell/search` per-region band enrichment with bulk-enrich-all workflow

### Search tab
- Local-first network search: checks pulled wiglecsv files and KML placemarks before hitting the WiGLE API
- WiGLE API fallback for misses; cached negative responses

### Export
- CSV, JSON, KML (simplekml), PDF (reportlab), Excel (openpyxl)
- **GeoPackage** via GDAL — see QGIS integration above

### Other
- Cohesive dark theme throughout
- KDE launcher entry installed at `~/.local/share/applications/airparse.desktop`

## Installation

### Quick Setup (Arch / CachyOS / Debian / Fedora)

```bash
git clone https://github.com/FadeVT/airparse.git ~/AirParse
cd ~/AirParse
./setup.sh
```

### Manual Setup

**Requirements:** Python 3.10+, PyQt6, PyQt6-WebEngine, GDAL 3.12+

```bash
git clone https://github.com/FadeVT/airparse.git ~/AirParse
cd ~/AirParse
pip install -r requirements.txt
python main.py
```

### Optional Dependencies

- `hashcat` + `hcxtools` — WPA password cracking
- `tshark` — PCAP repair
- `rockyou.txt` — auto-downloaded on first crack attempt
- WiGLE API credentials — stored at `~/.config/airparse/wigle_credentials.json` (chmod 600)

## Usage

```bash
python main.py                    # Launch empty
python main.py capture.kismet     # Open Kismet database
python main.py capture.pcap       # Open PCAP file
python main.py pineapple.zip      # Open zip archive (PCAP + WiGLE CSVs)
```

## Screenshots

### Landing dashboard
![Stats](screenshots/1.%20Stats.png)

### Interactive map (cluster view)
![Cluster map](screenshots/5.%20Map_Cluster.png)

### Heatmap
![Heatmap](screenshots/6.%20Map_Heat.png)

### Satellite overlay
![Satellite map](screenshots/7.%20Map_Sat.png)

### SSID hub
![SSID](screenshots/3.%20SSID.png)

### AP detail
![AP](screenshots/2.%20AP.png)

## Tech Stack

- **GUI:** PyQt6 + PyQt6-WebEngine
- **Maps:** Leaflet.js via QWebEngineView (CartoDB Dark Matter)
- **Geospatial:** GDAL / `osgeo.ogr` (GeoPackage merge into QGIS layers)
- **WiGLE:** REST API client (BSSID lookup, stats, upload, transactions, KML, network search, cell search)
- **SSH:** paramiko (remote pull from Kismet RPi5, Hak5 Pager, Pwnagotchi)
- **Data:** pandas, SQLite, dpkt
- **Charts:** pyqtgraph + matplotlib
- **Geocoding:** Nominatim (OpenStreetMap) — reverse for AP locations, forward for search
- **Cracking:** hashcat (mode 22000) + hcxpcapngtool
- **Export:** CSV, JSON, KML (simplekml), PDF (reportlab), Excel (openpyxl), GeoPackage (GDAL)

## License

MIT License
