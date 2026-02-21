# AirParse

A PyQt6 desktop application for analyzing wireless capture data. Supports Kismet `.kismet` databases and `.pcap`/`.pcapng` files (e.g., WiFi Pineapple captures) with interactive maps, device analysis, hashcat WPA cracking, and multi-format export.

## Features

- **Dual Format Support**: Load Kismet `.kismet` SQLite databases and PCAP/PCAPNG captures
- **WiGLE GPS Integration**: Auto-loads companion WiGLE CSV files for wardriving GPS data
- **Interactive Map**: CartoDB Dark Matter tiles with clustering, heatmaps, GPS tracks via Leaflet.js
- **Hashcat WPA Cracking**: Right-click a captured handshake to crack with hashcat (GPU-accelerated)
- **Device Analysis**: Browse APs, clients, probe requests with filtering and search
- **Network Investigation**: SSID hub with reverse geocoding, AP breakdown, and handshake status
- **Deauth Analysis**: Smart 3-category classification (band steering, client departures, attacks)
- **Handshake Tracking**: Progress bars showing EAPOL capture completeness (25-100%)
- **Export**: CSV, JSON, KML, PDF, Excel
- **Dark Theme**: Cohesive dark UI throughout

## Installation

### Quick Setup (Arch/CachyOS/Debian/Fedora)

```bash
git clone https://github.com/FadeVT/airparse.git
cd airparse
./setup.sh
```

### Manual Setup

**Requirements:** Python 3.10+, PyQt6, PyQt6-WebEngine

```bash
git clone https://github.com/FadeVT/airparse.git
cd airparse
pip install -r requirements.txt
python main.py
```

### Optional Dependencies

- `hashcat` + `hcxtools` — WPA password cracking
- `tshark` — PCAP repair
- `rockyou.txt` — auto-downloaded on first crack attempt

## Usage

```bash
python main.py                    # Launch empty
python main.py capture.kismet     # Open Kismet database
python main.py capture.pcap       # Open PCAP file
python main.py pineapple.zip      # Open zip archive (PCAP + WiGLE CSVs)
```

## Screenshots

### Handshake Cracking
![Cracking](screenshots/crack_progress.png)

### Network Analysis
![Networks](screenshots/networks.png)

### Interactive Map
![Map](screenshots/map.png)

## Tech Stack

- **GUI:** PyQt6 + PyQt6-WebEngine
- **Maps:** Leaflet.js via QWebEngineView (CartoDB Dark Matter)
- **Charts:** pyqtgraph
- **Data:** pandas, SQLite, dpkt
- **Cracking:** hashcat + hcxpcapngtool
- **Geocoding:** Nominatim (OpenStreetMap)
- **Export:** CSV, JSON, KML (simplekml), PDF (reportlab), Excel (openpyxl)

## License

MIT License
