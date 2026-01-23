# Kismet GUI Reader

A cross-platform desktop application for reading and visualizing Kismet wireless network capture data.

## Features

- **Database Browser**: Load and browse Kismet `.kismet` database files
- **Device Table**: View all captured devices with filtering, sorting, and search
- **Network Details**: Detailed view of access points with associated clients
- **Interactive Map**: Visualize device locations with:
  - OpenStreetMap, Satellite, and Terrain layers
  - Device clustering for large datasets
  - Heatmap overlay showing signal density
  - GPS track visualization
  - Click-to-view device details
- **Statistics Dashboard**: Charts and graphs showing:
  - Device type distribution
  - Signal strength analysis
  - Manufacturer breakdown
  - Encryption usage
- **Timeline View**: Temporal analysis of device activity
- **Export Options**: Export data to CSV, JSON, KML, and PDF formats

## Installation

### Windows Installer (Recommended)

Download the latest installer from the [Releases](https://github.com/FadeVT/kismet-gui-reader/releases) page:
- `KismetGUIReader_Setup_1.0.0.exe` - Windows installer with all dependencies included

### From Source

**Requirements:**
- Python 3.10+
- PyQt6
- PyQt6-WebEngine (for map visualization)

1. Clone the repository:
   ```bash
   git clone https://github.com/FadeVT/kismet-gui-reader.git
   cd kismet-gui-reader
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Run the application:
   ```bash
   python main.py
   ```

## Dependencies

- PyQt6 - GUI framework
- PyQt6-WebEngine - Map display
- pandas - Data processing
- pyqtgraph - High-performance plotting
- matplotlib - Additional charting
- reportlab - PDF generation
- simplekml - KML export
- openpyxl - Excel export support
- folium - Map export

## Usage

1. Launch the application
2. Use **File > Open Database** to load a Kismet `.kismet` file
3. Navigate between views using the sidebar:
   - **Map**: Geographic visualization of captured devices
   - **Devices**: Table view with filtering and search
   - **Statistics**: Charts and analysis
   - **Timeline**: Temporal device activity

## Screenshots

### Statistics Dashboard
![Statistics](screenshots/1.%20Stats.png)

### Access Points
![Access Points](screenshots/2.%20AP.png)

### SSID View
![SSID View](screenshots/3.%20SSID.png)

### Alerts
![Alerts](screenshots/4.%20Alerts.png)

### Map with Clustering
![Map Clustering](screenshots/5.%20Map_Cluster.png)

### Map with Heatmap
![Map Heatmap](screenshots/6.%20Map_Heat.png)

### Map with Satellite View
![Map Satellite](screenshots/7.%20Map_Sat.png)

## Building from Source

### Windows Executable

```bash
pip install pyinstaller
python -m PyInstaller kismet_gui_reader.spec --clean
```

The executable will be created in the `dist/` folder.

### Windows Installer

Requires [Inno Setup](https://jrsoftware.org/isinfo.php):

```bash
"C:\Program Files (x86)\Inno Setup 6\ISCC.exe" installer.iss
```

The installer will be created in the `installer_output/` folder.

## License

MIT License

## Acknowledgments

- [Kismet](https://www.kismetwireless.net/) - Wireless network detector and sniffer
- [Leaflet](https://leafletjs.com/) - Interactive map library
- [OpenStreetMap](https://www.openstreetmap.org/) - Map tiles
