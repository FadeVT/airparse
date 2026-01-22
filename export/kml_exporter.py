"""KML export functionality for Kismet data (Google Earth compatible)."""

from datetime import datetime
from pathlib import Path
from typing import List, Optional
import pandas as pd

try:
    import simplekml
    HAS_SIMPLEKML = True
except ImportError:
    HAS_SIMPLEKML = False


class KMLExporter:
    """Export Kismet data to KML format for Google Earth."""

    # Device type icon URLs (using Google Earth default icons)
    ICON_URLS = {
        'ap': 'http://maps.google.com/mapfiles/kml/shapes/wifi.png',
        'client': 'http://maps.google.com/mapfiles/kml/shapes/phone.png',
        'bluetooth': 'http://maps.google.com/mapfiles/kml/shapes/electronics.png',
        'default': 'http://maps.google.com/mapfiles/kml/shapes/placemark_circle.png'
    }

    # Signal strength to color mapping (AABBGGRR format for KML)
    SIGNAL_COLORS = {
        'excellent': 'ff00ff00',  # Green
        'good': 'ff00ffff',       # Yellow
        'fair': 'ff00a5ff',       # Orange
        'weak': 'ff0000ff',       # Red
        'poor': 'ff000080'        # Dark red
    }

    def __init__(self):
        if not HAS_SIMPLEKML:
            raise ImportError("simplekml is required for KML export. Install with: pip install simplekml")

        self.kml = None
        self.include_track = True
        self.include_devices = True

    def _signal_to_color(self, signal: int) -> str:
        """Convert signal strength to KML color."""
        if signal is None:
            return self.SIGNAL_COLORS['fair']
        if signal >= -50:
            return self.SIGNAL_COLORS['excellent']
        elif signal >= -60:
            return self.SIGNAL_COLORS['good']
        elif signal >= -70:
            return self.SIGNAL_COLORS['fair']
        elif signal >= -80:
            return self.SIGNAL_COLORS['weak']
        else:
            return self.SIGNAL_COLORS['poor']

    def _get_device_icon(self, device_type: str) -> str:
        """Get icon URL for device type."""
        device_type_lower = str(device_type).lower()
        if 'ap' in device_type_lower or '802.11' in device_type_lower:
            return self.ICON_URLS['ap']
        elif 'client' in device_type_lower:
            return self.ICON_URLS['client']
        elif 'bluetooth' in device_type_lower or 'btle' in device_type_lower:
            return self.ICON_URLS['bluetooth']
        return self.ICON_URLS['default']

    def export_devices(self, df: pd.DataFrame, output_path: str,
                       include_details: bool = True) -> bool:
        """
        Export devices to KML file.

        Args:
            df: Device DataFrame with GPS coordinates
            output_path: Path to output KML file
            include_details: Include device details in description

        Returns:
            True if successful
        """
        if not HAS_SIMPLEKML:
            return False

        try:
            self.kml = simplekml.Kml(name="Kismet Devices")

            # Create folders for different device types
            ap_folder = self.kml.newfolder(name="Access Points")
            client_folder = self.kml.newfolder(name="Clients")
            bt_folder = self.kml.newfolder(name="Bluetooth")
            other_folder = self.kml.newfolder(name="Other Devices")

            # Determine lat/lon columns
            lat_col = 'min_lat' if 'min_lat' in df.columns else 'lat'
            lon_col = 'min_lon' if 'min_lon' in df.columns else 'lon'

            if lat_col not in df.columns or lon_col not in df.columns:
                return False

            for _, row in df.iterrows():
                lat = row.get(lat_col, 0)
                lon = row.get(lon_col, 0)

                # Skip invalid coordinates
                if lat == 0 or lon == 0 or pd.isna(lat) or pd.isna(lon):
                    continue

                # Determine device type and folder
                phy_type = str(row.get('phyname', '')).lower()
                device_type = str(row.get('type', '')).lower()

                if '802.11' in phy_type:
                    if 'ap' in device_type or row.get('ssid'):
                        folder = ap_folder
                    else:
                        folder = client_folder
                elif 'bluetooth' in phy_type or 'btle' in phy_type:
                    folder = bt_folder
                else:
                    folder = other_folder

                # Create placemark
                mac = row.get('devmac', row.get('client_mac', 'Unknown'))
                name = row.get('ssid') or row.get('name') or mac

                pnt = folder.newpoint(name=str(name))
                pnt.coords = [(float(lon), float(lat))]

                # Set icon style
                signal = row.get('strongest_signal')
                pnt.style.iconstyle.icon.href = self._get_device_icon(phy_type)
                pnt.style.iconstyle.color = self._signal_to_color(signal)
                pnt.style.iconstyle.scale = 1.0

                # Add description
                if include_details:
                    desc = f"""
<![CDATA[
<b>MAC Address:</b> {mac}<br/>
<b>Type:</b> {row.get('phyname', 'Unknown')}<br/>
<b>Name:</b> {row.get('name', '-')}<br/>
<b>SSID:</b> {row.get('ssid', '-')}<br/>
<b>Manufacturer:</b> {row.get('manufacturer', '-')}<br/>
<b>Signal:</b> {signal if signal else '-'} dBm<br/>
<b>Channel:</b> {row.get('channel', '-')}<br/>
<b>First Seen:</b> {row.get('first_time', '-')}<br/>
<b>Last Seen:</b> {row.get('last_time', '-')}<br/>
<b>Location:</b> {lat:.6f}, {lon:.6f}
]]>
"""
                    pnt.description = desc

            # Save KML
            self.kml.save(output_path)
            return True

        except Exception as e:
            print(f"KML export error: {e}")
            return False

    def export_gps_track(self, df: pd.DataFrame, output_path: str,
                         track_name: str = "GPS Track") -> bool:
        """
        Export GPS track to KML file.

        Args:
            df: GPS DataFrame with lat, lon, timestamp
            output_path: Path to output KML file
            track_name: Name for the track

        Returns:
            True if successful
        """
        if not HAS_SIMPLEKML:
            return False

        try:
            self.kml = simplekml.Kml(name="Kismet GPS Track")

            # Filter valid coordinates
            valid_df = df[(df['lat'] != 0) & (df['lon'] != 0)].copy()

            if valid_df.empty:
                return False

            # Create linestring for track
            coords = [(row['lon'], row['lat']) for _, row in valid_df.iterrows()]

            if len(coords) < 2:
                return False

            track = self.kml.newlinestring(name=track_name)
            track.coords = coords
            track.style.linestyle.color = simplekml.Color.red
            track.style.linestyle.width = 3

            # Add start and end markers
            start_pnt = self.kml.newpoint(name="Track Start")
            start_pnt.coords = [coords[0]]
            start_pnt.style.iconstyle.color = simplekml.Color.green
            start_pnt.style.iconstyle.scale = 1.2

            end_pnt = self.kml.newpoint(name="Track End")
            end_pnt.coords = [coords[-1]]
            end_pnt.style.iconstyle.color = simplekml.Color.red
            end_pnt.style.iconstyle.scale = 1.2

            # Add track info
            if 'timestamp' in valid_df.columns:
                start_time = valid_df['timestamp'].iloc[0]
                end_time = valid_df['timestamp'].iloc[-1]
                track.description = f"Start: {start_time}\nEnd: {end_time}\nPoints: {len(coords)}"

            self.kml.save(output_path)
            return True

        except Exception as e:
            print(f"KML export error: {e}")
            return False

    def export_combined(self, devices_df: pd.DataFrame, gps_df: pd.DataFrame,
                        output_path: str) -> bool:
        """
        Export devices and GPS track to a single KML file.

        Args:
            devices_df: Device DataFrame
            gps_df: GPS track DataFrame
            output_path: Path to output KML file

        Returns:
            True if successful
        """
        if not HAS_SIMPLEKML:
            return False

        try:
            self.kml = simplekml.Kml(name="Kismet Capture Data")

            # Add devices
            if devices_df is not None and not devices_df.empty:
                devices_folder = self.kml.newfolder(name="Devices")

                lat_col = 'min_lat' if 'min_lat' in devices_df.columns else 'lat'
                lon_col = 'min_lon' if 'min_lon' in devices_df.columns else 'lon'

                for _, row in devices_df.iterrows():
                    lat = row.get(lat_col, 0)
                    lon = row.get(lon_col, 0)

                    if lat == 0 or lon == 0 or pd.isna(lat) or pd.isna(lon):
                        continue

                    mac = row.get('devmac', row.get('client_mac', 'Unknown'))
                    name = row.get('ssid') or row.get('name') or mac

                    pnt = devices_folder.newpoint(name=str(name))
                    pnt.coords = [(float(lon), float(lat))]

                    signal = row.get('strongest_signal')
                    pnt.style.iconstyle.color = self._signal_to_color(signal)

            # Add GPS track
            if gps_df is not None and not gps_df.empty:
                track_folder = self.kml.newfolder(name="GPS Track")

                valid_df = gps_df[(gps_df['lat'] != 0) & (gps_df['lon'] != 0)]

                if not valid_df.empty:
                    coords = [(row['lon'], row['lat']) for _, row in valid_df.iterrows()]

                    if len(coords) >= 2:
                        track = track_folder.newlinestring(name="Capture Track")
                        track.coords = coords
                        track.style.linestyle.color = simplekml.Color.blue
                        track.style.linestyle.width = 2

            self.kml.save(output_path)
            return True

        except Exception as e:
            print(f"KML export error: {e}")
            return False


def export_to_kml(devices_df: pd.DataFrame, output_path: str,
                  gps_df: pd.DataFrame = None) -> bool:
    """
    Convenience function to export to KML.

    Args:
        devices_df: Device DataFrame
        output_path: Path to output file
        gps_df: Optional GPS track DataFrame

    Returns:
        True if successful
    """
    if not HAS_SIMPLEKML:
        print("simplekml is required for KML export")
        return False

    exporter = KMLExporter()

    if gps_df is not None:
        return exporter.export_combined(devices_df, gps_df, output_path)
    else:
        return exporter.export_devices(devices_df, output_path)
