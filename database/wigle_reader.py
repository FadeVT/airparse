"""WiGLE CSV file reader for wireless survey data."""

import csv
import gzip
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from utils.oui_lookup import lookup_manufacturer, is_randomized_mac


def _parse_auth_mode(auth_str: str) -> str:
    """Convert WiGLE AuthMode string to a simple encryption label."""
    if not auth_str or auth_str == '[]':
        return 'Open'
    auth_upper = auth_str.upper()
    if 'WPA3' in auth_upper or 'SAE' in auth_upper:
        return 'WPA3'
    if 'WPA2' in auth_upper:
        return 'WPA2'
    if 'WPA' in auth_upper:
        return 'WPA'
    if 'WEP' in auth_upper:
        return 'WEP'
    if 'OPEN' in auth_upper:
        return 'Open'
    return 'Unknown'


def _parse_timestamp(ts_str: str) -> Optional[datetime]:
    """Parse WiGLE timestamp string."""
    if not ts_str:
        return None
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S'):
        try:
            return datetime.strptime(ts_str, fmt)
        except ValueError:
            continue
    return None


class WigleCsvReader:
    """Reader for WiGLE CSV wireless survey exports."""

    def __init__(self):
        self._file_path: Optional[Path] = None
        self._is_loaded: bool = False
        self._devices: dict[str, dict] = {}  # MAC -> aggregated record
        self._raw_rows: int = 0

    def _open_file(self, path: Path):
        """Open a plain or gzipped file, returning a text-mode file object."""
        if path.name.lower().endswith('.csv.gz'):
            return gzip.open(path, 'rt', encoding='utf-8', errors='replace')
        return open(path, 'r', encoding='utf-8', errors='replace')

    def open_database(self, path: str) -> bool:
        """Validate and load a WiGLE CSV file (plain or gzipped)."""
        self._file_path = Path(path)

        if not self._file_path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        with self._open_file(self._file_path) as f:
            first_line = f.readline()
            if not first_line.startswith('WigleWifi'):
                raise ValueError("Not a valid WiGLE CSV file (missing WigleWifi header)")

        self._parse()
        return True

    def _parse(self):
        """Parse the WiGLE CSV and aggregate by MAC."""
        self._devices.clear()
        self._raw_rows = 0

        with self._open_file(self._file_path) as f:
            # Skip WiGLE metadata header line
            next(f)
            reader = csv.DictReader(f)

            for row in reader:
                self._raw_rows += 1
                mac = row.get('MAC', '').upper()
                if not mac:
                    continue

                ssid = row.get('SSID', '').strip('"')
                auth_mode = row.get('AuthMode', '')
                first_seen = _parse_timestamp(row.get('FirstSeen', ''))
                channel = row.get('Channel', '')
                frequency = row.get('Frequency', '')
                rssi = int(row.get('RSSI', 0) or 0)
                lat = float(row.get('CurrentLatitude', 0) or 0)
                lon = float(row.get('CurrentLongitude', 0) or 0)
                alt = float(row.get('AltitudeMeters', 0) or 0)
                accuracy = float(row.get('AccuracyMeters', 0) or 0)
                dev_type = row.get('Type', 'WIFI').strip()

                mac_lower = mac.lower()

                if mac_lower not in self._devices:
                    self._devices[mac_lower] = {
                        'mac': mac_lower,
                        'ssid': ssid,
                        'auth_mode': auth_mode,
                        'encryption': _parse_auth_mode(auth_mode),
                        'channels': set(),
                        'frequency': frequency,
                        'strongest_signal': rssi,
                        'first_time': first_seen,
                        'last_time': first_seen,
                        'manufacturer': lookup_manufacturer(mac_lower),
                        'randomized': is_randomized_mac(mac_lower),
                        'min_lat': lat if lat != 0 else 999,
                        'max_lat': lat if lat != 0 else -999,
                        'min_lon': lon if lon != 0 else 999,
                        'max_lon': lon if lon != 0 else -999,
                        'altitude': alt,
                        'accuracy': accuracy,
                        'type': dev_type,
                        'sighting_count': 0,
                        'has_gps': lat != 0 and lon != 0,
                    }

                dev = self._devices[mac_lower]
                dev['sighting_count'] += 1

                # Update SSID if we got a better one
                if ssid and not dev['ssid']:
                    dev['ssid'] = ssid

                # Track channels
                if channel:
                    dev['channels'].add(channel)

                # Update signal (keep strongest / closest to 0)
                if rssi != 0 and rssi > dev['strongest_signal']:
                    dev['strongest_signal'] = rssi

                # Update time range
                if first_seen:
                    if dev['first_time'] is None or first_seen < dev['first_time']:
                        dev['first_time'] = first_seen
                    if dev['last_time'] is None or first_seen > dev['last_time']:
                        dev['last_time'] = first_seen

                # Update GPS bounds
                if lat != 0 and lon != 0:
                    dev['has_gps'] = True
                    if lat < dev['min_lat']:
                        dev['min_lat'] = lat
                    if lat > dev['max_lat']:
                        dev['max_lat'] = lat
                    if lon < dev['min_lon']:
                        dev['min_lon'] = lon
                    if lon > dev['max_lon']:
                        dev['max_lon'] = lon

        # Clean up GPS sentinels
        for dev in self._devices.values():
            if not dev['has_gps']:
                dev['min_lat'] = dev['max_lat'] = 0
                dev['min_lon'] = dev['max_lon'] = 0

        self._is_loaded = True

    def close_database(self):
        """Reset all state."""
        self._file_path = None
        self._is_loaded = False
        self._devices.clear()
        self._raw_rows = 0

    def is_connected(self) -> bool:
        return self._is_loaded

    def get_database_info(self) -> dict:
        if not self._is_loaded:
            return {}
        return {
            'path': str(self._file_path),
            'filename': self._file_path.name if self._file_path else '',
            'size_bytes': self._file_path.stat().st_size if self._file_path else 0,
            'tables': ['wigle_csv'],
            'total_devices': len(self._devices),
            'total_sightings': self._raw_rows,
            'source_type': 'wigle_csv',
        }

    def get_device_summary(self) -> dict:
        if not self._is_loaded:
            return {}

        wifi_devs = [d for d in self._devices.values() if d['type'] == 'WIFI']
        bt_devs = [d for d in self._devices.values() if d['type'] in ('BT', 'BLE')]

        all_times = []
        for d in self._devices.values():
            if d['first_time']:
                all_times.append(d['first_time'])
            if d['last_time']:
                all_times.append(d['last_time'])

        gps_devs = [d for d in self._devices.values() if d['has_gps']]
        all_lats = [d['min_lat'] for d in gps_devs] + [d['max_lat'] for d in gps_devs]
        all_lons = [d['min_lon'] for d in gps_devs] + [d['max_lon'] for d in gps_devs]

        return {
            'total_devices': len(self._devices),
            'by_phy_type': {
                'IEEE802.11': len(wifi_devs),
                'Bluetooth': len(bt_devs),
            },
            'time_range': {
                'earliest': min(all_times) if all_times else None,
                'latest': max(all_times) if all_times else None,
            },
            'geographic_bounds': {
                'min_lat': min(all_lats) if all_lats else 0,
                'max_lat': max(all_lats) if all_lats else 0,
                'min_lon': min(all_lons) if all_lons else 0,
                'max_lon': max(all_lons) if all_lons else 0,
                'unique_devices_with_gps': len(gps_devs),
            },
        }

    def get_access_points(self, filters: Optional[dict] = None) -> pd.DataFrame:
        if not self._is_loaded:
            return pd.DataFrame()

        records = []
        for dev in self._devices.values():
            if dev['type'] != 'WIFI':
                continue
            # WiGLE doesn't distinguish AP vs client — all entries are APs
            # the recon module logs what it sees broadcasting
            channels = ', '.join(sorted(dev['channels'], key=lambda c: int(c) if c.isdigit() else 0))
            records.append({
                'devmac': dev['mac'],
                'name': dev['ssid'],
                'commonname': '',
                'channel': channels,
                'manufacturer': dev['manufacturer'],
                'encryption': dev['encryption'],
                'strongest_signal': dev['strongest_signal'] if dev['strongest_signal'] != 0 else None,
                'first_time': pd.Timestamp(dev['first_time']) if dev['first_time'] else pd.NaT,
                'last_time': pd.Timestamp(dev['last_time']) if dev['last_time'] else pd.NaT,
                'min_lat': dev['min_lat'],
                'min_lon': dev['min_lon'],
                'max_lat': dev['max_lat'],
                'max_lon': dev['max_lon'],
                'type': 'Wi-Fi AP',
                'device': None,
                'sighting_count': dev['sighting_count'],
            })

        df = pd.DataFrame(records)
        if filters and not df.empty:
            df = self._apply_filters(df, filters)
        return df

    def get_clients(self, filters: Optional[dict] = None) -> pd.DataFrame:
        """WiGLE CSV doesn't distinguish clients from APs."""
        return pd.DataFrame()

    def get_all_devices(self, filters: Optional[dict] = None) -> pd.DataFrame:
        if not self._is_loaded:
            return pd.DataFrame()

        records = []
        for dev in self._devices.values():
            channels = ', '.join(sorted(dev['channels'], key=lambda c: int(c) if c.isdigit() else 0))
            records.append({
                'devmac': dev['mac'],
                'phyname': 'IEEE802.11' if dev['type'] == 'WIFI' else 'Bluetooth',
                'type': 'Wi-Fi AP' if dev['type'] == 'WIFI' else 'Bluetooth',
                'name': dev['ssid'],
                'commonname': '',
                'manufacturer': dev['manufacturer'],
                'channel': channels,
                'strongest_signal': dev['strongest_signal'] if dev['strongest_signal'] != 0 else None,
                'first_time': pd.Timestamp(dev['first_time']) if dev['first_time'] else pd.NaT,
                'last_time': pd.Timestamp(dev['last_time']) if dev['last_time'] else pd.NaT,
                'min_lat': dev['min_lat'],
                'min_lon': dev['min_lon'],
                'max_lat': dev['max_lat'],
                'max_lon': dev['max_lon'],
            })

        df = pd.DataFrame(records)
        if filters and not df.empty:
            df = self._apply_filters(df, filters)
        return df

    def get_bluetooth_devices(self, filters: Optional[dict] = None) -> pd.DataFrame:
        if not self._is_loaded:
            return pd.DataFrame()

        records = []
        for dev in self._devices.values():
            if dev['type'] not in ('BT', 'BLE'):
                continue
            records.append({
                'devmac': dev['mac'],
                'name': dev['ssid'],
                'manufacturer': dev['manufacturer'],
                'strongest_signal': dev['strongest_signal'] if dev['strongest_signal'] != 0 else None,
                'first_time': pd.Timestamp(dev['first_time']) if dev['first_time'] else pd.NaT,
                'last_time': pd.Timestamp(dev['last_time']) if dev['last_time'] else pd.NaT,
                'type': dev['type'],
            })

        df = pd.DataFrame(records)
        if filters and not df.empty:
            df = self._apply_filters(df, filters)
        return df

    def get_networks(self) -> pd.DataFrame:
        if not self._is_loaded:
            return pd.DataFrame()

        ssid_counts: dict[str, int] = {}
        for dev in self._devices.values():
            if dev['ssid'] and dev['type'] == 'WIFI':
                ssid_counts[dev['ssid']] = ssid_counts.get(dev['ssid'], 0) + 1

        records = [{'ssid': ssid, 'ap_count': count}
                   for ssid, count in sorted(ssid_counts.items(),
                                             key=lambda x: x[1], reverse=True)]
        return pd.DataFrame(records)

    def get_gps_data(self) -> pd.DataFrame:
        if not self._is_loaded:
            return pd.DataFrame()

        records = []
        for dev in self._devices.values():
            if dev['has_gps']:
                avg_lat = (dev['min_lat'] + dev['max_lat']) / 2
                avg_lon = (dev['min_lon'] + dev['max_lon']) / 2
                records.append({
                    'devmac': dev['mac'],
                    'lat': avg_lat,
                    'lon': avg_lon,
                    'alt': dev['altitude'],
                    'name': dev['ssid'],
                })

        return pd.DataFrame(records)

    def get_device_gps_track(self, devmac: str) -> pd.DataFrame:
        """WiGLE CSV aggregates positions, no track data."""
        dev = self._devices.get(devmac.lower())
        if not dev or not dev['has_gps']:
            return pd.DataFrame()

        # Return bounding points as a simple track
        records = []
        if dev['min_lat'] != dev['max_lat'] or dev['min_lon'] != dev['max_lon']:
            records.append({'lat': dev['min_lat'], 'lon': dev['min_lon'], 'alt': dev['altitude']})
            records.append({'lat': dev['max_lat'], 'lon': dev['max_lon'], 'alt': dev['altitude']})
        else:
            records.append({'lat': dev['min_lat'], 'lon': dev['min_lon'], 'alt': dev['altitude']})

        return pd.DataFrame(records)

    def get_data_sources(self) -> pd.DataFrame:
        if not self._is_loaded:
            return pd.DataFrame()

        return pd.DataFrame([{
            'uuid': 'wigle-csv-import',
            'typestring': 'wigle_csv',
            'definition': str(self._file_path),
            'name': self._file_path.name if self._file_path else '',
            'interface': 'file',
        }])

    def get_alerts(self) -> pd.DataFrame:
        return pd.DataFrame()

    def get_packets_timeline(self) -> pd.DataFrame:
        """Build a timeline from first-seen timestamps."""
        if not self._is_loaded:
            return pd.DataFrame()

        # Bucket sightings by minute (WiGLE data is sparse compared to PCAP)
        time_buckets: dict[int, int] = {}
        for dev in self._devices.values():
            if dev['first_time']:
                ts = int(dev['first_time'].timestamp())
                bucket = (ts // 60) * 60  # per-minute buckets
                time_buckets[bucket] = time_buckets.get(bucket, 0) + 1

        records = []
        for ts, count in sorted(time_buckets.items()):
            records.append({
                'ts_sec': ts,
                'packet_count': count,
                'avg_signal': 0,
                'timestamp': pd.Timestamp(ts, unit='s'),
            })

        return pd.DataFrame(records)

    def get_signal_distribution(self) -> pd.DataFrame:
        if not self._is_loaded:
            return pd.DataFrame()

        signal_buckets: dict[int, int] = {}
        for dev in self._devices.values():
            sig = dev['strongest_signal']
            if sig != 0:
                bucket = (sig // 10) * 10
                signal_buckets[bucket] = signal_buckets.get(bucket, 0) + 1

        records = [{'signal_bucket': k, 'count': v}
                   for k, v in sorted(signal_buckets.items())]
        return pd.DataFrame(records)

    # PCAP-specific methods — WiGLE has none of this data
    def get_handshakes(self) -> pd.DataFrame:
        return pd.DataFrame()

    def get_deauth_frames(self) -> pd.DataFrame:
        return pd.DataFrame()

    def get_probe_requests(self) -> pd.DataFrame:
        return pd.DataFrame()

    def get_frame_type_distribution(self) -> pd.DataFrame:
        return pd.DataFrame()

    def has_pcap_features(self) -> bool:
        return False

    # ---- Filter support ----

    def _apply_filters(self, df: pd.DataFrame, filters: dict) -> pd.DataFrame:
        if not filters or df.empty:
            return df

        mask = pd.Series(True, index=df.index)

        if 'min_signal' in filters and filters['min_signal'] is not None:
            if 'strongest_signal' in df.columns:
                mask &= df['strongest_signal'].fillna(-100) >= filters['min_signal']

        if 'max_signal' in filters and filters['max_signal'] is not None:
            if 'strongest_signal' in df.columns:
                mask &= df['strongest_signal'].fillna(-100) <= filters['max_signal']

        if 'start_time' in filters and filters['start_time'] is not None:
            if 'first_time' in df.columns:
                mask &= df['first_time'] >= pd.Timestamp(filters['start_time'])

        if 'end_time' in filters and filters['end_time'] is not None:
            if 'last_time' in df.columns:
                mask &= df['last_time'] <= pd.Timestamp(filters['end_time'])

        if 'manufacturer' in filters and filters['manufacturer']:
            if 'manufacturer' in df.columns:
                mask &= df['manufacturer'].str.contains(
                    filters['manufacturer'], case=False, na=False)

        if 'channels' in filters and filters['channels']:
            if 'channel' in df.columns:
                channel_strs = [str(c) for c in filters['channels']]
                mask &= df['channel'].apply(
                    lambda x: any(ch in str(x).split(', ') for ch in channel_strs))

        return df[mask].reset_index(drop=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_database()
        return False
