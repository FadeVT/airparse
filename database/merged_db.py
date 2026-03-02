"""Unified merge database that combines data from multiple capture sources.

Implements the CaptureReader protocol so the UI treats it identically
to any single-file reader. Supports BSSID-keyed GPS enrichment — devices
captured without GPS get coordinates from other sources that saw the same BSSID.
"""

from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

from utils.oui_lookup import lookup_manufacturer, is_randomized_mac


@dataclass
class MergedNetwork:
    bssid: str
    ssid: str = ''
    channel: int = 0
    encryption: str = 'Unknown'
    manufacturer: str = ''
    strongest_signal: int = -100
    first_time: int = 0
    last_time: int = 0
    beacon_count: int = 0
    data_count: int = 0
    clients: set = field(default_factory=set)
    sources: set = field(default_factory=set)
    # GPS
    has_gps: bool = False
    lat: float = 0.0
    lon: float = 0.0
    min_lat: float = 0.0
    max_lat: float = 0.0
    min_lon: float = 0.0
    max_lon: float = 0.0
    gps_source: str = ''


@dataclass
class MergedClient:
    mac: str
    manufacturer: str = ''
    strongest_signal: int = -100
    first_time: int = 0
    last_time: int = 0
    last_bssid: str = ''
    probed_ssids: set = field(default_factory=set)
    associated_aps: set = field(default_factory=set)
    packet_count: int = 0
    sources: set = field(default_factory=set)


@dataclass
class MergedHandshake:
    bssid: str
    client_mac: str
    ssid: str = ''
    timestamp: int = 0
    eapol_messages: set = field(default_factory=set)
    source: str = ''
    pcap_path: str = ''

    @property
    def complete(self) -> bool:
        return len(self.eapol_messages) >= 4


class MergedDatabase:
    """Unified database merging data from multiple capture sources.

    Implements the CaptureReader protocol for seamless UI integration.
    """

    def __init__(self):
        self._networks: dict[str, MergedNetwork] = {}
        self._clients: dict[str, MergedClient] = {}
        self._handshakes: dict[str, MergedHandshake] = {}  # "bssid:client" key
        self._gps_points: dict[str, list] = {}  # bssid -> [(lat, lon, signal, source)]
        self._deauths: list[dict] = []
        self._probes: list[dict] = []
        self._data_sources: list[dict] = []
        self._source_names: list[str] = []
        self._has_pcap: bool = False
        self._is_loaded: bool = False
        self._pcap_paths: list[str] = []

    def open_database(self, path: str) -> bool:
        return True

    def close_database(self) -> None:
        self._networks.clear()
        self._clients.clear()
        self._handshakes.clear()
        self._gps_points.clear()
        self._deauths.clear()
        self._probes.clear()
        self._data_sources.clear()
        self._source_names.clear()
        self._pcap_paths.clear()
        self._has_pcap = False
        self._is_loaded = False

    def is_connected(self) -> bool:
        return self._is_loaded

    # --- Ingestion methods ---

    def ingest_pcap(self, reader, source_name: str, pcap_path: str = ''):
        """Ingest data from a PcapReader into the merged database."""
        self._source_names.append(source_name)
        self._has_pcap = True
        if pcap_path:
            self._pcap_paths.append(pcap_path)

        # APs
        ap_df = reader.get_access_points()
        if not ap_df.empty:
            for _, row in ap_df.iterrows():
                bssid = str(row.get('devmac', '')).lower()
                if not bssid:
                    continue
                self._merge_network(
                    bssid=bssid,
                    ssid=str(row.get('name', '')),
                    channel=int(row.get('channel', 0) or 0) if str(row.get('channel', '')).isdigit() else 0,
                    encryption=str(row.get('encryption', 'Unknown')),
                    manufacturer=str(row.get('manufacturer', '')),
                    signal=int(row.get('strongest_signal', -100) or -100),
                    first_time=self._ts_to_epoch(row.get('first_time')),
                    last_time=self._ts_to_epoch(row.get('last_time')),
                    beacon_count=int(row.get('beacon_count', 0) or 0),
                    data_count=int(row.get('data_count', 0) or 0),
                    client_count=int(row.get('client_count', 0) or 0),
                    lat=float(row.get('min_lat', 0) or 0),
                    lon=float(row.get('min_lon', 0) or 0),
                    min_lat=float(row.get('min_lat', 0) or 0),
                    max_lat=float(row.get('max_lat', 0) or 0),
                    min_lon=float(row.get('min_lon', 0) or 0),
                    max_lon=float(row.get('max_lon', 0) or 0),
                    source=source_name,
                )

        # Clients
        cl_df = reader.get_clients()
        if not cl_df.empty:
            for _, row in cl_df.iterrows():
                mac = str(row.get('client_mac', '')).lower()
                if not mac:
                    continue
                last_bssid = str(row.get('last_bssid', '')).lower()
                self._merge_client(
                    mac=mac,
                    manufacturer=str(row.get('manufacturer', '')),
                    signal=int(row.get('strongest_signal', -100) or -100),
                    first_time=self._ts_to_epoch(row.get('first_time')),
                    last_time=self._ts_to_epoch(row.get('last_time')),
                    last_bssid=last_bssid,
                    probed_ssids=str(row.get('probed_ssids', '')).split(', ') if row.get('probed_ssids') else [],
                    source=source_name,
                )
                if last_bssid and last_bssid in self._networks:
                    self._networks[last_bssid].clients.add(mac)

        # Handshakes
        hs_df = reader.get_handshakes()
        if not hs_df.empty:
            for _, row in hs_df.iterrows():
                bssid = str(row.get('bssid', '')).lower()
                client = str(row.get('client_mac', '')).lower()
                if not bssid:
                    continue
                msgs_str = str(row.get('messages', ''))
                msgs = {int(m.strip()) for m in msgs_str.split(',') if m.strip().isdigit()}
                self._merge_handshake(
                    bssid=bssid,
                    client_mac=client,
                    ssid=str(row.get('ssid', '')),
                    timestamp=self._ts_to_epoch(row.get('timestamp')),
                    messages=msgs,
                    source=source_name,
                    pcap_path=pcap_path,
                )

        # Deauths
        deauth_df = reader.get_deauth_frames()
        if not deauth_df.empty:
            for _, row in deauth_df.iterrows():
                self._deauths.append(row.to_dict())

        # Probes
        probe_df = reader.get_probe_requests()
        if not probe_df.empty:
            for _, row in probe_df.iterrows():
                self._probes.append(row.to_dict())

        self._data_sources.append({
            'uuid': f'{source_name}-pcap',
            'typestring': 'pcap',
            'definition': pcap_path,
            'name': source_name,
            'interface': 'file',
        })

        self._is_loaded = True

    def ingest_kismet(self, reader, source_name: str):
        """Ingest data from a KismetDBReader."""
        self._source_names.append(source_name)

        # APs
        ap_df = reader.get_access_points()
        if not ap_df.empty:
            for _, row in ap_df.iterrows():
                bssid = str(row.get('devmac', '')).lower()
                if not bssid:
                    continue
                self._merge_network(
                    bssid=bssid,
                    ssid=str(row.get('name', '')),
                    channel=int(row.get('channel', 0) or 0) if str(row.get('channel', '')).isdigit() else 0,
                    encryption=str(row.get('encryption', 'Unknown')),
                    manufacturer=str(row.get('manufacturer', '')),
                    signal=int(row.get('strongest_signal', -100) or -100),
                    first_time=self._ts_to_epoch(row.get('first_time')),
                    last_time=self._ts_to_epoch(row.get('last_time')),
                    lat=float(row.get('min_lat', 0) or 0),
                    lon=float(row.get('min_lon', 0) or 0),
                    min_lat=float(row.get('min_lat', 0) or 0),
                    max_lat=float(row.get('max_lat', 0) or 0),
                    min_lon=float(row.get('min_lon', 0) or 0),
                    max_lon=float(row.get('max_lon', 0) or 0),
                    source=source_name,
                )

        # GPS data from Kismet's data table
        gps_df = reader.get_gps_data()
        if not gps_df.empty:
            for _, row in gps_df.iterrows():
                mac = str(row.get('devmac', '')).lower()
                lat = float(row.get('lat', 0) or 0)
                lon = float(row.get('lon', 0) or 0)
                if mac and lat != 0 and lon != 0:
                    self._gps_points.setdefault(mac, []).append(
                        (lat, lon, 0, source_name))

        # Clients
        cl_df = reader.get_clients()
        if not cl_df.empty:
            for _, row in cl_df.iterrows():
                mac = str(row.get('devmac', row.get('client_mac', ''))).lower()
                if not mac:
                    continue
                self._merge_client(
                    mac=mac,
                    manufacturer=str(row.get('manufacturer', '')),
                    signal=int(row.get('strongest_signal', -100) or -100),
                    first_time=self._ts_to_epoch(row.get('first_time')),
                    last_time=self._ts_to_epoch(row.get('last_time')),
                    last_bssid='',
                    source=source_name,
                )

        self._data_sources.append({
            'uuid': f'{source_name}-kismet',
            'typestring': 'kismet',
            'definition': str(reader.db_path) if hasattr(reader, 'db_path') else '',
            'name': source_name,
            'interface': 'file',
        })

        self._is_loaded = True

    def ingest_wigle(self, reader, source_name: str):
        """Ingest GPS data from a WigleCsvReader."""
        self._source_names.append(source_name)

        gps_df = reader.get_gps_data()
        if not gps_df.empty:
            for _, row in gps_df.iterrows():
                mac = str(row.get('devmac', '')).lower()
                lat = float(row.get('lat', 0) or 0)
                lon = float(row.get('lon', 0) or 0)
                if mac and lat != 0 and lon != 0:
                    self._gps_points.setdefault(mac, []).append(
                        (lat, lon, 0, source_name))

        # Also merge AP data from WiGLE
        ap_df = reader.get_access_points()
        if not ap_df.empty:
            for _, row in ap_df.iterrows():
                mac = str(row.get('devmac', row.get('mac', ''))).lower()
                if not mac:
                    continue
                self._merge_network(
                    bssid=mac,
                    ssid=str(row.get('name', row.get('ssid', ''))),
                    channel=int(row.get('channel', 0) or 0) if str(row.get('channel', '')).isdigit() else 0,
                    encryption=str(row.get('encryption', 'Unknown')),
                    manufacturer=str(row.get('manufacturer', '')),
                    signal=int(row.get('strongest_signal', -100) or -100),
                    first_time=self._ts_to_epoch(row.get('first_time')),
                    last_time=self._ts_to_epoch(row.get('last_time')),
                    lat=float(row.get('min_lat', row.get('lat', 0)) or 0),
                    lon=float(row.get('min_lon', row.get('lon', 0)) or 0),
                    min_lat=float(row.get('min_lat', 0) or 0),
                    max_lat=float(row.get('max_lat', 0) or 0),
                    min_lon=float(row.get('min_lon', 0) or 0),
                    max_lon=float(row.get('max_lon', 0) or 0),
                    source=source_name,
                )

        self._is_loaded = True

    def ingest_hc22000(self, reader, source_name: str):
        """Ingest handshake data from a Hc22000Reader."""
        self._source_names.append(source_name)
        self._has_pcap = True

        hs_df = reader.get_handshakes()
        if not hs_df.empty:
            for _, row in hs_df.iterrows():
                bssid = str(row.get('bssid', '')).lower()
                client = str(row.get('client_mac', '')).lower()
                ssid = str(row.get('ssid', ''))
                if not bssid:
                    continue
                msgs_str = str(row.get('messages', ''))
                msgs = {int(m.strip()) for m in msgs_str.split(',') if m.strip().isdigit()}
                self._merge_handshake(bssid, client, ssid, 0, msgs, source_name)

                # Also create network entries
                self._merge_network(bssid=bssid, ssid=ssid, source=source_name)

        self._is_loaded = True

    def _link_clients_to_networks(self):
        """Back-link all clients to their AP's clients set via associated_aps."""
        for mac, cl in self._clients.items():
            for ap_bssid in cl.associated_aps:
                if ap_bssid in self._networks:
                    self._networks[ap_bssid].clients.add(mac)

    # --- GPS Enrichment ---

    def enrich_gps(self):
        """Cross-reference BSSIDs to fill GPS gaps from other sources."""
        self._link_clients_to_networks()
        for bssid, net in self._networks.items():
            if net.has_gps:
                continue
            points = self._gps_points.get(bssid, [])
            if not points:
                continue

            # Signal-weighted centroid (reusing PcapReader algorithm)
            total_weight = 0.0
            w_lat = 0.0
            w_lon = 0.0
            min_lat = min_lon = float('inf')
            max_lat = max_lon = float('-inf')

            for lat, lon, signal, src in points:
                if lat == 0 and lon == 0:
                    continue
                weight = 10.0 ** ((signal + 100.0) / 20.0) if signal != 0 else 1.0
                w_lat += lat * weight
                w_lon += lon * weight
                total_weight += weight
                min_lat = min(min_lat, lat)
                max_lat = max(max_lat, lat)
                min_lon = min(min_lon, lon)
                max_lon = max(max_lon, lon)

            if total_weight > 0:
                net.lat = w_lat / total_weight
                net.lon = w_lon / total_weight
                net.min_lat = min_lat
                net.max_lat = max_lat
                net.min_lon = min_lon
                net.max_lon = max_lon
                net.has_gps = True
                net.gps_source = 'cross-reference'

    def get_networks_without_gps(self) -> list[str]:
        """Return BSSIDs that still lack GPS after local enrichment."""
        return [bssid for bssid, net in self._networks.items()
                if not net.has_gps]

    def apply_wigle_result(self, bssid: str, lat: float, lon: float,
                           ssid: str = '', channel: int = 0,
                           encryption: str = ''):
        """Apply a WiGLE API result to a network entry."""
        net = self._networks.get(bssid)
        if not net:
            return
        if lat == 0 and lon == 0:
            return
        net.lat = lat
        net.lon = lon
        net.min_lat = lat
        net.max_lat = lat
        net.min_lon = lon
        net.max_lon = lon
        net.has_gps = True
        net.gps_source = 'wigle-api'
        if ssid and not net.ssid:
            net.ssid = ssid
        if channel and not net.channel:
            net.channel = channel
        if encryption and net.encryption == 'Unknown':
            net.encryption = encryption

    # --- Merge helpers ---

    def _merge_network(self, bssid: str, ssid: str = '', channel: int = 0,
                       encryption: str = 'Unknown', manufacturer: str = '',
                       signal: int = -100, first_time: int = 0, last_time: int = 0,
                       beacon_count: int = 0, data_count: int = 0,
                       client_count: int = 0,
                       lat: float = 0, lon: float = 0,
                       min_lat: float = 0, max_lat: float = 0,
                       min_lon: float = 0, max_lon: float = 0,
                       source: str = ''):
        if bssid in self._networks:
            net = self._networks[bssid]
            if ssid and not net.ssid:
                net.ssid = ssid
            if channel and not net.channel:
                net.channel = channel
            if encryption != 'Unknown' and net.encryption == 'Unknown':
                net.encryption = encryption
            if manufacturer and not net.manufacturer:
                net.manufacturer = manufacturer
            if signal > net.strongest_signal:
                net.strongest_signal = signal
            if first_time and (not net.first_time or first_time < net.first_time):
                net.first_time = first_time
            if last_time and last_time > net.last_time:
                net.last_time = last_time
            net.beacon_count += beacon_count
            net.data_count += data_count
            # GPS: take the first non-zero GPS, or update with better bounds
            if lat != 0 and lon != 0 and not net.has_gps:
                net.lat = lat
                net.lon = lon
                net.min_lat = min_lat or lat
                net.max_lat = max_lat or lat
                net.min_lon = min_lon or lon
                net.max_lon = max_lon or lon
                net.has_gps = True
                net.gps_source = source
            net.sources.add(source)
        else:
            has_gps = lat != 0 and lon != 0
            self._networks[bssid] = MergedNetwork(
                bssid=bssid, ssid=ssid, channel=channel,
                encryption=encryption,
                manufacturer=manufacturer or lookup_manufacturer(bssid),
                strongest_signal=signal,
                first_time=first_time, last_time=last_time,
                beacon_count=beacon_count, data_count=data_count,
                sources={source},
                has_gps=has_gps,
                lat=lat, lon=lon,
                min_lat=min_lat or lat, max_lat=max_lat or lat,
                min_lon=min_lon or lon, max_lon=max_lon or lon,
                gps_source=source if has_gps else '',
            )

    def _merge_client(self, mac: str, manufacturer: str = '', signal: int = -100,
                      first_time: int = 0, last_time: int = 0,
                      last_bssid: str = '', probed_ssids: list = None,
                      source: str = ''):
        if mac in self._clients:
            cl = self._clients[mac]
            if manufacturer and not cl.manufacturer:
                cl.manufacturer = manufacturer
            if signal > cl.strongest_signal:
                cl.strongest_signal = signal
            if first_time and (not cl.first_time or first_time < cl.first_time):
                cl.first_time = first_time
            if last_time and last_time > cl.last_time:
                cl.last_time = last_time
                if last_bssid:
                    cl.last_bssid = last_bssid
            if probed_ssids:
                cl.probed_ssids.update(s for s in probed_ssids if s)
            if last_bssid:
                cl.associated_aps.add(last_bssid)
            cl.sources.add(source)
        else:
            self._clients[mac] = MergedClient(
                mac=mac,
                manufacturer=manufacturer or lookup_manufacturer(mac),
                strongest_signal=signal,
                first_time=first_time, last_time=last_time,
                last_bssid=last_bssid,
                probed_ssids=set(s for s in (probed_ssids or []) if s),
                associated_aps={last_bssid} if last_bssid else set(),
                sources={source},
            )

    def _merge_handshake(self, bssid: str, client_mac: str, ssid: str = '',
                         timestamp: int = 0, messages: set = None,
                         source: str = '', pcap_path: str = ''):
        key = f"{bssid}:{client_mac}"
        if key in self._handshakes:
            hs = self._handshakes[key]
            if messages:
                hs.eapol_messages.update(messages)
            if ssid and not hs.ssid:
                hs.ssid = ssid
            if timestamp and not hs.timestamp:
                hs.timestamp = timestamp
            if pcap_path and not hs.pcap_path:
                hs.pcap_path = pcap_path
        else:
            self._handshakes[key] = MergedHandshake(
                bssid=bssid, client_mac=client_mac, ssid=ssid,
                timestamp=timestamp,
                eapol_messages=messages or set(),
                source=source,
                pcap_path=pcap_path,
            )

    @staticmethod
    def _ts_to_epoch(val) -> int:
        """Convert a timestamp value to Unix epoch seconds."""
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return 0
        if isinstance(val, pd.Timestamp):
            if val is pd.NaT:
                return 0
            return int(val.timestamp())
        if isinstance(val, (int, float)):
            return int(val)
        return 0

    # --- CaptureReader protocol ---

    def get_database_info(self) -> dict:
        return {
            'path': 'Merged Database',
            'type': 'merged',
            'sources': self._source_names,
            'network_count': len(self._networks),
            'client_count': len(self._clients),
            'handshake_count': len(self._handshakes),
        }

    def get_device_summary(self) -> dict:
        gps_count = sum(1 for n in self._networks.values() if n.has_gps)
        return {
            'total': len(self._networks) + len(self._clients),
            'access_points': len(self._networks),
            'clients': len(self._clients),
            'handshakes': len(self._handshakes),
            'sources': len(self._source_names),
            'gps_enriched': gps_count,
        }

    def get_access_points(self, filters=None) -> pd.DataFrame:
        if not self._is_loaded:
            return pd.DataFrame()
        records = []
        for net in self._networks.values():
            records.append({
                'devmac': net.bssid,
                'name': net.ssid,
                'commonname': '',
                'channel': str(net.channel) if net.channel else '',
                'manufacturer': net.manufacturer,
                'encryption': net.encryption,
                'strongest_signal': net.strongest_signal if net.strongest_signal > -100 else None,
                'first_time': pd.Timestamp(net.first_time, unit='s') if net.first_time else pd.NaT,
                'last_time': pd.Timestamp(net.last_time, unit='s') if net.last_time else pd.NaT,
                'min_lat': net.min_lat if net.has_gps else 0,
                'min_lon': net.min_lon if net.has_gps else 0,
                'max_lat': net.max_lat if net.has_gps else 0,
                'max_lon': net.max_lon if net.has_gps else 0,
                'type': 'Wi-Fi AP',
                'device': None,
                'beacon_count': net.beacon_count,
                'data_count': net.data_count,
                'client_count': len(net.clients),
            })
        return pd.DataFrame(records)

    def get_clients(self, filters=None) -> pd.DataFrame:
        if not self._is_loaded:
            return pd.DataFrame()
        records = []
        for cl in self._clients.values():
            last_ssid = ''
            if cl.last_bssid and cl.last_bssid in self._networks:
                last_ssid = self._networks[cl.last_bssid].ssid
            records.append({
                'client_mac': cl.mac,
                'name': last_ssid,
                'commonname': '',
                'manufacturer': cl.manufacturer,
                'last_bssid': cl.last_bssid,
                'strongest_signal': cl.strongest_signal if cl.strongest_signal > -100 else None,
                'first_time': pd.Timestamp(cl.first_time, unit='s') if cl.first_time else pd.NaT,
                'last_time': pd.Timestamp(cl.last_time, unit='s') if cl.last_time else pd.NaT,
                'min_lat': 0, 'min_lon': 0,
                'type': 'Wi-Fi Client',
                'device': None,
                'probed_ssids': ', '.join(sorted(cl.probed_ssids)),
                'associated_aps': len(cl.associated_aps),
                'randomized_mac': is_randomized_mac(cl.mac),
            })
        return pd.DataFrame(records)

    def get_all_devices(self, filters=None) -> pd.DataFrame:
        ap_df = self.get_access_points(filters)
        cl_df = self.get_clients(filters)
        frames = []
        if not ap_df.empty:
            ap_df = ap_df.copy()
            ap_df['phyname'] = 'IEEE802.11'
            frames.append(ap_df)
        if not cl_df.empty:
            cl_df = cl_df.copy()
            cl_df['phyname'] = 'IEEE802.11'
            # Rename for consistency
            if 'client_mac' in cl_df.columns:
                cl_df = cl_df.rename(columns={'client_mac': 'devmac'})
            frames.append(cl_df)
        if frames:
            return pd.concat(frames, ignore_index=True)
        return pd.DataFrame()

    def get_bluetooth_devices(self, filters=None) -> pd.DataFrame:
        return pd.DataFrame()

    def get_networks(self) -> pd.DataFrame:
        if not self._is_loaded:
            return pd.DataFrame()
        ssid_counts: dict[str, int] = {}
        for net in self._networks.values():
            if net.ssid:
                ssid_counts[net.ssid] = ssid_counts.get(net.ssid, 0) + 1
        records = [{'ssid': ssid, 'ap_count': count}
                   for ssid, count in sorted(ssid_counts.items(),
                                             key=lambda x: x[1], reverse=True)]
        return pd.DataFrame(records)

    def get_gps_data(self) -> pd.DataFrame:
        if not self._is_loaded:
            return pd.DataFrame()
        records = []
        for net in self._networks.values():
            if net.has_gps and net.lat != 0 and net.lon != 0:
                records.append({
                    'devmac': net.bssid,
                    'lat': net.lat,
                    'lon': net.lon,
                    'alt': 0,
                    'name': net.ssid,
                })
        return pd.DataFrame(records)

    def get_device_gps_track(self, devmac: str) -> pd.DataFrame:
        mac = devmac.lower()
        net = self._networks.get(mac)
        if not net or not net.has_gps:
            return pd.DataFrame()
        records = [{'lat': net.lat, 'lon': net.lon, 'alt': 0}]
        if net.min_lat != net.max_lat or net.min_lon != net.max_lon:
            records.append({'lat': net.min_lat, 'lon': net.min_lon, 'alt': 0})
            records.append({'lat': net.max_lat, 'lon': net.max_lon, 'alt': 0})
        return pd.DataFrame(records)

    def get_data_sources(self) -> pd.DataFrame:
        if not self._data_sources:
            return pd.DataFrame()
        return pd.DataFrame(self._data_sources)

    def get_alerts(self) -> pd.DataFrame:
        return pd.DataFrame()

    def get_packets_timeline(self) -> pd.DataFrame:
        return pd.DataFrame()

    def get_signal_distribution(self) -> pd.DataFrame:
        if not self._is_loaded:
            return pd.DataFrame()
        signal_buckets: dict[int, int] = {}
        for net in self._networks.values():
            if net.strongest_signal and net.strongest_signal > -100:
                bucket = (net.strongest_signal // 10) * 10
                signal_buckets[bucket] = signal_buckets.get(bucket, 0) + 1
        for cl in self._clients.values():
            if cl.strongest_signal and cl.strongest_signal > -100:
                bucket = (cl.strongest_signal // 10) * 10
                signal_buckets[bucket] = signal_buckets.get(bucket, 0) + 1
        records = [{'signal_bucket': k, 'count': v}
                   for k, v in sorted(signal_buckets.items())]
        return pd.DataFrame(records)

    def get_handshakes(self) -> pd.DataFrame:
        if not self._is_loaded:
            return pd.DataFrame()
        records = []
        for hs in self._handshakes.values():
            ssid = hs.ssid
            if not ssid and hs.bssid in self._networks:
                ssid = self._networks[hs.bssid].ssid
            records.append({
                'bssid': hs.bssid,
                'ssid': ssid,
                'client_mac': hs.client_mac,
                'client_manufacturer': lookup_manufacturer(hs.client_mac),
                'timestamp': pd.Timestamp(hs.timestamp, unit='s') if hs.timestamp else pd.NaT,
                'messages': ', '.join(str(m) for m in sorted(hs.eapol_messages)),
                'complete': hs.complete,
            })
        return pd.DataFrame(records)

    def get_deauth_frames(self) -> pd.DataFrame:
        if not self._deauths:
            return pd.DataFrame()
        return pd.DataFrame(self._deauths)

    def get_probe_requests(self) -> pd.DataFrame:
        if not self._probes:
            return pd.DataFrame()
        return pd.DataFrame(self._probes)

    def get_frame_type_distribution(self) -> pd.DataFrame:
        return pd.DataFrame()

    def has_pcap_features(self) -> bool:
        return self._has_pcap

    @property
    def primary_pcap_path(self) -> str:
        """Return the first PCAP path for hashcat operations."""
        return self._pcap_paths[0] if self._pcap_paths else ''
