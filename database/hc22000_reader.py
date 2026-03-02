"""Reader for .hc22000 hashcat WPA hash files."""

from pathlib import Path
from typing import Optional

import pandas as pd

from utils.oui_lookup import lookup_manufacturer


# Message pair bitmask → which EAPOL messages were captured
# See https://hashcat.net/wiki/doku.php?id=hccapx#message_pair_table
_MP_MESSAGES = {
    0: {1, 2},          # M1+M2, challenge
    1: {1, 4},          # M1+M4, verified
    2: {2, 3},          # M2+M3, verified
    3: {1, 2, 3, 4},    # Full (rare in practice from hcxpcapngtool)
    4: {1, 2},          # M1+M2, LE
    5: {1, 4},          # M1+M4, BE
    128: {1, 2},        # M1+M2, NC (not checked)
    129: {1, 4},
    130: {2, 3},
}


class Hc22000Reader:
    """Reader for hashcat .hc22000 / .22000 WPA hash files.

    Parses WPA*01* (PMKID) and WPA*02* (EAPOL) lines into handshake
    records compatible with PcapReader's get_handshakes() format.
    """

    def __init__(self):
        self._file_path: Optional[Path] = None
        self._is_loaded: bool = False
        self._handshakes: list[dict] = []
        self._networks: dict[str, dict] = {}  # bssid -> {ssid, ...}

    def open_database(self, path: str) -> bool:
        self._file_path = Path(path)
        if not self._file_path.exists():
            raise FileNotFoundError(f"File not found: {path}")
        self._parse()
        return True

    def close_database(self) -> None:
        self._handshakes.clear()
        self._networks.clear()
        self._is_loaded = False

    def is_connected(self) -> bool:
        return self._is_loaded

    def _parse(self):
        self._handshakes.clear()
        self._networks.clear()

        with open(self._file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue

                parts = line.split('*')
                if len(parts) < 4 or parts[0] != 'WPA':
                    continue

                hash_type = parts[1]  # 01=PMKID, 02=EAPOL

                if hash_type == '01':
                    self._parse_pmkid(parts)
                elif hash_type == '02':
                    self._parse_eapol(parts)

        self._is_loaded = True

    def _parse_pmkid(self, parts: list[str]):
        """Parse WPA*01*pmkid*macap*macsta*essid_hex***"""
        if len(parts) < 6:
            return
        mac_ap = self._format_mac(parts[3])
        mac_sta = self._format_mac(parts[4])
        essid = self._hex_to_str(parts[5])

        if not mac_ap:
            return

        self._networks.setdefault(mac_ap, {'ssid': essid})
        self._handshakes.append({
            'bssid': mac_ap,
            'client_mac': mac_sta or '00:00:00:00:00:00',
            'ssid': essid,
            'messages': {1},  # PMKID captures are crackable like a single-message capture
            'complete': True,  # PMKID is sufficient for cracking
            'hash_type': 'PMKID',
        })

    def _parse_eapol(self, parts: list[str]):
        """Parse WPA*02*mic*macap*macsta*essid_hex*nonce_ap*eapol*messagepair*"""
        if len(parts) < 9:
            return
        mac_ap = self._format_mac(parts[3])
        mac_sta = self._format_mac(parts[4])
        essid = self._hex_to_str(parts[5])
        mp_str = parts[8]

        if not mac_ap:
            return

        try:
            mp = int(mp_str)
        except ValueError:
            mp = 0

        messages = _MP_MESSAGES.get(mp, {1, 2})
        complete = len(messages) >= 2  # Any valid pair is crackable

        self._networks.setdefault(mac_ap, {'ssid': essid})
        self._handshakes.append({
            'bssid': mac_ap,
            'client_mac': mac_sta or '00:00:00:00:00:00',
            'ssid': essid,
            'messages': messages,
            'complete': complete,
            'hash_type': 'EAPOL',
        })

    @staticmethod
    def _format_mac(hex_str: str) -> str:
        """Convert '0011aabbccdd' → '00:11:aa:bb:cc:dd'."""
        h = hex_str.lower().replace(':', '').replace('-', '')
        if len(h) != 12:
            return ''
        return ':'.join(h[i:i+2] for i in range(0, 12, 2))

    @staticmethod
    def _hex_to_str(hex_str: str) -> str:
        """Decode hex-encoded ESSID."""
        try:
            return bytes.fromhex(hex_str).decode('utf-8', errors='replace')
        except (ValueError, UnicodeDecodeError):
            return hex_str

    # --- CaptureReader protocol methods ---

    def get_database_info(self) -> dict:
        return {
            'path': str(self._file_path) if self._file_path else '',
            'type': 'hc22000',
            'hash_count': len(self._handshakes),
            'network_count': len(self._networks),
        }

    def get_device_summary(self) -> dict:
        return {
            'total': len(self._networks),
            'access_points': len(self._networks),
            'clients': len({h['client_mac'] for h in self._handshakes}),
            'handshakes': len(self._handshakes),
        }

    def get_access_points(self, filters=None) -> pd.DataFrame:
        if not self._is_loaded:
            return pd.DataFrame()
        records = []
        for bssid, info in self._networks.items():
            hs_for_ap = [h for h in self._handshakes if h['bssid'] == bssid]
            clients = {h['client_mac'] for h in hs_for_ap}
            records.append({
                'mac': bssid,
                'ssid': info.get('ssid', ''),
                'manufacturer': lookup_manufacturer(bssid),
                'encryption': 'WPA2',
                'channel': 0,
                'strongest_signal': 0,
                'first_time': 0,
                'last_time': 0,
                'packets': 0,
                'clients': len(clients),
            })
        return pd.DataFrame(records)

    def get_clients(self, filters=None) -> pd.DataFrame:
        if not self._is_loaded:
            return pd.DataFrame()
        client_aps: dict[str, set] = {}
        for h in self._handshakes:
            client_aps.setdefault(h['client_mac'], set()).add(h['bssid'])
        records = []
        for mac, aps in client_aps.items():
            records.append({
                'mac': mac,
                'manufacturer': lookup_manufacturer(mac),
                'strongest_signal': 0,
                'first_time': 0,
                'last_time': 0,
                'last_bssid': next(iter(aps)),
                'packets': 0,
                'associated_aps': len(aps),
                'probed_ssids': '',
            })
        return pd.DataFrame(records)

    def get_all_devices(self, filters=None) -> pd.DataFrame:
        ap_df = self.get_access_points(filters)
        cl_df = self.get_clients(filters)
        if ap_df.empty and cl_df.empty:
            return pd.DataFrame()
        frames = []
        if not ap_df.empty:
            ap_df = ap_df.copy()
            ap_df['type'] = 'Wi-Fi AP'
            frames.append(ap_df)
        if not cl_df.empty:
            cl_df = cl_df.copy()
            cl_df['type'] = 'Wi-Fi Client'
            frames.append(cl_df)
        return pd.concat(frames, ignore_index=True)

    def get_bluetooth_devices(self, filters=None) -> pd.DataFrame:
        return pd.DataFrame()

    def get_networks(self) -> pd.DataFrame:
        return self.get_access_points()

    def get_gps_data(self) -> pd.DataFrame:
        return pd.DataFrame()

    def get_device_gps_track(self, devmac: str) -> pd.DataFrame:
        return pd.DataFrame()

    def get_data_sources(self) -> pd.DataFrame:
        return pd.DataFrame()

    def get_alerts(self) -> pd.DataFrame:
        return pd.DataFrame()

    def get_packets_timeline(self) -> pd.DataFrame:
        return pd.DataFrame()

    def get_signal_distribution(self) -> pd.DataFrame:
        return pd.DataFrame()

    def get_handshakes(self) -> pd.DataFrame:
        if not self._is_loaded:
            return pd.DataFrame()
        records = []
        for h in self._handshakes:
            records.append({
                'bssid': h['bssid'],
                'ssid': h['ssid'],
                'client_mac': h['client_mac'],
                'client_manufacturer': lookup_manufacturer(h['client_mac']),
                'timestamp': pd.NaT,
                'messages': ', '.join(str(m) for m in sorted(h['messages'])),
                'complete': h['complete'],
            })
        return pd.DataFrame(records)

    def get_deauth_frames(self) -> pd.DataFrame:
        return pd.DataFrame()

    def get_probe_requests(self) -> pd.DataFrame:
        return pd.DataFrame()

    def get_frame_type_distribution(self) -> pd.DataFrame:
        return pd.DataFrame()

    def has_pcap_features(self) -> bool:
        return True  # We have handshakes
