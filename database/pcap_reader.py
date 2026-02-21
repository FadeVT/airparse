"""PCAP file reader for wireless capture analysis."""

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

import pandas as pd

from utils.oui_lookup import lookup_manufacturer, is_randomized_mac


# 802.11 deauthentication reason codes
DEAUTH_REASONS = {
    0: "Unspecified",
    1: "Unspecified",
    2: "Auth no longer valid",
    3: "Deauth: STA leaving",
    4: "Inactivity",
    5: "AP busy",
    6: "Class 2 from non-auth STA",
    7: "Class 3 from non-assoc STA",
    8: "Disassoc: STA leaving",
    9: "STA not authenticated",
    10: "Bad power capability",
    11: "Bad supported channels",
    12: "Unspecified (12)",
    13: "Invalid information element",
    14: "MIC failure",
    15: "4-way handshake timeout",
    16: "Group key handshake timeout",
    17: "IE in 4-way differs",
    18: "Invalid group cipher",
    19: "Invalid pairwise cipher",
    20: "Invalid AKMP",
    21: "Unsupported RSN version",
    22: "Invalid RSN capabilities",
    23: "802.1X auth failed",
    24: "Cipher suite rejected",
    25: "TDLS teardown unreachable",
    26: "TDLS teardown unspecified",
}

# Broadcast/multicast MAC addresses to ignore as clients
IGNORED_MACS = {
    'ff:ff:ff:ff:ff:ff',
    '00:00:00:00:00:00',
    '33:33:00:00:00:01',  # IPv6 all-nodes
    '33:33:ff:00:00:00',  # IPv6 solicited-node prefix
    '01:00:5e:00:00:00',  # IPv4 multicast prefix
}


def _is_broadcast_or_multicast(mac: str) -> bool:
    """Check if a MAC is broadcast, multicast, or should be ignored."""
    if not mac:
        return True
    mac_lower = mac.lower()
    if mac_lower in IGNORED_MACS:
        return True
    # Check multicast bit (LSB of first octet)
    try:
        first_octet = int(mac_lower.replace(':', '').replace('-', '')[:2], 16)
        return bool(first_octet & 0x01)
    except (ValueError, IndexError):
        return True


@dataclass
class APRecord:
    """Aggregated access point record."""
    bssid: str
    ssid: str = ""
    channel: int = 0
    frequency: int = 0
    encryption: str = "Unknown"
    manufacturer: str = ""
    strongest_signal: int = -100
    first_time: int = 0
    last_time: int = 0
    packet_count: int = 0
    beacon_count: int = 0
    data_count: int = 0
    clients: set = field(default_factory=set)

    def update_time(self, ts: int):
        if self.first_time == 0 or ts < self.first_time:
            self.first_time = ts
        if ts > self.last_time:
            self.last_time = ts

    def update_signal(self, sig: int):
        if sig != 0 and sig > self.strongest_signal:
            self.strongest_signal = sig


@dataclass
class ClientRecord:
    """Aggregated client device record."""
    mac: str
    manufacturer: str = ""
    strongest_signal: int = -100
    first_time: int = 0
    last_time: int = 0
    last_bssid: str = ""
    probed_ssids: list = field(default_factory=list)
    packet_count: int = 0
    associated_aps: set = field(default_factory=set)

    def update_time(self, ts: int):
        if self.first_time == 0 or ts < self.first_time:
            self.first_time = ts
        if ts > self.last_time:
            self.last_time = ts

    def update_signal(self, sig: int):
        if sig != 0 and sig > self.strongest_signal:
            self.strongest_signal = sig


@dataclass
class HandshakeRecord:
    """Detected WPA handshake."""
    bssid: str
    client_mac: str
    timestamp: int
    eapol_messages: set = field(default_factory=set)

    @property
    def complete(self) -> bool:
        return len(self.eapol_messages) >= 4


@dataclass
class DeauthRecord:
    """Deauthentication/disassociation frame."""
    source_mac: str
    dest_mac: str
    bssid: str
    timestamp: int
    reason_code: int
    subtype: str  # "deauth" or "disassoc"

    @property
    def reason_text(self) -> str:
        return DEAUTH_REASONS.get(self.reason_code, f"Unknown ({self.reason_code})")


@dataclass
class ProbeRecord:
    """Probe request record."""
    client_mac: str
    ssid: str
    timestamp: int
    signal: int = -100


@dataclass
class TimelineBucket:
    """Packet counts for a single second."""
    packet_count: int = 0
    signal_sum: int = 0
    signal_count: int = 0

    @property
    def avg_signal(self) -> float:
        if self.signal_count == 0:
            return 0
        return self.signal_sum / self.signal_count


class PcapReader:
    """Reader for PCAP wireless capture files."""

    # Maximum deauth records to store (deauth floods can be huge)
    MAX_DEAUTH_RECORDS = 10000

    def __init__(self):
        self._file_path: Optional[Path] = None
        self._is_loaded: bool = False

        # Aggregated device records
        self._access_points: dict[str, APRecord] = {}
        self._clients: dict[str, ClientRecord] = {}

        # PCAP-specific data
        self._handshakes: dict[tuple, HandshakeRecord] = {}  # (bssid, client) -> record
        self._deauths: list[DeauthRecord] = []
        self._deauth_total_count: int = 0
        self._probe_requests: list[ProbeRecord] = []

        # Timeline data
        self._packet_timeline: dict[int, TimelineBucket] = {}
        self._frame_type_counts: dict[str, int] = {}
        self._frame_subtype_counts: dict[str, int] = {}

        # Metadata
        self._total_packets: int = 0
        self._file_size: int = 0

        # Companion WiGLE GPS data (loaded from external CSV files)
        self._wigle_gps: dict[str, dict] = {}  # mac_lower -> {min_lat, max_lat, min_lon, max_lon, alt, ssid}

    def open_database(self, path: str) -> bool:
        """Validate a PCAP file can be opened."""
        self._file_path = Path(path)

        if not self._file_path.exists():
            raise FileNotFoundError(f"PCAP file not found: {path}")

        self._file_size = self._file_path.stat().st_size

        # Validate PCAP magic bytes
        with open(self._file_path, 'rb') as f:
            magic = f.read(4)

        # pcap: d4c3b2a1 or a1b2c3d4, pcapng: 0a0d0d0a
        valid_magic = {
            b'\xd4\xc3\xb2\xa1', b'\xa1\xb2\xc3\xd4',  # pcap LE/BE
            b'\x0a\x0d\x0d\x0a',  # pcapng
            b'\x4d\x3c\xb2\xa1', b'\xa1\xb2\x3c\x4d',  # pcap nanosecond
        }

        if magic not in valid_magic:
            raise ValueError("Not a valid PCAP/PCAPNG file")

        return True

    # Reasonable timestamp range for validation during corruption recovery
    _TS_MIN = 946684800   # 2000-01-01
    _TS_MAX = 2000000000  # 2033-05-18

    def parse(self, progress_callback: Optional[Callable] = None,
              cancel_check: Optional[Callable] = None) -> bool:
        """
        Stream-parse the PCAP file with corruption recovery.

        Reads raw pcap packet records directly. If corruption is detected
        (e.g., from hard power-down during capture), scans forward to find
        the next valid packet header and continues parsing.

        Args:
            progress_callback: Called with (bytes_read, total_bytes, stats_dict)
            cancel_check: Called to check if parsing should stop

        Returns:
            True if parsing completed successfully
        """
        import struct

        cancelled = False
        file_size = self._file_size

        with open(self._file_path, 'rb') as f:
            # Read and validate pcap global header (24 bytes)
            global_hdr = f.read(24)
            if len(global_hdr) < 24:
                raise ValueError("File too small for pcap header")

            magic = global_hdr[:4]
            if magic == b'\xd4\xc3\xb2\xa1':
                endian = '<'
            elif magic == b'\xa1\xb2\xc3\xd4':
                endian = '>'
            elif magic == b'\x0a\x0d\x0d\x0a':
                return self._parse_pcapng(f, progress_callback, cancel_check)
            elif magic in (b'\x4d\x3c\xb2\xa1', b'\xa1\xb2\x3c\x4d'):
                endian = '<'
            else:
                raise ValueError("Not a valid pcap file")

            snaplen = struct.unpack_from(endian + 'I', global_hdr, 16)[0]

            pkt_idx = 0

            while f.tell() < file_size:
                if cancel_check and pkt_idx % 5000 == 0 and cancel_check():
                    cancelled = True
                    break

                offset = f.tell()
                pkt_hdr = f.read(16)
                if len(pkt_hdr) < 16:
                    break

                ts_sec, ts_usec, incl_len, orig_len = struct.unpack(
                    endian + 'IIII', pkt_hdr)

                # Validate packet header
                if incl_len > snaplen or incl_len == 0 or orig_len > 65535:
                    # Corruption detected — scan forward to recover
                    recovery_pos = self._recover_from_corruption(
                        f, offset, file_size, snaplen, endian)
                    if recovery_pos is None:
                        break
                    f.seek(recovery_pos)
                    continue

                pkt_data = f.read(incl_len)
                if len(pkt_data) < incl_len:
                    break

                pkt_idx += 1
                self._total_packets = pkt_idx
                ts = ts_sec

                if ts < self._TS_MIN or ts > self._TS_MAX:
                    ts = 0

                # Parse RadioTap header
                signal = 0
                rt_channel = 0
                dot11_buf = pkt_data

                if len(pkt_data) >= 4 and pkt_data[0] == 0:
                    rt_len = struct.unpack_from('<H', pkt_data, 2)[0]
                    if rt_len <= len(pkt_data):
                        signal, rt_freq = self._parse_radiotap_fields(
                            pkt_data[:rt_len])
                        if rt_freq:
                            rt_channel = self._freq_to_channel(rt_freq)
                        dot11_buf = pkt_data[rt_len:]

                self._process_raw_packet(dot11_buf, ts, signal, rt_channel)

                if progress_callback and pkt_idx % 50000 == 0:
                    pos = f.tell()
                    stats = {
                        'packets': self._total_packets,
                        'aps': len(self._access_points),
                        'clients': len(self._clients),
                        'handshakes': sum(
                            1 for h in self._handshakes.values() if h.complete),
                        'deauths': self._deauth_total_count,
                    }
                    progress_callback(pos, self._file_size, stats)

        self._finalize()
        self._is_loaded = True
        return not cancelled

    @staticmethod
    def _recover_from_corruption(f, bad_offset: int, file_size: int,
                                  snaplen: int, endian: str) -> Optional[int]:
        """Manual byte-scan recovery when tshark is unavailable.

        Scans forward from a corruption point to find the next valid packet.
        Tries a narrow scan first (10KB), then a wider scan (1MB).
        """
        import struct

        # Estimate timestamp range from file position
        TS_MIN = 946684800
        TS_MAX = 2000000000

        for scan_limit in (10000, 1000000):
            end = min(bad_offset + scan_limit, file_size - 32)

            for pos in range(bad_offset + 1, end):
                f.seek(pos)
                candidate = f.read(16)
                if len(candidate) < 16:
                    break

                c_ts, c_us, c_incl, c_orig = struct.unpack(
                    endian + 'IIII', candidate)

                if (TS_MIN < c_ts < TS_MAX and c_us < 1000000 and
                        0 < c_incl <= snaplen and c_incl <= c_orig <= 65535):
                    next_pos = pos + 16 + c_incl
                    f.seek(next_pos)
                    nxt = f.read(16)
                    if len(nxt) == 16:
                        n_ts, n_us, n_incl, n_orig = struct.unpack(
                            endian + 'IIII', nxt)
                        if (TS_MIN < n_ts < TS_MAX and n_us < 1000000 and
                                0 < n_incl <= snaplen and
                                n_incl <= n_orig <= 65535):
                            return pos

        return None

    def _parse_pcapng(self, f, progress_callback, cancel_check) -> bool:
        """Fallback parser for pcapng files using dpkt."""
        import dpkt
        import struct

        f.seek(0)
        cancelled = False
        reader = dpkt.pcapng.Reader(f)

        for i, (ts_float, buf) in enumerate(reader):
            if cancel_check and i % 5000 == 0 and cancel_check():
                cancelled = True
                break

            self._total_packets = i + 1
            ts = int(ts_float)
            if ts < self._TS_MIN or ts > self._TS_MAX:
                ts = 0

            signal = 0
            rt_channel = 0
            dot11_buf = buf

            if len(buf) >= 4 and buf[0] == 0:
                rt_len = struct.unpack_from('<H', buf, 2)[0]
                if rt_len <= len(buf):
                    signal, rt_freq = self._parse_radiotap_fields(buf[:rt_len])
                    if rt_freq:
                        rt_channel = self._freq_to_channel(rt_freq)
                    dot11_buf = buf[rt_len:]

            self._process_raw_packet(dot11_buf, ts, signal, rt_channel)

            if progress_callback and i % 50000 == 0:
                pos = f.tell()
                stats = {
                    'packets': self._total_packets,
                    'aps': len(self._access_points),
                    'clients': len(self._clients),
                    'handshakes': sum(
                        1 for h in self._handshakes.values() if h.complete),
                    'deauths': self._deauth_total_count,
                }
                progress_callback(pos, self._file_size, stats)

        self._finalize()
        self._is_loaded = True
        return not cancelled

    @staticmethod
    def _parse_radiotap_fields(rt_buf: bytes) -> tuple[int, int]:
        """Extract dBm signal and frequency from RadioTap header.

        Returns:
            (signal_dbm, frequency_mhz) — either may be 0 if not present.
        """
        import struct
        if len(rt_buf) < 8:
            return 0, 0

        present = struct.unpack_from('<I', rt_buf, 4)[0]
        offset = 8

        # Handle extended present bitmasks
        extra_present = present
        while extra_present & (1 << 31):
            if offset + 4 > len(rt_buf):
                return 0, 0
            extra_present = struct.unpack_from('<I', rt_buf, offset)[0]
            offset += 4

        signal = 0
        freq = 0

        # Bit 0: TSFT (8 bytes, aligned to 8)
        if present & (1 << 0):
            offset = (offset + 7) & ~7
            offset += 8
        # Bit 1: Flags (1 byte)
        if present & (1 << 1):
            offset += 1
        # Bit 2: Rate (1 byte)
        if present & (1 << 2):
            offset += 1
        # Bit 3: Channel (2 byte freq + 2 byte flags, aligned to 2)
        if present & (1 << 3):
            offset = (offset + 1) & ~1
            if offset + 4 <= len(rt_buf):
                freq = struct.unpack_from('<H', rt_buf, offset)[0]
            offset += 4
        # Bit 4: FHSS (2 bytes)
        if present & (1 << 4):
            offset += 2
        # Bit 5: dBm Antenna Signal (1 byte, signed)
        if present & (1 << 5):
            if offset < len(rt_buf):
                signal = struct.unpack_from('b', rt_buf, offset)[0]

        return signal, freq

    @staticmethod
    def _mac_bytes_to_str(mac_bytes: bytes) -> str:
        """Convert 6-byte MAC to colon-separated string."""
        return ':'.join(f'{b:02x}' for b in mac_bytes)

    def _process_raw_packet(self, buf: bytes, ts: int, signal: int, rt_channel: int = 0):
        """Process raw 802.11 frame bytes."""
        if len(buf) < 2:
            self._frame_type_counts['Non-802.11'] = \
                self._frame_type_counts.get('Non-802.11', 0) + 1
            return

        # Frame Control field (2 bytes, little-endian)
        fc = buf[0] | (buf[1] << 8)
        frame_type = (fc >> 2) & 0x3
        frame_subtype = (fc >> 4) & 0xf
        to_ds = bool(fc & 0x100)
        from_ds = bool(fc & 0x200)

        # Track frame types
        type_names = {0: 'Management', 1: 'Control', 2: 'Data'}
        type_name = type_names.get(frame_type, f'Type-{frame_type}')
        self._frame_type_counts[type_name] = \
            self._frame_type_counts.get(type_name, 0) + 1

        subtype_name = self._get_subtype_name(frame_type, frame_subtype)
        self._frame_subtype_counts[subtype_name] = \
            self._frame_subtype_counts.get(subtype_name, 0) + 1

        # Update timeline (skip invalid timestamps)
        if ts > 0:
            if ts not in self._packet_timeline:
                self._packet_timeline[ts] = TimelineBucket()
            bucket = self._packet_timeline[ts]
            bucket.packet_count += 1
            if signal != 0 and signal > -100:
                bucket.signal_sum += signal
                bucket.signal_count += 1

        # Control frames have shorter headers, skip detailed parsing
        if frame_type == 1:
            return

        # Need at least 24 bytes for management/data frame header
        if len(buf) < 24:
            return

        # Extract MAC addresses (bytes 4-9, 10-15, 16-21)
        addr1 = self._mac_bytes_to_str(buf[4:10])
        addr2 = self._mac_bytes_to_str(buf[10:16])
        addr3 = self._mac_bytes_to_str(buf[16:22])

        # Route by frame type
        if frame_type == 0:  # Management
            # Management frame body starts after 24-byte header
            body = buf[24:]
            if frame_subtype == 8:     # Beacon
                self._process_mgmt_beacon(addr2, body, ts, signal, rt_channel)
            elif frame_subtype == 4:   # Probe Request
                self._process_mgmt_probe_req(addr2, body, ts, signal)
            elif frame_subtype == 5:   # Probe Response
                self._process_mgmt_probe_resp(addr2, body, ts, signal, rt_channel)
            elif frame_subtype == 0:   # Association Request
                self._process_mgmt_assoc(addr2, addr1, ts, signal)
            elif frame_subtype == 12:  # Deauthentication
                self._process_mgmt_deauth(addr2, addr1, addr3, body, ts, 'deauth')
            elif frame_subtype == 10:  # Disassociation
                self._process_mgmt_deauth(addr2, addr1, addr3, body, ts, 'disassoc')
        elif frame_type == 2:  # Data
            self._process_raw_data(addr1, addr2, addr3, to_ds, from_ds, buf, ts, signal)

    def _process_mgmt_beacon(self, bssid: str, body: bytes, ts: int, signal: int,
                              rt_channel: int = 0):
        """Process beacon frame body."""
        if _is_broadcast_or_multicast(bssid):
            return

        # Beacon body: timestamp(8) + interval(2) + capability(2) + IEs
        if len(body) < 12:
            return

        cap_info = body[10] | (body[11] << 8)
        ie_data = body[12:]

        ssid = self._parse_ie_ssid(ie_data)
        channel = self._parse_ie_channel(ie_data) or rt_channel
        encryption = self._parse_ie_encryption(ie_data, cap_info)

        if bssid not in self._access_points:
            self._access_points[bssid] = APRecord(
                bssid=bssid,
                ssid=ssid,
                channel=channel,
                encryption=encryption,
                manufacturer=lookup_manufacturer(bssid),
            )

        ap = self._access_points[bssid]
        ap.update_time(ts)
        ap.update_signal(signal)
        ap.beacon_count += 1
        ap.packet_count += 1

        if ssid and not ap.ssid:
            ap.ssid = ssid
        if channel and not ap.channel:
            ap.channel = channel
        if encryption != "Unknown" and ap.encryption == "Unknown":
            ap.encryption = encryption

    def _process_mgmt_probe_req(self, client_mac: str, body: bytes, ts: int, signal: int):
        """Process probe request frame body."""
        if _is_broadcast_or_multicast(client_mac):
            return

        ssid = self._parse_ie_ssid(body) if len(body) >= 2 else ""

        if client_mac not in self._clients:
            self._clients[client_mac] = ClientRecord(
                mac=client_mac,
                manufacturer=lookup_manufacturer(client_mac),
            )

        client = self._clients[client_mac]
        client.update_time(ts)
        client.update_signal(signal)
        client.packet_count += 1

        if ssid and ssid not in client.probed_ssids:
            client.probed_ssids.append(ssid)

        self._probe_requests.append(ProbeRecord(
            client_mac=client_mac,
            ssid=ssid if ssid else "<broadcast>",
            timestamp=ts,
            signal=signal,
        ))

    def _process_mgmt_probe_resp(self, bssid: str, body: bytes, ts: int, signal: int,
                                  rt_channel: int = 0):
        """Process probe response frame body."""
        if _is_broadcast_or_multicast(bssid):
            return

        if len(body) < 12:
            return

        cap_info = body[10] | (body[11] << 8)
        ie_data = body[12:]

        ssid = self._parse_ie_ssid(ie_data)
        channel = self._parse_ie_channel(ie_data) or rt_channel
        encryption = self._parse_ie_encryption(ie_data, cap_info)

        if bssid not in self._access_points:
            self._access_points[bssid] = APRecord(
                bssid=bssid,
                ssid=ssid,
                channel=channel,
                encryption=encryption,
                manufacturer=lookup_manufacturer(bssid),
            )

        ap = self._access_points[bssid]
        ap.update_time(ts)
        ap.update_signal(signal)
        ap.packet_count += 1

        if ssid and not ap.ssid:
            ap.ssid = ssid
        if channel and not ap.channel:
            ap.channel = channel
        if encryption != "Unknown" and ap.encryption == "Unknown":
            ap.encryption = encryption

    def _process_mgmt_assoc(self, client_mac: str, bssid: str, ts: int, signal: int):
        """Process association request."""
        if _is_broadcast_or_multicast(client_mac):
            return

        if client_mac not in self._clients:
            self._clients[client_mac] = ClientRecord(
                mac=client_mac,
                manufacturer=lookup_manufacturer(client_mac),
            )

        client = self._clients[client_mac]
        client.update_time(ts)
        client.update_signal(signal)
        client.last_bssid = bssid
        client.associated_aps.add(bssid)
        client.packet_count += 1

        if bssid in self._access_points:
            self._access_points[bssid].clients.add(client_mac)

    def _process_mgmt_deauth(self, source: str, dest: str, bssid: str,
                              body: bytes, ts: int, subtype: str):
        """Process deauth/disassociation frame."""
        reason = 0
        if len(body) >= 2:
            reason = body[0] | (body[1] << 8)

        if not bssid:
            bssid = source

        self._deauth_total_count += 1

        if len(self._deauths) < self.MAX_DEAUTH_RECORDS:
            self._deauths.append(DeauthRecord(
                source_mac=source,
                dest_mac=dest,
                bssid=bssid,
                timestamp=ts,
                reason_code=reason,
                subtype=subtype,
            ))

    def _process_raw_data(self, addr1: str, addr2: str, addr3: str,
                          to_ds: bool, from_ds: bool, buf: bytes,
                          ts: int, signal: int):
        """Process data frame for client-AP tracking and EAPOL detection."""
        bssid = None
        client_mac = None

        if to_ds and not from_ds:
            bssid = addr1
            client_mac = addr2
        elif not to_ds and from_ds:
            bssid = addr2
            client_mac = addr1
        elif to_ds and from_ds:
            return  # WDS
        else:
            bssid = addr3
            client_mac = addr2

        if not bssid or not client_mac:
            return
        if _is_broadcast_or_multicast(client_mac):
            return
        if _is_broadcast_or_multicast(bssid):
            return

        # Update AP
        if bssid in self._access_points:
            ap = self._access_points[bssid]
            ap.update_time(ts)
            ap.data_count += 1
            ap.packet_count += 1
            ap.clients.add(client_mac)

        # Update client
        if client_mac not in self._clients:
            self._clients[client_mac] = ClientRecord(
                mac=client_mac,
                manufacturer=lookup_manufacturer(client_mac),
            )

        client = self._clients[client_mac]
        client.update_time(ts)
        client.update_signal(signal)
        client.last_bssid = bssid
        client.associated_aps.add(bssid)
        client.packet_count += 1

        # Check for EAPOL (802.1X authentication / WPA handshake)
        # Data frame header is 24 bytes (or 30 with addr4 for WDS)
        # After that: LLC/SNAP header (8 bytes) with ethertype
        hdr_len = 24
        # Check QoS (subtype bit 3)
        fc = buf[0] | (buf[1] << 8)
        if (fc >> 4) & 0xf >= 8:  # QoS subtypes are 8+
            hdr_len += 2

        if len(buf) >= hdr_len + 8:
            llc = buf[hdr_len:hdr_len + 8]
            # LLC/SNAP: AA AA 03 00 00 00 [ethertype]
            if llc[:6] == b'\xaa\xaa\x03\x00\x00\x00':
                ethertype = (llc[6] << 8) | llc[7]
                if ethertype == 0x888e:  # EAPOL
                    eapol_data = buf[hdr_len + 8:]
                    self._process_raw_eapol(bssid, client_mac, eapol_data, ts)

    def _process_raw_eapol(self, bssid: str, client_mac: str,
                           eapol_data: bytes, ts: int):
        """Detect WPA 4-way handshake from raw EAPOL bytes."""
        # EAPOL: version(1) + type(1) + length(2) + key_descriptor(1) + key_info(2)
        if len(eapol_data) < 7:
            return

        key_info = (eapol_data[5] << 8) | eapol_data[6]

        pairwise = bool(key_info & 0x0008)
        install = bool(key_info & 0x0040)
        ack = bool(key_info & 0x0080)
        mic = bool(key_info & 0x0100)

        msg_num = 0
        if pairwise and ack and not mic:
            msg_num = 1
        elif pairwise and mic and not ack and not install:
            msg_num = 2
        elif pairwise and ack and mic and install:
            msg_num = 3
        elif pairwise and mic and not ack:
            msg_num = 4

        if msg_num == 0:
            return

        key = (bssid, client_mac)
        if key not in self._handshakes:
            self._handshakes[key] = HandshakeRecord(
                bssid=bssid,
                client_mac=client_mac,
                timestamp=ts,
            )

        self._handshakes[key].eapol_messages.add(msg_num)

    @staticmethod
    def _parse_ie_ssid(ie_data: bytes) -> str:
        """Extract SSID from information elements."""
        offset = 0
        while offset + 2 <= len(ie_data):
            ie_id = ie_data[offset]
            ie_len = ie_data[offset + 1]
            if offset + 2 + ie_len > len(ie_data):
                break
            if ie_id == 0:  # SSID
                try:
                    return ie_data[offset + 2:offset + 2 + ie_len].decode('utf-8', errors='ignore').strip('\x00')
                except Exception:
                    return ""
            offset += 2 + ie_len
        return ""

    @staticmethod
    def _parse_ie_channel(ie_data: bytes) -> int:
        """Extract channel from DS Parameter Set IE."""
        offset = 0
        while offset + 2 <= len(ie_data):
            ie_id = ie_data[offset]
            ie_len = ie_data[offset + 1]
            if offset + 2 + ie_len > len(ie_data):
                break
            if ie_id == 3 and ie_len >= 1:  # DS Parameter Set
                return ie_data[offset + 2]
            offset += 2 + ie_len
        return 0

    @staticmethod
    def _parse_ie_encryption(ie_data: bytes, cap_info: int) -> str:
        """Determine encryption from IEs and capability info."""
        crypto = set()
        offset = 0

        while offset + 2 <= len(ie_data):
            ie_id = ie_data[offset]
            ie_len = ie_data[offset + 1]
            if offset + 2 + ie_len > len(ie_data):
                break

            ie_body = ie_data[offset + 2:offset + 2 + ie_len]

            if ie_id == 48:  # RSN (WPA2/WPA3)
                crypto.add('WPA2')
                if b'\x00\x0f\xac\x08' in ie_body:  # SAE
                    crypto.add('WPA3')
            elif ie_id == 221 and ie_len >= 4:  # Vendor-specific
                if ie_body[:4] == b'\x00\x50\xf2\x01':  # WPA1 OUI
                    crypto.add('WPA')

            offset += 2 + ie_len

        if not crypto:
            if cap_info & 0x0010:  # Privacy bit
                return 'WEP'
            return 'Open'

        if 'WPA3' in crypto:
            return 'WPA3'
        if 'WPA2' in crypto:
            return 'WPA2'
        if 'WPA' in crypto:
            return 'WPA'
        return 'Open'

    @staticmethod
    def _freq_to_channel(freq: int) -> int:
        """Convert frequency (MHz) to channel number."""
        if freq == 2484:
            return 14
        if 2412 <= freq <= 2472:
            return (freq - 2412) // 5 + 1
        if 5180 <= freq <= 5825:
            return (freq - 5000) // 5
        return 0

    @staticmethod
    def _get_subtype_name(frame_type: int, subtype: int) -> str:
        """Get human-readable name for frame type/subtype."""
        mgmt_subtypes = {
            0: 'Association Request', 1: 'Association Response',
            2: 'Reassociation Request', 3: 'Reassociation Response',
            4: 'Probe Request', 5: 'Probe Response',
            8: 'Beacon', 9: 'ATIM',
            10: 'Disassociation', 11: 'Authentication',
            12: 'Deauthentication', 13: 'Action',
        }
        data_subtypes = {
            0: 'Data', 4: 'Null', 8: 'QoS Data', 12: 'QoS Null',
        }

        if frame_type == 0:
            return mgmt_subtypes.get(subtype, f'Mgmt-{subtype}')
        elif frame_type == 1:
            return f'Control-{subtype}'
        elif frame_type == 2:
            return data_subtypes.get(subtype, f'Data-{subtype}')
        return f'Unknown-{frame_type}-{subtype}'

    def _finalize(self):
        """Post-processing after parsing is complete."""
        # Remove clients that are also known APs (some APs probe too)
        ap_macs = set(self._access_points.keys())
        client_only = {k: v for k, v in self._clients.items() if k not in ap_macs}
        self._clients = client_only

    # ---- Companion GPS data ----

    def load_wigle_gps(self, csv_paths: list[str]):
        """Load GPS coordinates from companion WiGLE CSV files.

        Pineapple loot zips store GPS data in separate WiGLE CSV files
        alongside the PCAP. This merges that GPS data into our records.
        """
        import csv

        self._wigle_gps.clear()

        for csv_path in csv_paths:
            try:
                with open(csv_path, 'r') as f:
                    first_line = f.readline()
                    if not first_line.startswith('WigleWifi'):
                        continue
                    reader = csv.DictReader(f)
                    for row in reader:
                        mac = (row.get('MAC', '') or '').lower()
                        if not mac:
                            continue
                        lat = float(row.get('CurrentLatitude', 0) or 0)
                        lon = float(row.get('CurrentLongitude', 0) or 0)
                        if lat == 0 and lon == 0:
                            continue

                        if mac not in self._wigle_gps:
                            self._wigle_gps[mac] = {
                                'min_lat': lat, 'max_lat': lat,
                                'min_lon': lon, 'max_lon': lon,
                                'alt': float(row.get('AltitudeMeters', 0) or 0),
                                'ssid': (row.get('SSID', '') or '').strip('"'),
                            }
                        else:
                            d = self._wigle_gps[mac]
                            if lat < d['min_lat']:
                                d['min_lat'] = lat
                            if lat > d['max_lat']:
                                d['max_lat'] = lat
                            if lon < d['min_lon']:
                                d['min_lon'] = lon
                            if lon > d['max_lon']:
                                d['max_lon'] = lon
            except Exception:
                continue

    def has_gps_data(self) -> bool:
        """Check if companion GPS data has been loaded."""
        return bool(self._wigle_gps)

    def _get_gps_for_mac(self, mac: str) -> tuple[float, float, float, float]:
        """Get GPS bounds for a MAC address. Returns (min_lat, min_lon, max_lat, max_lon)."""
        gps = self._wigle_gps.get(mac.lower())
        if gps:
            return gps['min_lat'], gps['min_lon'], gps['max_lat'], gps['max_lon']
        return 0, 0, 0, 0

    # ---- CaptureReader Protocol Implementation ----

    def close_database(self):
        """Reset all state."""
        self._file_path = None
        self._is_loaded = False
        self._access_points.clear()
        self._clients.clear()
        self._handshakes.clear()
        self._deauths.clear()
        self._deauth_total_count = 0
        self._probe_requests.clear()
        self._packet_timeline.clear()
        self._frame_type_counts.clear()
        self._frame_subtype_counts.clear()
        self._total_packets = 0
        self._wigle_gps.clear()

    def is_connected(self) -> bool:
        return self._is_loaded

    def get_database_info(self) -> dict:
        if not self._is_loaded:
            return {}
        return {
            'path': str(self._file_path),
            'filename': self._file_path.name if self._file_path else '',
            'size_bytes': self._file_size,
            'tables': ['pcap_data'],
            'total_packets': self._total_packets,
            'source_type': 'pcap',
        }

    def get_device_summary(self) -> dict:
        if not self._is_loaded:
            return {}

        # Calculate time range
        all_times = []
        for ap in self._access_points.values():
            if ap.first_time:
                all_times.append(ap.first_time)
            if ap.last_time:
                all_times.append(ap.last_time)
        for cl in self._clients.values():
            if cl.first_time:
                all_times.append(cl.first_time)
            if cl.last_time:
                all_times.append(cl.last_time)

        earliest = min(all_times) if all_times else None
        latest = max(all_times) if all_times else None

        # GPS bounds from companion WiGLE data
        gps_count = 0
        all_lats = []
        all_lons = []
        if self._wigle_gps:
            for mac, gps in self._wigle_gps.items():
                gps_count += 1
                all_lats.extend([gps['min_lat'], gps['max_lat']])
                all_lons.extend([gps['min_lon'], gps['max_lon']])

        return {
            'total_devices': len(self._access_points) + len(self._clients),
            'by_phy_type': {
                'IEEE802.11': len(self._access_points) + len(self._clients),
            },
            'time_range': {
                'earliest': datetime.fromtimestamp(earliest) if earliest else None,
                'latest': datetime.fromtimestamp(latest) if latest else None,
            },
            'geographic_bounds': {
                'min_lat': min(all_lats) if all_lats else 0,
                'max_lat': max(all_lats) if all_lats else 0,
                'min_lon': min(all_lons) if all_lons else 0,
                'max_lon': max(all_lons) if all_lons else 0,
                'unique_devices_with_gps': gps_count,
            },
        }

    def get_access_points(self, filters: Optional[dict] = None) -> pd.DataFrame:
        if not self._is_loaded:
            return pd.DataFrame()

        records = []
        for ap in self._access_points.values():
            min_lat, min_lon, max_lat, max_lon = self._get_gps_for_mac(ap.bssid)
            records.append({
                'devmac': ap.bssid,
                'name': ap.ssid,
                'commonname': '',
                'channel': str(ap.channel) if ap.channel else '',
                'manufacturer': ap.manufacturer,
                'encryption': ap.encryption,
                'strongest_signal': ap.strongest_signal if ap.strongest_signal > -100 else None,
                'first_time': pd.Timestamp(ap.first_time, unit='s') if ap.first_time else pd.NaT,
                'last_time': pd.Timestamp(ap.last_time, unit='s') if ap.last_time else pd.NaT,
                'min_lat': min_lat, 'min_lon': min_lon,
                'max_lat': max_lat, 'max_lon': max_lon,
                'type': 'Wi-Fi AP',
                'device': None,
                'beacon_count': ap.beacon_count,
                'data_count': ap.data_count,
                'client_count': len(ap.clients),
            })

        df = pd.DataFrame(records)
        if filters and not df.empty:
            df = self._apply_filters(df, filters)
        return df

    def get_clients(self, filters: Optional[dict] = None) -> pd.DataFrame:
        if not self._is_loaded:
            return pd.DataFrame()

        records = []
        for cl in self._clients.values():
            # Look up last AP's SSID
            last_ssid = ''
            if cl.last_bssid and cl.last_bssid in self._access_points:
                last_ssid = self._access_points[cl.last_bssid].ssid

            min_lat, min_lon, _, _ = self._get_gps_for_mac(cl.mac)
            records.append({
                'client_mac': cl.mac,
                'name': last_ssid,
                'commonname': '',
                'manufacturer': cl.manufacturer,
                'last_bssid': cl.last_bssid,
                'strongest_signal': cl.strongest_signal if cl.strongest_signal > -100 else None,
                'first_time': pd.Timestamp(cl.first_time, unit='s') if cl.first_time else pd.NaT,
                'last_time': pd.Timestamp(cl.last_time, unit='s') if cl.last_time else pd.NaT,
                'min_lat': min_lat, 'min_lon': min_lon,
                'type': 'Wi-Fi Client',
                'device': None,
                'probed_ssids': ', '.join(cl.probed_ssids) if cl.probed_ssids else '',
                'associated_aps': len(cl.associated_aps),
                'randomized_mac': is_randomized_mac(cl.mac),
            })

        df = pd.DataFrame(records)
        if filters and not df.empty:
            df = self._apply_filters(df, filters)
        return df

    def get_all_devices(self, filters: Optional[dict] = None) -> pd.DataFrame:
        if not self._is_loaded:
            return pd.DataFrame()

        records = []
        for ap in self._access_points.values():
            min_lat, min_lon, max_lat, max_lon = self._get_gps_for_mac(ap.bssid)
            records.append({
                'devmac': ap.bssid,
                'phyname': 'IEEE802.11',
                'type': 'Wi-Fi AP',
                'name': ap.ssid,
                'commonname': '',
                'manufacturer': ap.manufacturer,
                'channel': str(ap.channel) if ap.channel else '',
                'strongest_signal': ap.strongest_signal if ap.strongest_signal > -100 else None,
                'first_time': pd.Timestamp(ap.first_time, unit='s') if ap.first_time else pd.NaT,
                'last_time': pd.Timestamp(ap.last_time, unit='s') if ap.last_time else pd.NaT,
                'min_lat': min_lat, 'min_lon': min_lon,
                'max_lat': max_lat, 'max_lon': max_lon,
            })
        for cl in self._clients.values():
            min_lat, min_lon, max_lat, max_lon = self._get_gps_for_mac(cl.mac)
            records.append({
                'devmac': cl.mac,
                'phyname': 'IEEE802.11',
                'type': 'Wi-Fi Client',
                'name': '',
                'commonname': '',
                'manufacturer': cl.manufacturer,
                'channel': '',
                'strongest_signal': cl.strongest_signal if cl.strongest_signal > -100 else None,
                'first_time': pd.Timestamp(cl.first_time, unit='s') if cl.first_time else pd.NaT,
                'last_time': pd.Timestamp(cl.last_time, unit='s') if cl.last_time else pd.NaT,
                'min_lat': min_lat, 'min_lon': min_lon,
                'max_lat': max_lat, 'max_lon': max_lon,
            })

        df = pd.DataFrame(records)
        if filters and not df.empty:
            df = self._apply_filters(df, filters)
        return df

    def get_bluetooth_devices(self, filters: Optional[dict] = None) -> pd.DataFrame:
        """PCAP from Pineapple is 802.11 only."""
        return pd.DataFrame()

    def get_networks(self) -> pd.DataFrame:
        if not self._is_loaded:
            return pd.DataFrame()

        ssid_counts: dict[str, int] = {}
        for ap in self._access_points.values():
            if ap.ssid:
                ssid_counts[ap.ssid] = ssid_counts.get(ap.ssid, 0) + 1

        records = [{'ssid': ssid, 'ap_count': count}
                   for ssid, count in sorted(ssid_counts.items(),
                                             key=lambda x: x[1], reverse=True)]
        return pd.DataFrame(records)

    def get_gps_data(self) -> pd.DataFrame:
        """GPS data from companion WiGLE CSV files."""
        if not self._wigle_gps:
            return pd.DataFrame()

        records = []
        for mac, gps in self._wigle_gps.items():
            avg_lat = (gps['min_lat'] + gps['max_lat']) / 2
            avg_lon = (gps['min_lon'] + gps['max_lon']) / 2
            records.append({
                'devmac': mac,
                'lat': avg_lat,
                'lon': avg_lon,
                'alt': gps.get('alt', 0),
                'name': gps.get('ssid', ''),
            })
        return pd.DataFrame(records)

    def get_device_gps_track(self, devmac: str) -> pd.DataFrame:
        """GPS bounds from companion WiGLE data (no true track)."""
        gps = self._wigle_gps.get(devmac.lower())
        if not gps:
            return pd.DataFrame()

        records = []
        if gps['min_lat'] != gps['max_lat'] or gps['min_lon'] != gps['max_lon']:
            records.append({'lat': gps['min_lat'], 'lon': gps['min_lon'], 'alt': gps.get('alt', 0)})
            records.append({'lat': gps['max_lat'], 'lon': gps['max_lon'], 'alt': gps.get('alt', 0)})
        else:
            records.append({'lat': gps['min_lat'], 'lon': gps['min_lon'], 'alt': gps.get('alt', 0)})
        return pd.DataFrame(records)

    def get_data_sources(self) -> pd.DataFrame:
        if not self._is_loaded:
            return pd.DataFrame()

        return pd.DataFrame([{
            'uuid': 'pcap-import',
            'typestring': 'pcap',
            'definition': str(self._file_path),
            'name': self._file_path.name if self._file_path else '',
            'interface': 'file',
        }])

    def get_alerts(self) -> pd.DataFrame:
        """Generate alerts from deauth patterns."""
        if not self._is_loaded:
            return pd.DataFrame()

        alerts = []
        # Flag deauth floods as alerts
        if self._deauth_total_count > 100:
            # Find top targeted BSSIDs
            target_counts: dict[str, int] = {}
            for d in self._deauths:
                target_counts[d.bssid] = target_counts.get(d.bssid, 0) + 1

            for bssid, count in sorted(target_counts.items(),
                                       key=lambda x: x[1], reverse=True)[:5]:
                if count > 10:
                    alerts.append({
                        'ts_sec': self._deauths[0].timestamp if self._deauths else 0,
                        'ts_usec': 0,
                        'phyname': 'IEEE802.11',
                        'devmac': bssid,
                        'lat': 0, 'lon': 0,
                        'header': f'Deauth flood detected ({count} frames)',
                        'json': '',
                        'timestamp': pd.Timestamp(self._deauths[0].timestamp, unit='s') if self._deauths else pd.NaT,
                    })

        return pd.DataFrame(alerts)

    def get_packets_timeline(self) -> pd.DataFrame:
        if not self._is_loaded or not self._packet_timeline:
            return pd.DataFrame()

        records = []
        for ts, bucket in sorted(self._packet_timeline.items()):
            records.append({
                'ts_sec': ts,
                'packet_count': bucket.packet_count,
                'avg_signal': bucket.avg_signal,
                'timestamp': pd.Timestamp(ts, unit='s'),
            })

        return pd.DataFrame(records)

    def get_signal_distribution(self) -> pd.DataFrame:
        if not self._is_loaded:
            return pd.DataFrame()

        signal_buckets: dict[int, int] = {}
        for ap in self._access_points.values():
            if ap.strongest_signal and ap.strongest_signal > -100:
                bucket = (ap.strongest_signal // 10) * 10
                signal_buckets[bucket] = signal_buckets.get(bucket, 0) + 1
        for cl in self._clients.values():
            if cl.strongest_signal and cl.strongest_signal > -100:
                bucket = (cl.strongest_signal // 10) * 10
                signal_buckets[bucket] = signal_buckets.get(bucket, 0) + 1

        records = [{'signal_bucket': k, 'count': v}
                   for k, v in sorted(signal_buckets.items())]
        return pd.DataFrame(records)

    # ---- PCAP-specific methods ----

    def get_handshakes(self) -> pd.DataFrame:
        if not self._is_loaded:
            return pd.DataFrame()

        records = []
        for hs in self._handshakes.values():
            ssid = ''
            if hs.bssid in self._access_points:
                ssid = self._access_points[hs.bssid].ssid

            client_manuf = lookup_manufacturer(hs.client_mac)

            records.append({
                'bssid': hs.bssid,
                'ssid': ssid,
                'client_mac': hs.client_mac,
                'client_manufacturer': client_manuf,
                'timestamp': pd.Timestamp(hs.timestamp, unit='s'),
                'messages': ', '.join(str(m) for m in sorted(hs.eapol_messages)),
                'complete': hs.complete,
            })

        return pd.DataFrame(records)

    def get_deauth_frames(self) -> pd.DataFrame:
        if not self._is_loaded:
            return pd.DataFrame()

        records = []
        for d in self._deauths:
            records.append({
                'timestamp': pd.Timestamp(d.timestamp, unit='s'),
                'source_mac': d.source_mac,
                'dest_mac': d.dest_mac,
                'bssid': d.bssid,
                'reason_code': d.reason_code,
                'reason_text': d.reason_text,
                'subtype': d.subtype,
            })

        df = pd.DataFrame(records)
        # Add total count as metadata attribute
        if not df.empty:
            df.attrs['total_deauth_count'] = self._deauth_total_count
        return df

    def get_probe_requests(self) -> pd.DataFrame:
        if not self._is_loaded:
            return pd.DataFrame()

        # Aggregate by client MAC
        client_probes: dict[str, dict] = {}
        for pr in self._probe_requests:
            if pr.client_mac not in client_probes:
                client_probes[pr.client_mac] = {
                    'client_mac': pr.client_mac,
                    'manufacturer': lookup_manufacturer(pr.client_mac),
                    'randomized': is_randomized_mac(pr.client_mac),
                    'ssids': set(),
                    'count': 0,
                    'first_seen': pr.timestamp,
                    'last_seen': pr.timestamp,
                    'strongest_signal': pr.signal,
                }
            entry = client_probes[pr.client_mac]
            if pr.ssid:
                entry['ssids'].add(pr.ssid)
            entry['count'] += 1
            if pr.timestamp < entry['first_seen']:
                entry['first_seen'] = pr.timestamp
            if pr.timestamp > entry['last_seen']:
                entry['last_seen'] = pr.timestamp
            if pr.signal > entry['strongest_signal']:
                entry['strongest_signal'] = pr.signal

        records = []
        for entry in sorted(client_probes.values(),
                           key=lambda x: x['count'], reverse=True):
            records.append({
                'client_mac': entry['client_mac'],
                'manufacturer': entry['manufacturer'],
                'randomized': entry['randomized'],
                'probed_ssids': ', '.join(sorted(entry['ssids'])),
                'probe_count': entry['count'],
                'first_seen': pd.Timestamp(entry['first_seen'], unit='s'),
                'last_seen': pd.Timestamp(entry['last_seen'], unit='s'),
                'strongest_signal': entry['strongest_signal'] if entry['strongest_signal'] > -100 else None,
            })

        return pd.DataFrame(records)

    def get_frame_type_distribution(self) -> pd.DataFrame:
        if not self._is_loaded:
            return pd.DataFrame()

        records = []
        # High-level types
        for frame_type, count in sorted(self._frame_type_counts.items()):
            records.append({
                'category': 'type',
                'name': frame_type,
                'count': count,
            })
        # Subtypes
        for subtype_name, count in sorted(self._frame_subtype_counts.items(),
                                          key=lambda x: x[1], reverse=True):
            records.append({
                'category': 'subtype',
                'name': subtype_name,
                'count': count,
            })

        return pd.DataFrame(records)

    def has_pcap_features(self) -> bool:
        return True

    # ---- Filter support ----

    def _apply_filters(self, df: pd.DataFrame, filters: dict) -> pd.DataFrame:
        """Apply filter dict to a DataFrame."""
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
                mask &= df['channel'].isin([str(c) for c in filters['channels']])

        return df[mask].reset_index(drop=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_database()
        return False
