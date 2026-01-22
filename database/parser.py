"""JSON field parser for Kismet device data."""

import json
from typing import Any, Optional


class KismetParser:
    """Parser for Kismet JSON device data."""

    @staticmethod
    def parse_device_json(device_blob: bytes | str) -> dict:
        """Parse the device JSON blob from the database."""
        if device_blob is None:
            return {}

        try:
            if isinstance(device_blob, bytes):
                device_blob = device_blob.decode('utf-8')
            return json.loads(device_blob)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return {}

    @staticmethod
    def get_nested_value(data: dict, path: str, default: Any = None) -> Any:
        """
        Get a nested value from a dictionary using dot notation.

        Args:
            data: Dictionary to search
            path: Dot-separated path (e.g., 'kismet.device.base.name')
            default: Default value if path not found

        Returns:
            Value at path or default
        """
        keys = path.split('.')
        current = data

        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return default

        return current

    @staticmethod
    def get_device_name(device_data: dict) -> Optional[str]:
        """Extract device name from parsed device data."""
        return KismetParser.get_nested_value(
            device_data, 'kismet.device.base.name'
        )

    @staticmethod
    def get_device_type(device_data: dict) -> Optional[str]:
        """Extract device type from parsed device data."""
        return KismetParser.get_nested_value(
            device_data, 'kismet.device.base.type'
        )

    @staticmethod
    def get_manufacturer(device_data: dict) -> Optional[str]:
        """Extract manufacturer from parsed device data."""
        return KismetParser.get_nested_value(
            device_data, 'kismet.device.base.manuf'
        )

    @staticmethod
    def get_channel(device_data: dict) -> Optional[str]:
        """Extract channel from parsed device data."""
        return KismetParser.get_nested_value(
            device_data, 'kismet.device.base.channel'
        )

    @staticmethod
    def get_frequency(device_data: dict) -> Optional[int]:
        """Extract frequency from parsed device data."""
        return KismetParser.get_nested_value(
            device_data, 'kismet.device.base.frequency'
        )

    @staticmethod
    def get_ssid(device_data: dict) -> Optional[str]:
        """Extract SSID from 802.11 device data."""
        return KismetParser.get_nested_value(
            device_data, 'dot11.device.last_beaconed_ssid'
        )

    @staticmethod
    def get_encryption_type(device_data: dict) -> str:
        """Determine encryption type from device data."""
        crypt_string = KismetParser.get_nested_value(
            device_data, 'dot11.device.last_beaconed_ssid_crypt'
        )

        if crypt_string is None:
            return 'Unknown'

        crypt_string = str(crypt_string).upper()

        if 'WPA3' in crypt_string:
            return 'WPA3'
        elif 'WPA2' in crypt_string:
            return 'WPA2'
        elif 'WPA' in crypt_string:
            return 'WPA'
        elif 'WEP' in crypt_string:
            return 'WEP'
        elif crypt_string == '' or 'NONE' in crypt_string or 'OPEN' in crypt_string:
            return 'Open'
        else:
            return crypt_string

    @staticmethod
    def get_typeset(device_data: dict) -> int:
        """Get the 802.11 typeset bitmask."""
        return KismetParser.get_nested_value(
            device_data, 'dot11.device.typeset', 0
        )

    @staticmethod
    def is_access_point(device_data: dict) -> bool:
        """Check if device is an access point."""
        typeset = KismetParser.get_typeset(device_data)
        return bool(typeset & 1)

    @staticmethod
    def is_client(device_data: dict) -> bool:
        """Check if device is a client."""
        typeset = KismetParser.get_typeset(device_data)
        return bool(typeset & 2)

    @staticmethod
    def get_client_associations(device_data: dict) -> list:
        """Get list of associated access points for a client."""
        associations = KismetParser.get_nested_value(
            device_data, 'dot11.device.associated_client_map', {}
        )
        if isinstance(associations, dict):
            return list(associations.keys())
        return []

    @staticmethod
    def get_probed_ssids(device_data: dict) -> list:
        """Get list of probed SSIDs from a client device."""
        probed = KismetParser.get_nested_value(
            device_data, 'dot11.device.probed_ssid_map', {}
        )
        if isinstance(probed, dict):
            ssids = []
            for ssid_data in probed.values():
                if isinstance(ssid_data, dict):
                    ssid = ssid_data.get('dot11.probedssid.ssid', '')
                    if ssid:
                        ssids.append(ssid)
            return ssids
        return []

    @staticmethod
    def get_packet_counts(device_data: dict) -> dict:
        """Get packet count statistics."""
        return {
            'packets_total': KismetParser.get_nested_value(
                device_data, 'kismet.device.base.packets.total', 0
            ),
            'packets_data': KismetParser.get_nested_value(
                device_data, 'kismet.device.base.packets.data', 0
            ),
            'packets_llc': KismetParser.get_nested_value(
                device_data, 'kismet.device.base.packets.llc', 0
            ),
        }

    @staticmethod
    def format_mac_address(mac: str) -> str:
        """Format MAC address to standard format."""
        if mac is None:
            return ''
        # Remove any existing separators and convert to uppercase
        mac_clean = mac.replace(':', '').replace('-', '').upper()
        # Insert colons every 2 characters
        if len(mac_clean) == 12:
            return ':'.join(mac_clean[i:i+2] for i in range(0, 12, 2))
        return mac
