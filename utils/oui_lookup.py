"""OUI (Organizationally Unique Identifier) lookup for MAC addresses."""

import re
from typing import Optional

# Common OUI prefixes for quick lookup without external database
# Format: First 6 hex digits (no separators) -> Manufacturer
COMMON_OUI = {
    # Apple
    '000A27': 'Apple',
    '000A95': 'Apple',
    '000D93': 'Apple',
    '001124': 'Apple',
    '001451': 'Apple',
    '0016CB': 'Apple',
    '0017F2': 'Apple',
    '0019E3': 'Apple',
    '001B63': 'Apple',
    '001CB3': 'Apple',
    '001D4F': 'Apple',
    '001E52': 'Apple',
    '001EC2': 'Apple',
    '001F5B': 'Apple',
    '001FF3': 'Apple',
    '002241': 'Apple',
    '002312': 'Apple',
    '002332': 'Apple',
    '00236C': 'Apple',
    '002500': 'Apple',
    '0025BC': 'Apple',
    '002608': 'Apple',
    '00264A': 'Apple',
    '0026B0': 'Apple',
    '0026BB': 'Apple',
    '003065': 'Apple',
    '003EE1': 'Apple',
    '0050E4': 'Apple',
    '005882': 'Apple',
    '006171': 'Apple',
    '0088E1': 'Apple',
    '00B362': 'Apple',
    '00C610': 'Apple',
    '00CDFE': 'Apple',
    '00F4B9': 'Apple',
    '00F76F': 'Apple',

    # Samsung
    '000D4B': 'Samsung',
    '0012FB': 'Samsung',
    '00166B': 'Samsung',
    '00166C': 'Samsung',
    '001247': 'Samsung',
    '001377': 'Samsung',
    '0015B9': 'Samsung',
    '001A8A': 'Samsung',
    '001B98': 'Samsung',
    '001D25': 'Samsung',
    '001E7D': 'Samsung',
    '001FCC': 'Samsung',
    '002119': 'Samsung',
    '0021D1': 'Samsung',
    '0021D2': 'Samsung',
    '002339': 'Samsung',
    '0024E9': 'Samsung',
    '002566': 'Samsung',
    '0026D0': 'Samsung',
    '0026E2': 'Samsung',

    # Intel
    '001111': 'Intel',
    '001302': 'Intel',
    '001500': 'Intel',
    '001517': 'Intel',
    '0016EA': 'Intel',
    '0016EB': 'Intel',
    '0018DE': 'Intel',
    '001A92': 'Intel',
    '001B21': 'Intel',
    '001B77': 'Intel',
    '001CC0': 'Intel',
    '001DE0': 'Intel',
    '001DE1': 'Intel',
    '001E64': 'Intel',
    '001E65': 'Intel',
    '001E67': 'Intel',
    '001F3B': 'Intel',
    '001F3C': 'Intel',
    '002128': 'Intel',
    '0021D7': 'Intel',
    '00215C': 'Intel',
    '002214': 'Intel',
    '002315': 'Intel',
    '002414': 'Intel',
    '002690': 'Intel',
    '00270E': 'Intel',

    # Cisco
    '000142': 'Cisco',
    '000143': 'Cisco',
    '000163': 'Cisco',
    '000164': 'Cisco',
    '000196': 'Cisco',
    '000197': 'Cisco',
    '0001C7': 'Cisco',
    '0001C9': 'Cisco',
    '000216': 'Cisco',
    '000217': 'Cisco',
    '00023D': 'Cisco',
    '00024A': 'Cisco',
    '0002B9': 'Cisco',
    '0002BA': 'Cisco',
    '0002FC': 'Cisco',
    '0002FD': 'Cisco',
    '00030D': 'Cisco',
    '00030E': 'Cisco',
    '000380': 'Cisco',
    '00039F': 'Cisco',

    # Google
    '001A11': 'Google',
    '3C5AB4': 'Google',
    '54609A': 'Google',
    'F88FCA': 'Google',
    'F4F5E8': 'Google',
    '94EB2C': 'Google',

    # Microsoft
    '000D3A': 'Microsoft',
    '001DD8': 'Microsoft',
    '0025AE': 'Microsoft',
    '0050F2': 'Microsoft',
    '28187C': 'Microsoft',
    '60455E': 'Microsoft',
    '7C1E52': 'Microsoft',

    # Amazon
    '00FC8B': 'Amazon',
    '0C47C9': 'Amazon',
    '18742E': 'Amazon',
    '34D270': 'Amazon',
    '40B4CD': 'Amazon',
    '44650D': 'Amazon',
    '4CEFC0': 'Amazon',
    '50DCE7': 'Amazon',
    '50F5DA': 'Amazon',

    # Huawei
    '000E6D': 'Huawei',
    '001E10': 'Huawei',
    '002128': 'Huawei',
    '0025FE': 'Huawei',
    '002568': 'Huawei',
    '002EC7': 'Huawei',
    '0034FE': 'Huawei',
    '00464B': 'Huawei',
    '00508B': 'Huawei',
    '005A13': 'Huawei',

    # TP-Link
    '000AEB': 'TP-Link',
    '001131': 'TP-Link',
    '00195B': 'TP-Link',
    '001D0F': 'TP-Link',
    '001F09': 'TP-Link',
    '002375': 'TP-Link',
    '002629': 'TP-Link',
    '002719': 'TP-Link',
    '00E04C': 'Realtek',

    # Netgear
    '00095B': 'Netgear',
    '000E6D': 'Netgear',
    '000F34': 'Netgear',
    '00146C': 'Netgear',
    '001B2F': 'Netgear',
    '001E2A': 'Netgear',
    '001F33': 'Netgear',
    '00223F': 'Netgear',
    '002430': 'Netgear',
    '002566': 'Netgear',

    # Linksys
    '000C41': 'Linksys',
    '000E08': 'Linksys',
    '000F66': 'Linksys',
    '001217': 'Linksys',
    '001310': 'Linksys',
    '0014BF': 'Linksys',
    '001601': 'Linksys',
    '001839': 'Linksys',
    '0018F8': 'Linksys',

    # Ubiquiti
    '00156D': 'Ubiquiti',
    '0027D5': 'Ubiquiti',
    '0418D6': 'Ubiquiti',
    '04183F': 'Ubiquiti',
    '18E8DD': 'Ubiquiti',
    '24A43C': 'Ubiquiti',
    '44D9E7': 'Ubiquiti',
    '687251': 'Ubiquiti',
    '788A20': 'Ubiquiti',

    # Espressif (ESP32, ESP8266)
    '24:0A:C4': 'Espressif',
    '240AC4': 'Espressif',
    '30AEA4': 'Espressif',
    '807D3A': 'Espressif',
    '84CCA8': 'Espressif',
    'A4CF12': 'Espressif',
    'BC:DD:C2': 'Espressif',
    'BCDDC2': 'Espressif',
    'CC50E3': 'Espressif',

    # Raspberry Pi Foundation
    'B827EB': 'Raspberry Pi',
    'DC:A6:32': 'Raspberry Pi',
    'DCA632': 'Raspberry Pi',
    'E4:5F:01': 'Raspberry Pi',
    'E45F01': 'Raspberry Pi',

    # Dell
    '001422': 'Dell',
    '0014A5': 'Dell',
    '0015C5': 'Dell',
    '0018A4': 'Dell',
    '0019B9': 'Dell',
    '001A6A': 'Dell',
    '001C23': 'Dell',
    '001D09': 'Dell',
    '001E4F': 'Dell',
    '001E C9': 'Dell',

    # HP
    '000A57': 'HP',
    '000D9D': 'HP',
    '000E7F': 'HP',
    '000F20': 'HP',
    '00110A': 'HP',
    '001185': 'HP',
    '00121E': 'HP',
    '001279': 'HP',
    '001321': 'HP',
    '00138F': 'HP',
    '001422': 'HP',
    '0014C2': 'HP',
    '00151A': 'HP',

    # Broadcom
    '000AF7': 'Broadcom',
    '001018': 'Broadcom',
    '00101F': 'Broadcom',
    '0010A9': 'Broadcom',
    '001109': 'Broadcom',
    '001217': 'Broadcom',

    # Realtek
    '00E04C': 'Realtek',
    '001F1F': 'Realtek',
    '00E018': 'Realtek',
    '4CEDDE': 'Realtek',
    '527F2C': 'Realtek',
    '9C5C8E': 'Realtek',

    # Qualcomm
    '001FA7': 'Qualcomm',
    '58CB52': 'Qualcomm',
    '909016': 'Qualcomm',
    'B8A386': 'Qualcomm',

    # MediaTek
    '000E8E': 'MediaTek',
    '001330': 'MediaTek',
    '001EE1': 'MediaTek',
    '002214': 'MediaTek',
}


class OUILookup:
    """MAC address manufacturer lookup using OUI database."""

    def __init__(self):
        self._oui_db = COMMON_OUI.copy()

    @staticmethod
    def normalize_mac(mac: str) -> str:
        """
        Normalize MAC address to uppercase without separators.

        Args:
            mac: MAC address in any format

        Returns:
            Normalized MAC address (uppercase, no separators)
        """
        if not mac:
            return ""
        # Remove all non-hex characters
        return re.sub(r'[^0-9A-Fa-f]', '', mac).upper()

    def get_oui(self, mac: str) -> str:
        """
        Get the OUI (first 6 hex digits) from a MAC address.

        Args:
            mac: MAC address

        Returns:
            OUI string (6 characters)
        """
        normalized = self.normalize_mac(mac)
        return normalized[:6] if len(normalized) >= 6 else ""

    def lookup(self, mac: str) -> Optional[str]:
        """
        Look up the manufacturer for a MAC address.

        Args:
            mac: MAC address in any format

        Returns:
            Manufacturer name or None if not found
        """
        oui = self.get_oui(mac)
        if not oui:
            return None
        return self._oui_db.get(oui)

    def lookup_with_fallback(self, mac: str) -> str:
        """
        Look up manufacturer with fallback to 'Unknown'.

        Args:
            mac: MAC address

        Returns:
            Manufacturer name or 'Unknown'
        """
        result = self.lookup(mac)
        return result if result else "Unknown"

    def is_local_admin(self, mac: str) -> bool:
        """
        Check if MAC address is locally administered (randomized).

        Args:
            mac: MAC address

        Returns:
            True if locally administered
        """
        normalized = self.normalize_mac(mac)
        if len(normalized) < 2:
            return False

        # Second character should be 2, 6, A, or E for locally administered
        second_char = normalized[1].upper()
        return second_char in ['2', '6', 'A', 'E']

    def format_mac(self, mac: str, separator: str = ':') -> str:
        """
        Format MAC address with specified separator.

        Args:
            mac: MAC address
            separator: Character to use between octets

        Returns:
            Formatted MAC address
        """
        normalized = self.normalize_mac(mac)
        if len(normalized) != 12:
            return mac  # Return original if invalid

        return separator.join(normalized[i:i+2] for i in range(0, 12, 2))

    def add_oui(self, oui: str, manufacturer: str):
        """
        Add or update an OUI entry.

        Args:
            oui: OUI string (6 hex characters)
            manufacturer: Manufacturer name
        """
        normalized = self.normalize_mac(oui)[:6]
        if len(normalized) == 6:
            self._oui_db[normalized] = manufacturer


# Global instance for convenience
_oui_lookup = None


def get_oui_lookup() -> OUILookup:
    """Get the global OUI lookup instance."""
    global _oui_lookup
    if _oui_lookup is None:
        _oui_lookup = OUILookup()
    return _oui_lookup


def lookup_manufacturer(mac: str) -> str:
    """
    Convenience function to look up manufacturer.

    Args:
        mac: MAC address

    Returns:
        Manufacturer name or 'Unknown'
    """
    return get_oui_lookup().lookup_with_fallback(mac)


def is_randomized_mac(mac: str) -> bool:
    """
    Convenience function to check if MAC is randomized.

    Args:
        mac: MAC address

    Returns:
        True if locally administered (likely randomized)
    """
    return get_oui_lookup().is_local_admin(mac)
