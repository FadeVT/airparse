"""Geographic utility functions for Kismet GUI Reader."""

import math
from typing import Tuple, List, Optional


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate the great circle distance between two points on Earth.

    Args:
        lat1, lon1: First point coordinates in decimal degrees
        lat2, lon2: Second point coordinates in decimal degrees

    Returns:
        Distance in meters
    """
    R = 6371000  # Earth's radius in meters

    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = (math.sin(delta_phi / 2) ** 2 +
         math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c


def get_bounding_box(points: List[Tuple[float, float]], padding: float = 0.001) -> dict:
    """
    Calculate bounding box for a list of coordinates.

    Args:
        points: List of (lat, lon) tuples
        padding: Padding to add around the bounding box in degrees

    Returns:
        Dictionary with min_lat, max_lat, min_lon, max_lon
    """
    if not points:
        return {'min_lat': 0, 'max_lat': 0, 'min_lon': 0, 'max_lon': 0}

    lats = [p[0] for p in points if p[0] != 0]
    lons = [p[1] for p in points if p[1] != 0]

    if not lats or not lons:
        return {'min_lat': 0, 'max_lat': 0, 'min_lon': 0, 'max_lon': 0}

    return {
        'min_lat': min(lats) - padding,
        'max_lat': max(lats) + padding,
        'min_lon': min(lons) - padding,
        'max_lon': max(lons) + padding
    }


def get_center_point(points: List[Tuple[float, float]]) -> Tuple[float, float]:
    """
    Calculate the center point of a list of coordinates.

    Args:
        points: List of (lat, lon) tuples

    Returns:
        Tuple of (center_lat, center_lon)
    """
    if not points:
        return (0.0, 0.0)

    valid_points = [(lat, lon) for lat, lon in points if lat != 0 and lon != 0]

    if not valid_points:
        return (0.0, 0.0)

    avg_lat = sum(p[0] for p in valid_points) / len(valid_points)
    avg_lon = sum(p[1] for p in valid_points) / len(valid_points)

    return (avg_lat, avg_lon)


def calculate_zoom_level(bounds: dict, map_width: int = 800, map_height: int = 600) -> int:
    """
    Calculate appropriate zoom level for given bounds.

    Args:
        bounds: Dictionary with min_lat, max_lat, min_lon, max_lon
        map_width: Map width in pixels
        map_height: Map height in pixels

    Returns:
        Zoom level (1-18)
    """
    if not bounds or bounds['min_lat'] == bounds['max_lat']:
        return 15

    lat_diff = bounds['max_lat'] - bounds['min_lat']
    lon_diff = bounds['max_lon'] - bounds['min_lon']

    # Approximate degrees per pixel at zoom level 0
    lat_zoom = math.log2(180 / lat_diff) if lat_diff > 0 else 18
    lon_zoom = math.log2(360 / lon_diff) if lon_diff > 0 else 18

    zoom = min(lat_zoom, lon_zoom) - 1
    return max(1, min(18, int(zoom)))


def signal_to_color(signal: int) -> str:
    """
    Convert signal strength to a color hex code.

    Args:
        signal: Signal strength in dBm

    Returns:
        Color hex code string
    """
    if signal >= -50:
        return '#00ff00'  # Green - excellent
    elif signal >= -60:
        return '#7fff00'  # Yellow-green - good
    elif signal >= -70:
        return '#ffff00'  # Yellow - fair
    elif signal >= -80:
        return '#ffa500'  # Orange - weak
    else:
        return '#ff0000'  # Red - poor


def device_type_to_color(device_type: str) -> str:
    """
    Get marker color based on device type.

    Args:
        device_type: Type of device (AP, Client, Bluetooth, etc.)

    Returns:
        Color hex code string
    """
    colors = {
        'Wi-Fi AP': '#3498db',      # Blue
        'Wi-Fi Client': '#2ecc71',   # Green
        'Bluetooth': '#9b59b6',      # Purple
        'BTLE': '#e74c3c',           # Red
        'IEEE802.11': '#3498db',     # Blue
        'default': '#95a5a6'         # Gray
    }
    return colors.get(device_type, colors['default'])


def format_coordinates(lat: float, lon: float, precision: int = 6) -> str:
    """
    Format coordinates as a readable string.

    Args:
        lat: Latitude
        lon: Longitude
        precision: Decimal places

    Returns:
        Formatted coordinate string
    """
    lat_dir = 'N' if lat >= 0 else 'S'
    lon_dir = 'E' if lon >= 0 else 'W'
    return f"{abs(lat):.{precision}f}°{lat_dir}, {abs(lon):.{precision}f}°{lon_dir}"


def meters_to_readable(meters: float) -> str:
    """
    Convert meters to a human-readable distance string.

    Args:
        meters: Distance in meters

    Returns:
        Formatted distance string
    """
    if meters < 1000:
        return f"{meters:.0f} m"
    else:
        return f"{meters / 1000:.2f} km"
