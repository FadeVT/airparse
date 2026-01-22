"""SQL query templates for Kismet database operations."""


class KismetQueries:
    """Collection of SQL queries for Kismet database."""

    # Get all access points with details
    GET_ACCESS_POINTS = """
        SELECT
            devmac,
            json_extract(device, '$."kismet.device.base.name"') as name,
            json_extract(device, '$."kismet.device.base.commonname"') as commonname,
            json_extract(device, '$."kismet.device.base.channel"') as channel,
            json_extract(device, '$."kismet.device.base.manuf"') as manufacturer,
            json_extract(device, '$."kismet.device.base.crypt"') as encryption,
            strongest_signal,
            first_time,
            last_time,
            min_lat,
            min_lon,
            max_lat,
            max_lon,
            type,
            device
        FROM devices
        WHERE phyname = 'IEEE802.11'
            AND type IN ('Wi-Fi AP', 'Wi-Fi WDS AP')
    """

    # Get client devices
    GET_CLIENTS = """
        SELECT
            devmac as client_mac,
            json_extract(device, '$."kismet.device.base.name"') as name,
            json_extract(device, '$."kismet.device.base.commonname"') as commonname,
            json_extract(device, '$."kismet.device.base.manuf"') as manufacturer,
            json_extract(device, '$."dot11.device"."dot11.device.last_bssid"') as last_bssid,
            strongest_signal,
            first_time,
            last_time,
            min_lat,
            min_lon,
            type,
            device
        FROM devices
        WHERE phyname = 'IEEE802.11'
            AND type IN ('Wi-Fi Client', 'Wi-Fi Bridged', 'Wi-Fi Device')
    """

    # Get all devices summary
    GET_ALL_DEVICES = """
        SELECT
            devmac,
            phyname,
            type,
            json_extract(device, '$."kismet.device.base.name"') as name,
            json_extract(device, '$."kismet.device.base.commonname"') as commonname,
            json_extract(device, '$."kismet.device.base.manuf"') as manufacturer,
            json_extract(device, '$."kismet.device.base.channel"') as channel,
            strongest_signal,
            first_time,
            last_time,
            min_lat,
            min_lon,
            max_lat,
            max_lon
        FROM devices
    """

    # Get Bluetooth devices
    GET_BLUETOOTH_DEVICES = """
        SELECT
            devmac,
            type,
            json_extract(device, '$."kismet.device.base.name"') as name,
            json_extract(device, '$."kismet.device.base.commonname"') as commonname,
            json_extract(device, '$."kismet.device.base.manuf"') as manufacturer,
            strongest_signal,
            first_time,
            last_time,
            min_lat,
            min_lon
        FROM devices
        WHERE phyname IN ('Bluetooth', 'BTLE')
    """

    # Get packets over time for timeline
    GET_PACKETS_TIMELINE = """
        SELECT
            ts_sec,
            COUNT(*) as packet_count,
            AVG(signal) as avg_signal
        FROM data
        GROUP BY ts_sec
        ORDER BY ts_sec
    """

    # Get geographic coverage
    GET_GEOGRAPHIC_BOUNDS = """
        SELECT
            MIN(lat) as min_lat,
            MAX(lat) as max_lat,
            MIN(lon) as min_lon,
            MAX(lon) as max_lon,
            COUNT(DISTINCT devmac) as unique_devices
        FROM data
        WHERE lat != 0 AND lon != 0
    """

    # Get GPS data points
    GET_GPS_DATA = """
        SELECT
            ts_sec,
            lat,
            lon,
            alt,
            speed
        FROM data
        WHERE lat != 0 AND lon != 0
        ORDER BY ts_sec
    """

    # Get device GPS tracks
    GET_DEVICE_GPS_TRACK = """
        SELECT
            ts_sec,
            lat,
            lon,
            signal
        FROM data
        WHERE devmac = ?
            AND lat != 0 AND lon != 0
        ORDER BY ts_sec
    """

    # Get data sources
    GET_DATA_SOURCES = """
        SELECT
            uuid,
            typestring,
            definition,
            name,
            interface
        FROM datasources
    """

    # Get alerts
    GET_ALERTS = """
        SELECT
            ts_sec,
            ts_usec,
            phyname,
            devmac,
            lat,
            lon,
            header,
            json
        FROM alerts
        ORDER BY ts_sec DESC
    """

    # Get device count by type
    GET_DEVICE_COUNTS = """
        SELECT
            phyname,
            COUNT(*) as count
        FROM devices
        GROUP BY phyname
    """

    # Get unique SSIDs
    GET_UNIQUE_SSIDS = """
        SELECT
            json_extract(device, '$."kismet.device.base.name"') as ssid,
            COUNT(*) as ap_count
        FROM devices
        WHERE phyname = 'IEEE802.11'
            AND type IN ('Wi-Fi AP', 'Wi-Fi WDS AP')
            AND json_extract(device, '$."kismet.device.base.name"') IS NOT NULL
            AND json_extract(device, '$."kismet.device.base.name"') != ''
        GROUP BY ssid
        ORDER BY ap_count DESC
    """

    # Get time range
    GET_TIME_RANGE = """
        SELECT
            MIN(first_time) as earliest,
            MAX(last_time) as latest
        FROM devices
    """

    # Get signal strength distribution
    GET_SIGNAL_DISTRIBUTION = """
        SELECT
            CAST((strongest_signal / 10) * 10 AS INTEGER) as signal_bucket,
            COUNT(*) as count
        FROM devices
        WHERE strongest_signal IS NOT NULL
            AND strongest_signal != 0
        GROUP BY signal_bucket
        ORDER BY signal_bucket
    """

    # Check if table exists
    CHECK_TABLE_EXISTS = """
        SELECT name FROM sqlite_master
        WHERE type='table' AND name=?
    """

    # Get table info
    GET_TABLE_INFO = """
        PRAGMA table_info({table_name})
    """
