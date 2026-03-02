"""Configuration settings for AirParse."""

DEFAULT_CONFIG = {
    'ui': {
        'theme': 'light',  # light, dark, auto
        'font_size': 10,
        'table_row_height': 25,
        'show_oui_lookups': True
    },
    'map': {
        'default_zoom': 13,
        'cluster_threshold': 50,
        'heatmap_radius': 20,
        'tile_server': 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png'
    },
    'filters': {
        'auto_apply': False,
        'remember_last': True,
        'min_signal_default': -90
    },
    'export': {
        'default_format': 'csv',
        'include_gps': True,
        'timestamp_format': 'ISO8601'
    },
    'performance': {
        'cache_device_json': True,
        'lazy_load_packets': True,
        'max_table_rows': 10000
    },
    'hashcat': {
        'wordlist_path': '/usr/share/wordlists/rockyou.txt',
        'use_gpu': True,
        'default_crack_level': 'standard',
        'rules_search_paths': [
            '/usr/share/doc/hashcat/rules',
            '/usr/share/hashcat/rules',
        ],
        'user_rules_dir': '~/.local/share/hashcat/rules',
    }
}

# Kismet database table definitions
KISMET_TABLES = {
    'devices': {
        'columns': ['devkey', 'phyname', 'devmac', 'strongest_signal',
                    'min_lat', 'min_lon', 'max_lat', 'max_lon',
                    'first_time', 'last_time', 'device'],
        'display_name': 'All Devices'
    },
    'data': {
        'columns': ['ts_sec', 'ts_usec', 'phyname', 'devmac', 'datasource',
                    'lat', 'lon', 'alt', 'speed', 'signal'],
        'display_name': 'Packet Data'
    },
    'datasources': {
        'columns': ['uuid', 'typestring', 'definition', 'name', 'interface'],
        'display_name': 'Data Sources'
    },
    'alerts': {
        'columns': ['ts_sec', 'ts_usec', 'phyname', 'devmac', 'lat', 'lon',
                    'header', 'json'],
        'display_name': 'Alerts'
    },
    'snapshots': {
        'columns': ['ts_sec', 'ts_usec', 'lat', 'lon', 'snaptype', 'json'],
        'display_name': 'Snapshots'
    }
}

# Filter options for the UI
FILTER_OPTIONS = {
    'signal_strength': {
        'min': -100,
        'max': 0,
        'type': 'range_slider'
    },
    'encryption': {
        'options': ['Open', 'WEP', 'WPA', 'WPA2', 'WPA3'],
        'type': 'checkbox_group'
    },
    'device_type': {
        'options': ['Wi-Fi AP', 'Wi-Fi Client', 'Bluetooth', 'BTLE'],
        'type': 'checkbox_group'
    },
    'time_range': {
        'type': 'datetime_range'
    },
    'manufacturer': {
        'type': 'text_search'
    },
    'channel': {
        'options': list(range(1, 15)) + list(range(36, 166, 4)),
        'type': 'multiselect'
    }
}

# Export format configurations
EXPORT_FORMATS = {
    'csv': {
        'extension': '.csv',
        'handler': 'export_to_csv',
        'options': ['Include headers', 'Select columns']
    },
    'json': {
        'extension': '.json',
        'handler': 'export_to_json',
        'options': ['Pretty print', 'Compact']
    },
    'kml': {
        'extension': '.kml',
        'handler': 'export_to_kml',
        'options': ['Include GPS tracks', 'Add device details']
    },
    'pdf': {
        'extension': '.pdf',
        'handler': 'export_to_pdf',
        'options': ['Include maps', 'Add statistics', 'Template style']
    },
    'html': {
        'extension': '.html',
        'handler': 'export_to_html',
        'options': ['Interactive maps', 'Embedded charts']
    }
}
