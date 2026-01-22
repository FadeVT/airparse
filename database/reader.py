"""Kismet database reader class."""

import sqlite3
from pathlib import Path
from typing import Optional
from datetime import datetime

import pandas as pd

from .queries import KismetQueries
from .parser import KismetParser


class KismetDBReader:
    """Main class for reading Kismet database files."""

    def __init__(self):
        self.db_path: Optional[Path] = None
        self.connection: Optional[sqlite3.Connection] = None
        self.devices_cache: dict = {}
        self.filters: dict = {}
        self._parser = KismetParser()

    def open_database(self, path: str) -> bool:
        """
        Open a Kismet database file.

        Args:
            path: Path to the .kismet database file

        Returns:
            True if successfully opened, False otherwise
        """
        try:
            self.close_database()
            self.db_path = Path(path)

            if not self.db_path.exists():
                raise FileNotFoundError(f"Database file not found: {path}")

            self.connection = sqlite3.connect(str(self.db_path))
            self.connection.row_factory = sqlite3.Row

            # Verify it's a valid Kismet database
            if not self._verify_kismet_database():
                self.close_database()
                raise ValueError("Not a valid Kismet database file")

            return True

        except Exception as e:
            self.close_database()
            raise e

    def close_database(self):
        """Close the current database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
        self.db_path = None
        self.devices_cache.clear()

    def _verify_kismet_database(self) -> bool:
        """Verify the database has expected Kismet tables."""
        if not self.connection:
            return False

        cursor = self.connection.cursor()

        # Check for devices table (primary Kismet table)
        cursor.execute(KismetQueries.CHECK_TABLE_EXISTS, ('devices',))
        return cursor.fetchone() is not None

    def is_connected(self) -> bool:
        """Check if database is connected."""
        return self.connection is not None

    def get_database_info(self) -> dict:
        """Get basic information about the database."""
        if not self.is_connected():
            return {}

        info = {
            'path': str(self.db_path),
            'filename': self.db_path.name if self.db_path else '',
            'size_bytes': self.db_path.stat().st_size if self.db_path else 0,
            'tables': self._get_table_list()
        }
        return info

    def _get_table_list(self) -> list:
        """Get list of tables in the database."""
        if not self.connection:
            return []

        cursor = self.connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return [row[0] for row in cursor.fetchall()]

    def get_device_summary(self) -> dict:
        """
        Get summary statistics about devices in the database.

        Returns:
            Dictionary with device counts and statistics
        """
        if not self.is_connected():
            return {}

        cursor = self.connection.cursor()

        # Get device counts by phy type
        cursor.execute(KismetQueries.GET_DEVICE_COUNTS)
        phy_counts = {row['phyname']: row['count'] for row in cursor.fetchall()}

        # Get time range
        cursor.execute(KismetQueries.GET_TIME_RANGE)
        time_row = cursor.fetchone()
        time_range = {
            'earliest': datetime.fromtimestamp(time_row['earliest']) if time_row['earliest'] else None,
            'latest': datetime.fromtimestamp(time_row['latest']) if time_row['latest'] else None
        }

        # Get geographic bounds
        cursor.execute(KismetQueries.GET_GEOGRAPHIC_BOUNDS)
        geo_row = cursor.fetchone()
        geo_bounds = {
            'min_lat': geo_row['min_lat'],
            'max_lat': geo_row['max_lat'],
            'min_lon': geo_row['min_lon'],
            'max_lon': geo_row['max_lon'],
            'unique_devices_with_gps': geo_row['unique_devices']
        } if geo_row else {}

        # Get total device count
        cursor.execute("SELECT COUNT(*) as total FROM devices")
        total = cursor.fetchone()['total']

        return {
            'total_devices': total,
            'by_phy_type': phy_counts,
            'time_range': time_range,
            'geographic_bounds': geo_bounds
        }

    def get_access_points(self, filters: Optional[dict] = None) -> pd.DataFrame:
        """
        Get access points from the database.

        Args:
            filters: Optional dictionary of filters to apply

        Returns:
            DataFrame with access point data
        """
        if not self.is_connected():
            return pd.DataFrame()

        query = KismetQueries.GET_ACCESS_POINTS

        # Apply filters if provided
        if filters:
            query = self._apply_filters_to_query(query, filters)

        df = pd.read_sql_query(query, self.connection)

        # Parse timestamps
        if 'first_time' in df.columns:
            df['first_time'] = pd.to_datetime(df['first_time'], unit='s')
        if 'last_time' in df.columns:
            df['last_time'] = pd.to_datetime(df['last_time'], unit='s')

        return df

    def get_clients(self, filters: Optional[dict] = None) -> pd.DataFrame:
        """
        Get client devices from the database.

        Args:
            filters: Optional dictionary of filters to apply

        Returns:
            DataFrame with client data
        """
        if not self.is_connected():
            return pd.DataFrame()

        query = KismetQueries.GET_CLIENTS

        if filters:
            query = self._apply_filters_to_query(query, filters)

        df = pd.read_sql_query(query, self.connection)

        # Parse timestamps
        if 'first_time' in df.columns:
            df['first_time'] = pd.to_datetime(df['first_time'], unit='s')
        if 'last_time' in df.columns:
            df['last_time'] = pd.to_datetime(df['last_time'], unit='s')

        return df

    def get_all_devices(self, filters: Optional[dict] = None) -> pd.DataFrame:
        """
        Get all devices from the database.

        Args:
            filters: Optional dictionary of filters to apply

        Returns:
            DataFrame with all device data
        """
        if not self.is_connected():
            return pd.DataFrame()

        query = KismetQueries.GET_ALL_DEVICES

        if filters:
            query = self._apply_filters_to_query(query, filters)

        df = pd.read_sql_query(query, self.connection)

        # Parse timestamps
        if 'first_time' in df.columns:
            df['first_time'] = pd.to_datetime(df['first_time'], unit='s')
        if 'last_time' in df.columns:
            df['last_time'] = pd.to_datetime(df['last_time'], unit='s')

        return df

    def get_bluetooth_devices(self, filters: Optional[dict] = None) -> pd.DataFrame:
        """
        Get Bluetooth devices from the database.

        Args:
            filters: Optional dictionary of filters to apply

        Returns:
            DataFrame with Bluetooth device data
        """
        if not self.is_connected():
            return pd.DataFrame()

        query = KismetQueries.GET_BLUETOOTH_DEVICES

        if filters:
            query = self._apply_filters_to_query(query, filters)

        df = pd.read_sql_query(query, self.connection)

        # Parse timestamps
        if 'first_time' in df.columns:
            df['first_time'] = pd.to_datetime(df['first_time'], unit='s')
        if 'last_time' in df.columns:
            df['last_time'] = pd.to_datetime(df['last_time'], unit='s')

        return df

    def get_networks(self) -> pd.DataFrame:
        """
        Get unique SSIDs/networks from the database.

        Returns:
            DataFrame with network data
        """
        if not self.is_connected():
            return pd.DataFrame()

        return pd.read_sql_query(KismetQueries.GET_UNIQUE_SSIDS, self.connection)

    def get_gps_data(self) -> pd.DataFrame:
        """
        Get GPS data points from the database.

        Returns:
            DataFrame with GPS data
        """
        if not self.is_connected():
            return pd.DataFrame()

        df = pd.read_sql_query(KismetQueries.GET_GPS_DATA, self.connection)

        if 'ts_sec' in df.columns:
            df['timestamp'] = pd.to_datetime(df['ts_sec'], unit='s')

        return df

    def get_device_gps_track(self, devmac: str) -> pd.DataFrame:
        """
        Get GPS track for a specific device.

        Args:
            devmac: Device MAC address

        Returns:
            DataFrame with device GPS track
        """
        if not self.is_connected():
            return pd.DataFrame()

        df = pd.read_sql_query(
            KismetQueries.GET_DEVICE_GPS_TRACK,
            self.connection,
            params=(devmac,)
        )

        if 'ts_sec' in df.columns:
            df['timestamp'] = pd.to_datetime(df['ts_sec'], unit='s')

        return df

    def get_data_sources(self) -> pd.DataFrame:
        """
        Get data sources from the database.

        Returns:
            DataFrame with data source information
        """
        if not self.is_connected():
            return pd.DataFrame()

        return pd.read_sql_query(KismetQueries.GET_DATA_SOURCES, self.connection)

    def get_alerts(self) -> pd.DataFrame:
        """
        Get alerts from the database.

        Returns:
            DataFrame with alert data
        """
        if not self.is_connected():
            return pd.DataFrame()

        df = pd.read_sql_query(KismetQueries.GET_ALERTS, self.connection)

        if 'ts_sec' in df.columns:
            df['timestamp'] = pd.to_datetime(df['ts_sec'], unit='s')

        return df

    def get_packets_timeline(self) -> pd.DataFrame:
        """
        Get packet count timeline data.

        Returns:
            DataFrame with timestamp and packet counts
        """
        if not self.is_connected():
            return pd.DataFrame()

        df = pd.read_sql_query(KismetQueries.GET_PACKETS_TIMELINE, self.connection)

        if 'ts_sec' in df.columns:
            df['timestamp'] = pd.to_datetime(df['ts_sec'], unit='s')

        return df

    def get_signal_distribution(self) -> pd.DataFrame:
        """
        Get signal strength distribution.

        Returns:
            DataFrame with signal buckets and counts
        """
        if not self.is_connected():
            return pd.DataFrame()

        return pd.read_sql_query(KismetQueries.GET_SIGNAL_DISTRIBUTION, self.connection)

    def _apply_filters_to_query(self, query: str, filters: dict) -> str:
        """
        Apply filters to a SQL query.

        Args:
            query: Base SQL query
            filters: Dictionary of filters

        Returns:
            Modified query with filters applied
        """
        conditions = []

        if 'min_signal' in filters and filters['min_signal'] is not None:
            conditions.append(f"strongest_signal >= {filters['min_signal']}")

        if 'max_signal' in filters and filters['max_signal'] is not None:
            conditions.append(f"strongest_signal <= {filters['max_signal']}")

        if 'start_time' in filters and filters['start_time'] is not None:
            timestamp = int(filters['start_time'].timestamp())
            conditions.append(f"first_time >= {timestamp}")

        if 'end_time' in filters and filters['end_time'] is not None:
            timestamp = int(filters['end_time'].timestamp())
            conditions.append(f"last_time <= {timestamp}")

        if 'manufacturer' in filters and filters['manufacturer']:
            # Escape single quotes for SQL
            manuf = filters['manufacturer'].replace("'", "''")
            conditions.append(
                f"json_extract(device, '$.kismet.device.base.manuf') LIKE '%{manuf}%'"
            )

        if conditions:
            # Check if query already has WHERE clause
            if 'WHERE' in query.upper():
                query += " AND " + " AND ".join(conditions)
            else:
                query += " WHERE " + " AND ".join(conditions)

        return query

    def execute_raw_query(self, query: str, params: tuple = ()) -> pd.DataFrame:
        """
        Execute a raw SQL query.

        Args:
            query: SQL query string
            params: Query parameters

        Returns:
            DataFrame with query results
        """
        if not self.is_connected():
            return pd.DataFrame()

        return pd.read_sql_query(query, self.connection, params=params)

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close_database()
        return False
