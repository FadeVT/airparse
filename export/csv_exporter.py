"""CSV export functionality for Kismet data."""

import csv
from pathlib import Path
from typing import List, Optional
import pandas as pd


class CSVExporter:
    """Export Kismet data to CSV format."""

    def __init__(self):
        self.include_headers = True
        self.delimiter = ','
        self.quoting = csv.QUOTE_MINIMAL

    def export_dataframe(self, df: pd.DataFrame, output_path: str,
                         columns: Optional[List[str]] = None,
                         exclude_columns: Optional[List[str]] = None,
                         include_headers: bool = True) -> bool:
        """
        Export a DataFrame to CSV.

        Args:
            df: DataFrame to export
            output_path: Path to output CSV file
            columns: Optional list of columns to include (None = all)
            exclude_columns: Optional list of columns to exclude
            include_headers: Whether to include column headers

        Returns:
            True if successful, False otherwise
        """
        try:
            if df is None or df.empty:
                return False

            df = df.copy()

            # Exclude specified columns
            if exclude_columns:
                cols_to_drop = [c for c in exclude_columns if c in df.columns]
                if cols_to_drop:
                    df = df.drop(columns=cols_to_drop)

            # Select columns if specified
            if columns:
                # Filter to only existing columns
                existing_cols = [c for c in columns if c in df.columns]
                if existing_cols:
                    df = df[existing_cols]

            # Exclude binary/complex columns
            for col in list(df.columns):
                if df[col].dtype == 'object':
                    # Check if it's a binary blob
                    sample = df[col].dropna().head(1)
                    if len(sample) > 0 and isinstance(sample.iloc[0], bytes):
                        df = df.drop(columns=[col])

            df.to_csv(
                output_path,
                index=False,
                header=include_headers,
                sep=self.delimiter,
                quoting=self.quoting
            )
            return True

        except Exception as e:
            print(f"CSV export error: {e}")
            return False

    def export_devices(self, df: pd.DataFrame, output_path: str,
                       device_type: str = "all") -> bool:
        """
        Export device data to CSV with appropriate columns.

        Args:
            df: Device DataFrame
            output_path: Path to output CSV file
            device_type: Type of devices ('ap', 'client', 'bluetooth', 'all')

        Returns:
            True if successful
        """
        # Define columns for each device type
        common_cols = ['devmac', 'phyname', 'manufacturer', 'strongest_signal',
                       'first_time', 'last_time', 'min_lat', 'min_lon']

        if device_type == 'ap':
            columns = common_cols + ['ssid', 'channel', 'encryption']
        elif device_type == 'client':
            columns = common_cols + ['associations']
        elif device_type == 'bluetooth':
            columns = common_cols + ['name']
        else:
            columns = None  # Include all

        return self.export_dataframe(df, output_path, columns)

    def export_networks(self, df: pd.DataFrame, output_path: str) -> bool:
        """
        Export network/SSID data to CSV.

        Args:
            df: Networks DataFrame
            output_path: Path to output CSV file

        Returns:
            True if successful
        """
        return self.export_dataframe(df, output_path)

    def export_alerts(self, df: pd.DataFrame, output_path: str) -> bool:
        """
        Export alerts to CSV.

        Args:
            df: Alerts DataFrame
            output_path: Path to output CSV file

        Returns:
            True if successful
        """
        # Exclude JSON blob column
        columns = [c for c in df.columns if c != 'json']
        return self.export_dataframe(df, output_path, columns)

    def export_gps_track(self, df: pd.DataFrame, output_path: str) -> bool:
        """
        Export GPS track data to CSV.

        Args:
            df: GPS DataFrame
            output_path: Path to output CSV file

        Returns:
            True if successful
        """
        columns = ['timestamp', 'lat', 'lon', 'alt', 'speed']
        existing_cols = [c for c in columns if c in df.columns]
        return self.export_dataframe(df, output_path, existing_cols)


def export_to_csv(df: pd.DataFrame, output_path: str,
                  columns: Optional[List[str]] = None) -> bool:
    """
    Convenience function to export DataFrame to CSV.

    Args:
        df: DataFrame to export
        output_path: Path to output file
        columns: Optional column list

    Returns:
        True if successful
    """
    exporter = CSVExporter()
    return exporter.export_dataframe(df, output_path, columns)
