"""JSON export functionality for Kismet data."""

import json
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Any
import pandas as pd


class JSONExporter:
    """Export Kismet data to JSON format."""

    def __init__(self):
        self.pretty_print = True
        self.indent = 2

    def _json_serializer(self, obj: Any) -> Any:
        """Custom JSON serializer for non-standard types."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        elif isinstance(obj, bytes):
            return obj.decode('utf-8', errors='replace')
        elif pd.isna(obj):
            return None
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    def export_dataframe(self, df: pd.DataFrame, output_path: str,
                         columns: Optional[List[str]] = None,
                         orient: str = 'records',
                         pretty: bool = True) -> bool:
        """
        Export a DataFrame to JSON.

        Args:
            df: DataFrame to export
            output_path: Path to output JSON file
            columns: Optional list of columns to include
            orient: JSON orientation ('records', 'columns', 'index', 'values')
            pretty: Whether to pretty-print the JSON

        Returns:
            True if successful
        """
        try:
            if df is None or df.empty:
                return False

            # Select columns if specified
            if columns:
                existing_cols = [c for c in columns if c in df.columns]
                if existing_cols:
                    df = df[existing_cols]

            # Exclude binary columns
            for col in df.columns:
                if df[col].dtype == 'object':
                    sample = df[col].dropna().head(1)
                    if len(sample) > 0 and isinstance(sample.iloc[0], bytes):
                        df = df.drop(columns=[col])

            # Convert to JSON
            if orient == 'records':
                data = df.to_dict(orient='records')
            else:
                data = df.to_dict(orient=orient)

            # Write to file
            with open(output_path, 'w', encoding='utf-8') as f:
                if pretty:
                    json.dump(data, f, indent=self.indent, default=self._json_serializer)
                else:
                    json.dump(data, f, default=self._json_serializer)

            return True

        except Exception as e:
            print(f"JSON export error: {e}")
            return False

    def export_with_metadata(self, df: pd.DataFrame, output_path: str,
                             metadata: dict = None,
                             data_key: str = 'devices') -> bool:
        """
        Export DataFrame with metadata wrapper.

        Args:
            df: DataFrame to export
            output_path: Path to output JSON file
            metadata: Additional metadata to include
            data_key: Key name for the data array

        Returns:
            True if successful
        """
        try:
            if df is None or df.empty:
                return False

            # Build output structure
            output = {
                'export_info': {
                    'exported_at': datetime.now().isoformat(),
                    'total_records': len(df),
                    'format_version': '1.0'
                }
            }

            # Add custom metadata
            if metadata:
                output['export_info'].update(metadata)

            # Convert DataFrame
            # Exclude binary columns first
            clean_df = df.copy()
            for col in clean_df.columns:
                if clean_df[col].dtype == 'object':
                    sample = clean_df[col].dropna().head(1)
                    if len(sample) > 0 and isinstance(sample.iloc[0], bytes):
                        clean_df = clean_df.drop(columns=[col])

            output[data_key] = clean_df.to_dict(orient='records')

            # Write to file
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output, f, indent=self.indent, default=self._json_serializer)

            return True

        except Exception as e:
            print(f"JSON export error: {e}")
            return False

    def export_summary(self, summary: dict, output_path: str) -> bool:
        """
        Export database summary to JSON.

        Args:
            summary: Summary dictionary from KismetDBReader
            output_path: Path to output JSON file

        Returns:
            True if successful
        """
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=self.indent, default=self._json_serializer)
            return True
        except Exception as e:
            print(f"JSON export error: {e}")
            return False

    def export_full_database(self, db_reader, output_path: str) -> bool:
        """
        Export entire database to a single JSON file.

        Args:
            db_reader: KismetDBReader instance
            output_path: Path to output JSON file

        Returns:
            True if successful
        """
        try:
            output = {
                'export_info': {
                    'exported_at': datetime.now().isoformat(),
                    'format_version': '1.0',
                    'source': str(db_reader.db_path) if db_reader.db_path else 'unknown'
                },
                'summary': db_reader.get_device_summary(),
                'access_points': [],
                'clients': [],
                'bluetooth_devices': [],
                'networks': [],
                'alerts': [],
                'data_sources': []
            }

            # Export each data type
            ap_df = db_reader.get_access_points()
            if not ap_df.empty:
                # Remove device blob
                if 'device' in ap_df.columns:
                    ap_df = ap_df.drop(columns=['device'])
                output['access_points'] = ap_df.to_dict(orient='records')

            client_df = db_reader.get_clients()
            if not client_df.empty:
                if 'device' in client_df.columns:
                    client_df = client_df.drop(columns=['device'])
                output['clients'] = client_df.to_dict(orient='records')

            bt_df = db_reader.get_bluetooth_devices()
            if not bt_df.empty:
                if 'device' in bt_df.columns:
                    bt_df = bt_df.drop(columns=['device'])
                output['bluetooth_devices'] = bt_df.to_dict(orient='records')

            networks_df = db_reader.get_networks()
            if not networks_df.empty:
                output['networks'] = networks_df.to_dict(orient='records')

            alerts_df = db_reader.get_alerts()
            if not alerts_df.empty:
                if 'json' in alerts_df.columns:
                    alerts_df = alerts_df.drop(columns=['json'])
                output['alerts'] = alerts_df.to_dict(orient='records')

            ds_df = db_reader.get_data_sources()
            if not ds_df.empty:
                output['data_sources'] = ds_df.to_dict(orient='records')

            # Write to file
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output, f, indent=self.indent, default=self._json_serializer)

            return True

        except Exception as e:
            print(f"JSON export error: {e}")
            return False


def export_to_json(df: pd.DataFrame, output_path: str,
                   pretty: bool = True) -> bool:
    """
    Convenience function to export DataFrame to JSON.

    Args:
        df: DataFrame to export
        output_path: Path to output file
        pretty: Whether to pretty-print

    Returns:
        True if successful
    """
    exporter = JSONExporter()
    return exporter.export_dataframe(df, output_path, pretty=pretty)
