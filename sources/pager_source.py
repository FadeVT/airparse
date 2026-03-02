"""Hak5 Pager device source — pulls .pcap + .22000 files over USB SSH."""

import logging
from pathlib import Path

from sources import DeviceSource, SourceConfig, RemoteFile

log = logging.getLogger(__name__)


class PagerSource(DeviceSource):
    """Source for Hak5 WiFi Pineapple Pager."""

    def scan_additional_paths(self) -> list[str]:
        """Look for WiGLE CSVs alongside captures."""
        csv_paths = []
        try:
            sftp = self._get_sftp()
            base = self.config.remote_path.rstrip('/')

            # Check sibling wigle/ directory (Pineapple loot structure)
            for wigle_dir in [f"{base}/../wigle", f"{base}/wigle", '/root/wigle']:
                try:
                    for entry in sftp.listdir_attr(wigle_dir):
                        if entry.filename.lower().endswith('.csv'):
                            csv_paths.append(f"{wigle_dir}/{entry.filename}")
                except Exception:
                    continue
        except Exception:
            pass
        return csv_paths
