"""Kismet device source — pulls .kismet DB files from a Kismet server over SSH."""

import logging
from pathlib import Path

from sources import DeviceSource, SourceConfig, RemoteFile

log = logging.getLogger(__name__)


class KismetSource(DeviceSource):
    """Source for Kismet running on RPi5 or similar."""

    def list_files(self) -> list[RemoteFile]:
        """List .kismet files, also checking /tmp/ which Kismet uses by default."""
        files = super().list_files()

        # Kismet sometimes writes to /tmp/ if log_prefix isn't set
        try:
            sftp = self._get_sftp()
            for alt_path in ['/tmp']:
                try:
                    for entry in sftp.listdir_attr(alt_path):
                        if entry.filename.endswith('.kismet'):
                            full_path = f"{alt_path}/{entry.filename}"
                            if not any(f.path == full_path for f in files):
                                files.append(RemoteFile(
                                    path=full_path,
                                    size=entry.st_size or 0,
                                    mtime=entry.st_mtime or 0,
                                    source_name=self.config.name,
                                ))
                except Exception:
                    continue
        except Exception:
            pass

        return sorted(files, key=lambda f: f.mtime, reverse=True)
