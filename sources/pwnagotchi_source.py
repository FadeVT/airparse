"""Pwnagotchi device source — pulls .pcap handshakes over USB SSH."""

import logging
from pathlib import Path

from sources import DeviceSource, SourceConfig, RemoteFile

log = logging.getLogger(__name__)


class PwnagotchiSource(DeviceSource):
    """Source for Pwnagotchi connected via USB gadget networking."""
    pass  # Base class handles everything; defaults set in SourceConfig
