"""Cell-tower analysis subsystem.

Fully isolated from the WiFi pipeline. Reads WiGLE KML downloads (same files
the WiFi side reads, read-only), parses the `Cellular Networks` layer, resolves
carrier + band from MCC/MNC/EARFCN/NR-ARFCN, and presents coverage on its own
top-level tab.

No imports from `ui.wigle_view`, `database.merged_db`, etc. Shared I/O is
restricted to the WiGLE KML directory (read-only).
"""
