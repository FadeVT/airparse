"""WiFi-observations subsystem for the WiGLE Dashboard map.

Mirrors the Cell subsystem's layout: KML parse (reading the `Wifi Networks`
layer from `~/AirParse/Wigle/*.kml`), state-tracked incremental import,
dedicated SQLite DB, map renderers decoupled from parsing.
"""
