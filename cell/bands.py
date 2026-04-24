"""3GPP EARFCN → LTE band and NR-ARFCN → 5G band lookup.

Tables sourced from:
  - 3GPP TS 36.101 §5.7.3 (LTE) — EARFCN ranges per E-UTRA band.
  - 3GPP TS 38.101-1 §5.4.2 (5G NR FR1) — NR-ARFCN ranges per NR band.

Each entry carries the downlink range only. Uplink-only bands (29, n28-u, etc.)
noted inline where relevant. `common_name` is the marketing label you see on
coverage maps (e.g. "AWS", "PCS", "Lower 700 MHz"); `None` if not widely named.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Band:
    number: int
    label: str           # "B2", "B4", "n41", "n71"
    common_name: Optional[str]
    dl_low: int          # channel (EARFCN / NR-ARFCN)
    dl_high: int         # inclusive


# ─── LTE bands (E-UTRA) ──────────────────────────────────────────────
#
# Focused on bands used in the US. Downlink ranges only.
# (Full 3GPP table is larger; add more as they come up in real data.)

LTE_BANDS: tuple[Band, ...] = (
    Band(1,  "B1",  "IMT (2100)",         0,     599),
    Band(2,  "B2",  "PCS",                600,   1199),
    Band(3,  "B3",  "DCS (1800)",         1200,  1949),
    Band(4,  "B4",  "AWS",                1950,  2399),
    Band(5,  "B5",  "CLR",                2400,  2649),
    Band(7,  "B7",  "IMT-E (2600)",       2750,  3449),
    Band(8,  "B8",  "Extended GSM",       3450,  3799),
    Band(12, "B12", "Lower 700 MHz",      5010,  5179),
    Band(13, "B13", "Upper 700 MHz",      5180,  5279),
    Band(14, "B14", "FirstNet",           5280,  5379),
    Band(17, "B17", "Lower 700 MHz (ATT)",5730,  5849),
    Band(25, "B25", "Extended PCS",       8040,  8689),
    Band(26, "B26", "Extended CLR",       8690,  9039),
    Band(29, "B29", "Lower 700 DL",       9660,  9769),   # DL only
    Band(30, "B30", "WCS",                9770,  9869),
    Band(41, "B41", "BRS/EBS (2.5 GHz)",  39650, 41589),
    Band(46, "B46", "LAA (5 GHz)",        46790, 54539),  # unlicensed
    Band(48, "B48", "CBRS",               55240, 56739),
    Band(66, "B66", "Extended AWS",       66436, 67335),
    Band(71, "B71", "600 MHz",            68586, 68935),
)


# ─── 5G NR (FR1) bands ───────────────────────────────────────────────
#
# NR-ARFCN ranges — global frequency raster. Downlink only.

NR_BANDS: tuple[Band, ...] = (
    Band(1,  "n1",  "IMT (2100)",         422000, 434000),
    Band(2,  "n2",  "PCS",                386000, 398000),
    Band(3,  "n3",  "DCS (1800)",         361000, 376000),
    Band(5,  "n5",  "CLR",                173800, 178800),
    Band(7,  "n7",  "IMT-E (2600)",       524000, 538000),
    Band(8,  "n8",  "Extended GSM",       185000, 192000),
    Band(12, "n12", "Lower 700 MHz",      145800, 149200),
    Band(14, "n14", "FirstNet",           151600, 153600),
    Band(20, "n20", "Digital Dividend",   158200, 164200),
    Band(25, "n25", "Extended PCS",       386000, 399000),
    Band(28, "n28", "APT 700",            151600, 160600),
    Band(38, "n38", "TDD 2600",           514000, 524000),
    Band(40, "n40", "TDD 2300",           460000, 480000),
    Band(41, "n41", "BRS/EBS (2.5 GHz)",  499200, 537999),
    Band(48, "n48", "CBRS",               636667, 646666),
    Band(66, "n66", "Extended AWS",       422000, 440000),
    Band(71, "n71", "600 MHz",            123400, 130400),
    Band(77, "n77", "C-Band (3.7 GHz)",   620000, 680000),
    Band(78, "n78", "C-Band (3.5 GHz)",   620000, 653333),
    Band(79, "n79", "4.7 GHz",            693334, 733333),
)


def lte_band_for_earfcn(earfcn: int) -> Optional[Band]:
    """Resolve an EARFCN to its LTE band. Returns None on no match."""
    if earfcn <= 0:
        return None
    for b in LTE_BANDS:
        if b.dl_low <= earfcn <= b.dl_high:
            return b
    return None


def nr_band_for_arfcn(arfcn: int) -> Optional[Band]:
    """Resolve an NR-ARFCN to its 5G band.

    Some NR bands overlap (e.g. n66 and n1 both include 422000–440000).
    Returning the first match in table order — callers who need stricter
    resolution should consult cell identity context.
    """
    if arfcn <= 0:
        return None
    for b in NR_BANDS:
        if b.dl_low <= arfcn <= b.dl_high:
            return b
    return None


def resolve(radio_type: str, channel: int) -> Optional[Band]:
    """Dispatch on radio type — 'LTE' → LTE table, 'NR' → NR table."""
    if not channel or channel <= 0:
        return None
    t = radio_type.upper().strip()
    if t == "LTE":
        return lte_band_for_earfcn(channel)
    if t in ("NR", "5G", "5GNR", "NR5G"):
        return nr_band_for_arfcn(channel)
    return None
