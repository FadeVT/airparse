"""MCC + MNC → carrier name.

Focused on US operators (the ones that'll show up in Vermont drives). Extended
with common international carriers in case the user crosses a border. Fallback
to the resolved operator name that WiGLE includes in the KML `<name>` element
if the MCC/MNC isn't known here — that's what `unknown_name_fallback` is for.
"""

from __future__ import annotations

from typing import Optional


# (MCC, MNC) → display name
_CARRIERS: dict[tuple[str, str], str] = {
    # ─── United States ───────────────────────────────────────
    # T-Mobile (post-Sprint merger, all PLMNs owned by T-Mobile USA Inc.)
    ("310", "026"): "T-Mobile",
    ("310", "160"): "T-Mobile",
    ("310", "200"): "T-Mobile",
    ("310", "210"): "T-Mobile",
    ("310", "220"): "T-Mobile",
    ("310", "230"): "T-Mobile",
    ("310", "240"): "T-Mobile",
    ("310", "250"): "T-Mobile",
    ("310", "260"): "T-Mobile",
    ("310", "270"): "T-Mobile",
    ("310", "310"): "T-Mobile",
    ("310", "490"): "T-Mobile",
    ("310", "580"): "T-Mobile",
    ("310", "660"): "T-Mobile",
    ("310", "800"): "T-Mobile",
    # Ex-Sprint PLMNs (still LTE-owned by T-Mobile):
    ("310", "120"): "T-Mobile (ex-Sprint)",
    ("310", "830"): "T-Mobile (ex-Sprint)",
    ("311", "490"): "T-Mobile (ex-Sprint)",
    ("311", "870"): "T-Mobile (ex-Sprint)",
    ("311", "880"): "T-Mobile (ex-Sprint)",
    ("311", "882"): "T-Mobile (ex-Sprint)",
    ("312", "190"): "T-Mobile (ex-Sprint)",
    ("312", "530"): "T-Mobile (ex-Sprint)",

    # Regional US carriers seen in VT drives
    ("311", "990"): "VTel Wireless",

    # AT&T
    ("310", "030"): "AT&T",
    ("310", "070"): "AT&T",
    ("310", "090"): "AT&T",
    ("310", "150"): "AT&T",
    ("310", "170"): "AT&T",
    ("310", "280"): "AT&T",
    ("310", "380"): "AT&T",
    ("310", "410"): "AT&T",
    ("310", "560"): "AT&T",
    ("310", "680"): "AT&T",
    ("311", "180"): "AT&T",

    # Verizon
    ("310", "004"): "Verizon",
    ("310", "010"): "Verizon",
    ("310", "012"): "Verizon",
    ("310", "013"): "Verizon",
    ("311", "110"): "Verizon",
    ("311", "270"): "Verizon",
    ("311", "271"): "Verizon",
    ("311", "272"): "Verizon",
    ("311", "273"): "Verizon",
    ("311", "274"): "Verizon",
    ("311", "275"): "Verizon",
    ("311", "276"): "Verizon",
    ("311", "277"): "Verizon",
    ("311", "278"): "Verizon",
    ("311", "279"): "Verizon",
    ("311", "280"): "Verizon",
    ("311", "281"): "Verizon",
    ("311", "282"): "Verizon",
    ("311", "283"): "Verizon",
    ("311", "284"): "Verizon",
    ("311", "285"): "Verizon",
    ("311", "286"): "Verizon",
    ("311", "287"): "Verizon",
    ("311", "288"): "Verizon",
    ("311", "289"): "Verizon",
    ("311", "480"): "Verizon",
    ("311", "481"): "Verizon",
    ("311", "482"): "Verizon",
    ("311", "483"): "Verizon",
    ("311", "484"): "Verizon",
    ("311", "485"): "Verizon",
    ("311", "486"): "Verizon",
    ("311", "487"): "Verizon",
    ("311", "488"): "Verizon",
    ("311", "489"): "Verizon",

    # Dish (Boost Mobile)
    ("313", "340"): "Dish",
    ("311", "970"): "Dish (Boost)",

    # US Cellular
    ("311", "580"): "US Cellular",
    ("311", "581"): "US Cellular",
    ("311", "582"): "US Cellular",
    ("311", "583"): "US Cellular",
    ("311", "584"): "US Cellular",
    ("311", "585"): "US Cellular",
    ("311", "586"): "US Cellular",
    ("311", "587"): "US Cellular",
    ("311", "588"): "US Cellular",
    ("311", "589"): "US Cellular",

    # ─── Canada (VT is near border) ──────────────────────────
    ("302", "220"): "Telus",
    ("302", "221"): "Telus",
    ("302", "320"): "Rogers",
    ("302", "370"): "Fido (Rogers)",
    ("302", "500"): "Videotron",
    ("302", "610"): "Bell",
    ("302", "620"): "ICE (Rogers)",
    ("302", "660"): "MTS",
    ("302", "720"): "Rogers",
    ("302", "780"): "SaskTel",
}


# MCC-only → country (for unknown MNC within a known country)
_COUNTRIES: dict[str, str] = {
    "310": "US", "311": "US", "312": "US", "313": "US", "314": "US", "316": "US",
    "302": "CA",
    "232": "AT",
    "310311312313314": "US",  # defensive only; not a real code
}


def lookup(mcc: str, mnc: str, unknown_name_fallback: Optional[str] = None) -> str:
    """Resolve MCC+MNC to a display name.

    Pads MNC to 3 digits for consistent lookup — WiGLE stores some as 2-digit.
    """
    mcc = (mcc or "").strip()
    mnc = (mnc or "").strip()
    if not mcc or not mnc:
        return unknown_name_fallback or "Unknown"

    # Try as-given, then zero-padded to 3 digits.
    for m in (mnc, mnc.zfill(3)):
        hit = _CARRIERS.get((mcc, m))
        if hit is not None:
            return hit

    # Unknown — if WiGLE gave us a name from the KML, trust it over a bare
    # country fallback. Otherwise label with the country.
    if unknown_name_fallback and unknown_name_fallback.strip() and unknown_name_fallback != "Unknown":
        return unknown_name_fallback
    country = _COUNTRIES.get(mcc)
    if country:
        return f"Unknown {country} ({mcc}-{mnc})"
    return f"Unknown ({mcc}-{mnc})"


def split_operator_key(key: str) -> tuple[str, str, str, str]:
    """Parse WiGLE's cell operator key `MCC<MNC>_<LAC|TAC>_<CID|CI|NCI>`.

    Returns (mcc, mnc, xac, cid). MCC is the first 3 digits, MNC the remaining
    digits before the first underscore.
    """
    parts = key.split("_")
    if len(parts) < 3:
        return "", "", "", ""
    mccmnc, xac, cid = parts[0], parts[1], parts[2]
    mcc = mccmnc[:3]
    mnc = mccmnc[3:]
    return mcc, mnc, xac, cid
