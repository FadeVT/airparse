"""WiGLE cell-search API client.

Separate from `database/wigle_api.py` to keep the Cell subsystem isolated,
but reuses the same credentials file (`~/.config/airparse/wigle_credentials.json`).

Endpoint: GET https://api.wigle.net/api/v2/cell/search
Query params we use:
  - latrange1, latrange2 — south, north
  - longrange1, longrange2 — west, east
  - resultsPerPage — page size (WiGLE caps this; experimentally ~1000)
  - searchAfter — pagination cursor (returned on truncated responses)
Auth: HTTP Basic with the saved API name + token.
"""

from __future__ import annotations

import base64
import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterator, Optional

log = logging.getLogger(__name__)

_CRED_PATH = Path.home() / ".config" / "airparse" / "wigle_credentials.json"
_URL = "https://api.wigle.net/api/v2/cell/search"
_MIN_REQUEST_INTERVAL = 2.0   # courtesy — same as the WiFi-side client
_PAGE_SIZE = 1000             # WiGLE's soft cap


@dataclass
class CellSearchRow:
    operator_key: str        # MCC_MNC_LAC_CID (their `id` field)
    lat: float
    lon: float
    carrier: str             # their `ssid`
    radio_type: str          # their `gentype`
    channel: Optional[int]
    attributes: str


def _credentials() -> tuple[str, str]:
    if not _CRED_PATH.exists():
        return "", ""
    try:
        data = json.loads(_CRED_PATH.read_text())
        return data.get("api_name", ""), data.get("api_token", "")
    except (json.JSONDecodeError, OSError):
        return "", ""


def has_credentials() -> bool:
    name, token = _credentials()
    return bool(name) and bool(token)


def search_bbox(
    lat_south: float,
    lat_north: float,
    lon_west: float,
    lon_east: float,
    progress_cb: Optional[Callable[[str], None]] = None,
    max_pages: int = 50,
) -> Iterator[CellSearchRow]:
    """Stream every cell WiGLE has in the given bbox, paginating via searchAfter."""
    name, token = _credentials()
    if not name or not token:
        raise RuntimeError("WiGLE API credentials not configured")

    params = {
        "latrange1": f"{lat_south:.6f}",
        "latrange2": f"{lat_north:.6f}",
        "longrange1": f"{lon_west:.6f}",
        "longrange2": f"{lon_east:.6f}",
        "resultsPerPage": str(_PAGE_SIZE),
    }

    last_call = 0.0
    total_seen = 0
    for page in range(max_pages):
        # Courtesy rate-limit
        elapsed = time.time() - last_call
        if elapsed < _MIN_REQUEST_INTERVAL:
            time.sleep(_MIN_REQUEST_INTERVAL - elapsed)
        last_call = time.time()

        url = f"{_URL}?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(url)
        cred = base64.b64encode(f"{name}:{token}".encode()).decode()
        req.add_header("Authorization", f"Basic {cred}")
        req.add_header("Accept", "application/json")

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = resp.read().decode("utf-8")
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"WiGLE HTTP {e.code}: {e.read().decode('utf-8', errors='replace')[:200]}")
        except urllib.error.URLError as e:
            raise RuntimeError(f"WiGLE request failed: {e}")

        doc = json.loads(body)
        if not doc.get("success", False):
            raise RuntimeError(f"WiGLE returned success=false: {body[:200]}")

        results = doc.get("results") or []
        if not results:
            break

        for r in results:
            yield _row_from_json(r)
        total_seen += len(results)

        if progress_cb:
            progress_cb(f"WiGLE page {page + 1}: {total_seen:,} cells pulled so far")

        cursor = doc.get("searchAfter")
        if not cursor or len(results) < _PAGE_SIZE:
            break
        params["searchAfter"] = str(cursor)


def _row_from_json(r: dict) -> CellSearchRow:
    channel = r.get("channel")
    try:
        channel = int(channel) if channel is not None else None
    except (TypeError, ValueError):
        channel = None
    return CellSearchRow(
        operator_key=str(r.get("id", "")),
        lat=float(r.get("trilat") or 0.0),
        lon=float(r.get("trilong") or 0.0),
        carrier=str(r.get("ssid") or ""),
        radio_type=str(r.get("gentype") or "").upper(),
        channel=channel,
        attributes=str(r.get("attributes") or ""),
    )
