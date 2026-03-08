"""WiGLE REST API client for BSSID GPS lookups.

No Qt dependency — pure stdlib + json. Designed for use from both
the merge pipeline (worker thread) and ad-hoc context-menu lookups.
"""

import base64
import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

_CONFIG_DIR = Path.home() / '.config' / 'airparse'
_CRED_PATH = _CONFIG_DIR / 'wigle_credentials.json'
_CACHE_PATH = _CONFIG_DIR / 'wigle_cache.json'
_BASE_URL = 'https://api.wigle.net/api/v2'
_MIN_REQUEST_INTERVAL = 2.0  # seconds between API calls


@dataclass
class WigleResult:
    bssid: str
    ssid: str = ''
    lat: float = 0.0
    lon: float = 0.0
    channel: int = 0
    encryption: str = ''
    found: bool = False


class WigleApiClient:
    """HTTP client for the WiGLE network search API."""

    def __init__(self):
        self._last_request_time: float = 0.0
        self._backoff: float = _MIN_REQUEST_INTERVAL
        self._cache: Optional[dict] = None

    # --- Credential management ---

    @staticmethod
    def save_credentials(api_name: str, api_token: str):
        _CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        data = {'api_name': api_name, 'api_token': api_token}
        _CRED_PATH.write_text(json.dumps(data))
        os.chmod(_CRED_PATH, 0o600)

    @staticmethod
    def has_credentials() -> bool:
        if not _CRED_PATH.exists():
            return False
        try:
            data = json.loads(_CRED_PATH.read_text())
            return bool(data.get('api_name')) and bool(data.get('api_token'))
        except (json.JSONDecodeError, OSError):
            return False

    @staticmethod
    def get_credentials() -> tuple[str, str]:
        if not _CRED_PATH.exists():
            return '', ''
        try:
            data = json.loads(_CRED_PATH.read_text())
            return data.get('api_name', ''), data.get('api_token', '')
        except (json.JSONDecodeError, OSError):
            return '', ''

    # --- Cache management ---

    def _load_cache(self) -> dict:
        if self._cache is not None:
            return self._cache
        if _CACHE_PATH.exists():
            try:
                self._cache = json.loads(_CACHE_PATH.read_text())
            except (json.JSONDecodeError, OSError):
                self._cache = {}
        else:
            self._cache = {}
        return self._cache

    def _save_cache(self):
        if self._cache is None:
            return
        _CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        _CACHE_PATH.write_text(json.dumps(self._cache))

    def is_cached(self, bssid: str) -> bool:
        cache = self._load_cache()
        return bssid.upper() in cache

    def get_cached(self, bssid: str) -> Optional[WigleResult]:
        cache = self._load_cache()
        entry = cache.get(bssid.upper())
        if entry is None:
            return None
        return WigleResult(
            bssid=bssid,
            ssid=entry.get('ssid', ''),
            lat=entry.get('lat', 0.0),
            lon=entry.get('lon', 0.0),
            channel=entry.get('channel', 0),
            encryption=entry.get('encryption', ''),
            found=entry.get('found', False),
        )

    def clear_cache(self) -> int:
        count = len(self._load_cache())
        self._cache = {}
        if _CACHE_PATH.exists():
            _CACHE_PATH.unlink()
        return count

    def cache_size(self) -> int:
        return len(self._load_cache())

    # --- Auth header ---

    def _auth_header(self) -> str:
        name, token = self.get_credentials()
        encoded = base64.b64encode(f'{name}:{token}'.encode()).decode()
        return f'Basic {encoded}'

    # --- Rate limiting ---

    def _wait_for_rate_limit(self):
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < self._backoff:
            time.sleep(self._backoff - elapsed)

    def _record_request(self):
        self._last_request_time = time.monotonic()

    # --- API calls ---

    def test_credentials(self) -> tuple[bool, str]:
        """Validate credentials against the WiGLE profile endpoint.
        Returns (success, message).
        """
        if not self.has_credentials():
            return False, 'No credentials configured'
        try:
            req = urllib.request.Request(
                f'{_BASE_URL}/profile/user',
                headers={
                    'Authorization': self._auth_header(),
                    'User-Agent': 'AirParse/2.0',
                })
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())
                if data.get('success'):
                    userid = data.get('userid', 'unknown')
                    return True, f'Authenticated as {userid}'
                return False, data.get('message', 'Unknown error')
        except urllib.error.HTTPError as e:
            if e.code == 401:
                return False, 'Invalid API name or token'
            return False, f'HTTP {e.code}: {e.reason}'
        except Exception as e:
            return False, str(e)

    def lookup_bssid(self, bssid: str) -> WigleResult:
        """Look up a single BSSID via the WiGLE network search API.
        Uses cache if available. Stores both positive and negative results.
        """
        bssid_upper = bssid.upper()
        cached = self.get_cached(bssid)
        if cached is not None:
            return cached

        if not self.has_credentials():
            return WigleResult(bssid=bssid)

        self._wait_for_rate_limit()

        # netid param uses colon-separated uppercase MAC
        netid = bssid_upper
        if ':' not in netid and len(netid) == 12:
            netid = ':'.join(netid[i:i+2] for i in range(0, 12, 2))

        url = f'{_BASE_URL}/network/search?netid={netid}'
        try:
            req = urllib.request.Request(url, headers={
                'Authorization': self._auth_header(),
                'User-Agent': 'AirParse/2.0',
            })
            self._record_request()
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read())

            self._backoff = _MIN_REQUEST_INTERVAL  # reset on success

            results = data.get('results', [])
            if results:
                r = results[0]
                result = WigleResult(
                    bssid=bssid,
                    ssid=r.get('ssid', ''),
                    lat=float(r.get('trilat', 0) or 0),
                    lon=float(r.get('trilong', 0) or 0),
                    channel=int(r.get('channel', 0) or 0),
                    encryption=r.get('encryption', ''),
                    found=True,
                )
                self._cache_result(bssid_upper, result)
                return result
            else:
                result = WigleResult(bssid=bssid, found=False)
                self._cache_result(bssid_upper, result)
                return result

        except urllib.error.HTTPError as e:
            if e.code == 429:
                self._backoff = min(self._backoff * 2, 60.0)
                log.warning('WiGLE rate limited, backing off to %.1fs', self._backoff)
            elif e.code == 401:
                log.error('WiGLE auth failed')
            else:
                log.warning('WiGLE HTTP %d: %s', e.code, e.reason)
            return WigleResult(bssid=bssid)
        except Exception as e:
            log.warning('WiGLE lookup failed for %s: %s', bssid, e)
            return WigleResult(bssid=bssid)

    def get_user_stats(self) -> dict:
        """Fetch WiGLE user statistics (discovered, ranks).
        Returns dict with keys: discovered, total_locations, month_rank,
        prev_month_rank, rank, prev_rank. Empty dict on failure.
        """
        if not self.has_credentials():
            return {}
        self._wait_for_rate_limit()
        try:
            req = urllib.request.Request(
                f'{_BASE_URL}/stats/user',
                headers={
                    'Authorization': self._auth_header(),
                    'Accept': 'application/json',
                    'User-Agent': 'AirParse/2.0',
                })
            self._record_request()
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read())
            stats = data.get('statistics', {})
            return {
                'discovered': stats.get('discoveredWiFiGPS', 0) + stats.get('discoveredWiFi', 0),
                'total_locations': stats.get('totalWiFiLocations', 0),
                'month_rank': stats.get('monthRank', 0),
                'prev_month_rank': stats.get('prevMonthRank', 0),
                'rank': stats.get('rank', 0),
                'prev_rank': stats.get('prevRank', 0),
            }
        except Exception as e:
            log.warning('WiGLE stats fetch failed: %s', e)
            return {}

    def get_transactions(self) -> list[dict]:
        """Fetch all WiGLE upload transactions, paginating through all pages."""
        if not self.has_credentials():
            return []
        all_results = []
        page_start = 0
        while True:
            self._wait_for_rate_limit()
            try:
                req = urllib.request.Request(
                    f'{_BASE_URL}/file/transactions?pagestart={page_start}',
                    headers={
                        'Authorization': self._auth_header(),
                        'Accept': 'application/json',
                        'User-Agent': 'AirParse/2.0',
                    })
                self._record_request()
                with urllib.request.urlopen(req, timeout=15) as resp:
                    data = json.loads(resp.read())
                results = data.get('results', [])
                if not results:
                    break
                all_results.extend(results)
                page_start += len(results)
            except Exception as e:
                log.warning('WiGLE transactions fetch failed at page %d: %s', page_start, e)
                break
        return all_results

    def upload_file(self, file_path: str) -> tuple[bool, str]:
        """Upload a wiglecsv file to WiGLE.
        Returns (success, transid_or_error).
        """
        if not self.has_credentials():
            return False, 'No credentials configured'
        self._wait_for_rate_limit()
        try:
            boundary = f'----AirParseBoundary{int(time.time())}'
            filename = Path(file_path).name
            with open(file_path, 'rb') as f:
                file_data = f.read()
            body = (
                f'--{boundary}\r\n'
                f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'
                f'Content-Type: application/octet-stream\r\n\r\n'
            ).encode() + file_data + f'\r\n--{boundary}--\r\n'.encode()
            req = urllib.request.Request(
                f'{_BASE_URL}/file/upload',
                data=body,
                headers={
                    'Authorization': self._auth_header(),
                    'Content-Type': f'multipart/form-data; boundary={boundary}',
                    'Accept': 'application/json',
                    'User-Agent': 'AirParse/2.0',
                },
                method='POST')
            self._record_request()
            with urllib.request.urlopen(req, timeout=60) as resp:
                data = json.loads(resp.read())
            transid = data.get('transid', '')
            if transid:
                return True, transid
            return True, 'Uploaded (no transid returned)'
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8', errors='replace')
            try:
                msg = json.loads(body).get('message', e.reason)
            except Exception:
                msg = e.reason
            return False, f'HTTP {e.code}: {msg}'
        except Exception as e:
            return False, str(e)

    def search_networks(self, **kwargs) -> list[dict]:
        """Search WiGLE network database.
        Supported kwargs: ssid, netid (BSSID), ssidlike, encryption,
        freenet, paynet, latrange1, latrange2, longrange1, longrange2,
        lastupdt, resultsPerPage.
        Returns list of network result dicts.
        """
        if not self.has_credentials():
            return []
        self._wait_for_rate_limit()
        params = '&'.join(f'{k}={urllib.parse.quote(str(v))}' for k, v in kwargs.items() if v)
        try:
            req = urllib.request.Request(
                f'{_BASE_URL}/network/search?{params}',
                headers={
                    'Authorization': self._auth_header(),
                    'Accept': 'application/json',
                    'User-Agent': 'AirParse/2.0',
                })
            self._record_request()
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read())
            self._backoff = _MIN_REQUEST_INTERVAL
            return data.get('results', [])
        except urllib.error.HTTPError as e:
            if e.code == 429:
                self._backoff = min(self._backoff * 2, 60.0)
            log.warning('WiGLE search failed: HTTP %d', e.code)
            return []
        except Exception as e:
            log.warning('WiGLE search failed: %s', e)
            return []

    def download_kml(self, transid: str) -> tuple[bool, bytes]:
        """Download KML file for a transaction.
        Returns (success, kml_bytes).
        """
        if not self.has_credentials():
            return False, b''
        self._wait_for_rate_limit()
        try:
            req = urllib.request.Request(
                f'{_BASE_URL}/file/kml/{transid}',
                headers={
                    'Authorization': self._auth_header(),
                    'User-Agent': 'AirParse/2.0',
                })
            self._record_request()
            with urllib.request.urlopen(req, timeout=120) as resp:
                return True, resp.read()
        except Exception as e:
            log.warning('WiGLE KML download failed for %s: %s', transid, e)
            return False, b''

    def _cache_result(self, bssid_upper: str, result: WigleResult):
        cache = self._load_cache()
        cache[bssid_upper] = {
            'ssid': result.ssid,
            'lat': result.lat,
            'lon': result.lon,
            'channel': result.channel,
            'encryption': result.encryption,
            'found': result.found,
            'ts': int(time.time()),
        }
        self._save_cache()
