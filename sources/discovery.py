"""Device auto-discovery: try the cached IP, fall back to mDNS, then a subnet scan.

The point: when warpig (or any other source) jumps to a new IP, AirParse should
find it without manual intervention. Today the cached IP in sources.json works
until it doesn't.

Algorithm:
1. TCP-probe the cached `host` on `port`. If reachable, return it.
2. If `hostname` is set (e.g. 'warpig.local'), resolve it via avahi-resolve.
   If the resolved IP is reachable, return it.
3. (Optional, last resort) scan the local /24 for the port. Off by default.

The caller is responsible for persisting `host` updates back to sources.json.
"""

import ipaddress
import logging
import socket
import subprocess
from typing import Optional

log = logging.getLogger(__name__)


def is_reachable(host: str, port: int = 22, timeout: float = 2.0) -> bool:
    """Quick TCP connect probe. False on any failure (timeout, refused, DNS error)."""
    if not host:
        return False
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (socket.timeout, socket.gaierror, ConnectionRefusedError, OSError):
        return False


def resolve_mdns(hostname: str, timeout: float = 5.0) -> Optional[str]:
    """Resolve an mDNS hostname (e.g. 'warpig.local') to an IPv4 address via avahi.

    Returns None if avahi-resolve is missing, the host doesn't answer, or the
    output is malformed.
    """
    if not hostname:
        return None
    try:
        r = subprocess.run(
            ['avahi-resolve', '-4', '-n', hostname],
            capture_output=True, text=True, timeout=timeout)
    except FileNotFoundError:
        log.debug("avahi-resolve not installed; cannot resolve %s", hostname)
        return None
    except subprocess.TimeoutExpired:
        log.warning("mDNS resolve timed out for %s", hostname)
        return None

    if r.returncode != 0 or not r.stdout.strip():
        return None

    # Expected output: "warpig.local\t192.168.1.183\n"
    parts = r.stdout.strip().split()
    if len(parts) >= 2:
        candidate = parts[-1].strip()
        try:
            ipaddress.IPv4Address(candidate)
            return candidate
        except ValueError:
            pass
    return None


def _subnet_scan(reference_ip: str, port: int, timeout: float = 0.4) -> Optional[str]:
    """Walk the /24 of `reference_ip` looking for the first host with `port` open.

    Used as a last resort when both cached IP and mDNS fail. Slow (sequential
    probes with short timeout). Returns the first match or None.
    """
    if not reference_ip:
        return None
    try:
        net = ipaddress.IPv4Network(f'{reference_ip}/24', strict=False)
    except ValueError:
        return None

    log.info("Subnet-scanning %s for port %d (fallback)…", net, port)
    for ip in net.hosts():
        s = str(ip)
        if s == reference_ip:
            continue
        if is_reachable(s, port, timeout=timeout):
            log.info("Subnet scan hit: %s:%d", s, port)
            return s
    return None


def discover_host(
    cached_host: str,
    hostname: str,
    port: int = 22,
    *,
    subnet_scan_fallback: bool = False,
) -> Optional[str]:
    """Find the current working IP for a source.

    Tries: cached IP → mDNS hostname → (optional) subnet scan around cached IP.
    Returns the working IP, or None if nothing answered.
    """
    if is_reachable(cached_host, port):
        return cached_host

    if hostname:
        resolved = resolve_mdns(hostname)
        if resolved and resolved != cached_host and is_reachable(resolved, port):
            log.info("mDNS rediscovery: %s → %s (was %s)", hostname, resolved, cached_host)
            return resolved

    if subnet_scan_fallback and cached_host:
        hit = _subnet_scan(cached_host, port)
        if hit:
            return hit

    return None
