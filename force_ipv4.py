"""
force_ipv4.py — Prefer IPv4 for all outbound connections.

Some networks advertise IPv6 but can't actually route it. Python's socket
stack tries the IPv6 address first and waits the full ~75s TCP timeout before
falling back to IPv4, so every Google Sheets / API call appears to hang. gspread
has no request timeout, so the scraper freezes indefinitely at the first
Sheets call.

Importing this module (which happens via credential_manager, so every entry
point picks it up) filters DNS results to IPv4, eliminating the stall. It falls
back to the original result for hosts that only have IPv6, so it's safe to leave
on everywhere — including the droplet, which has working IPv4.
"""

import socket

_orig_getaddrinfo = socket.getaddrinfo


def _ipv4_only_getaddrinfo(*args, **kwargs):
    results = _orig_getaddrinfo(*args, **kwargs)
    ipv4 = [ai for ai in results if ai[0] == socket.AF_INET]
    return ipv4 or results  # fall back if a host is IPv6-only


# Idempotent: don't double-wrap if imported more than once.
if getattr(socket.getaddrinfo, "_ipv4_forced", False) is False:
    _ipv4_only_getaddrinfo._ipv4_forced = True
    socket.getaddrinfo = _ipv4_only_getaddrinfo
