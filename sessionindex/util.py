"""Small shared helpers (stdlib only)."""
from __future__ import annotations

import base64
import datetime as _dt
import hashlib
import json
import os
import re

B32 = "abcdefghijklmnopqrstuvwxyz234567"
PROVIDER_PREFIX = {"claude": "c", "codex": "x", "opencode": "o", "orca": "r"}


def short_sid(provider: str, pid: str) -> str:
    """Stable short session id: provider letter + 9 base32 chars of sha1(provider:pid)."""
    h = hashlib.sha1(f"{provider}:{pid}".encode("utf-8")).digest()
    enc = base64.b32encode(h).decode("ascii").lower()
    return PROVIDER_PREFIX.get(provider, "z") + enc[:9]


def text_hash(s: str | None) -> int | None:
    """64-bit signed content hash used to detect inherited/duplicated messages."""
    if not s:
        return None
    norm = " ".join(s.split())
    d = hashlib.blake2b(norm.encode("utf-8", "replace"), digest_size=8).digest()
    return int.from_bytes(d, "big", signed=True)


def trunc(s: str | None, n: int) -> tuple[str | None, int, int]:
    """Return (possibly truncated text, original length, truncated flag)."""
    if s is None:
        return None, 0, 0
    L = len(s)
    if L <= n:
        return s, L, 0
    return s[:n], L, 1


def ms_to_iso(ms) -> str | None:
    try:
        v = float(ms)
    except (TypeError, ValueError):
        return None
    if v > 1e14:  # nanoseconds
        v = v / 1e6
    elif v < 1e11:  # seconds
        v = v * 1000
    return _dt.datetime.fromtimestamp(v / 1000, _dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


_ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}")


def norm_ts(ts) -> str | None:
    """Normalize provider timestamps to ISO-8601 UTC strings (sortable)."""
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        return ms_to_iso(ts)
    s = str(ts)
    if _ISO_RE.match(s):
        if s.endswith("Z"):
            return s
        try:
            d = _dt.datetime.fromisoformat(s)
            if d.tzinfo is None:
                d = d.replace(tzinfo=_dt.timezone.utc)
            return d.astimezone(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        except ValueError:
            return s
    if s.isdigit():
        return ms_to_iso(int(s))
    return s


def iso_to_epoch(s: str | None) -> float | None:
    if not s:
        return None
    try:
        return _dt.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def jdump(o) -> str:
    return json.dumps(o, ensure_ascii=False, separators=(",", ":"), default=str)


def norm_path(p: str | None) -> str | None:
    """Normalize a filesystem path for comparison: forward slashes, lowercase drive."""
    if not p:
        return p
    p = str(p)
    if p.startswith("file:///"):
        p = p[8:]
    if p.startswith("\\\\?\\"):
        p = p[4:]
    p = p.replace("\\", "/")
    if len(p) >= 2 and p[1] == ":":
        p = p[0].upper() + p[1:]
    while len(p) > 3 and p.endswith("/"):
        p = p[:-1]
    return p


def expand(p: str) -> str:
    return os.path.normpath(os.path.expanduser(os.path.expandvars(p)))
