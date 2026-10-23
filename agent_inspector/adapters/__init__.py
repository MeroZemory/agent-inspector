"""Adapter registry. To add a source (e.g. Orca metadata), add a module exposing
PROVIDER, discover(root), parse(job), probe_last_ts(path), home_metadata(root) and register it here."""
from . import agy, claude, codex, cc_transcripts

ADAPTERS = {
    "claude": claude,
    "codex": codex,
    "cc_transcripts": cc_transcripts,
    "agy": agy,
}
