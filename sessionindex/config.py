"""Source-root configuration: a list of roots, each bound to a pluggable adapter."""
from __future__ import annotations

import glob
import json
import os
from dataclasses import dataclass, field

from .util import expand

PKG_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(PKG_DIR)
DATA_DIR = os.environ.get("SX_DATA_DIR") or os.path.join(PROJECT_DIR, "data")
DB_PATH = os.environ.get("SX_DB") or os.path.join(DATA_DIR, "index.sqlite")
ANN_PATH = os.environ.get("SX_ANN_DB") or os.path.join(DATA_DIR, "annotations.sqlite")
SOURCES_PATH = os.environ.get("SX_SOURCES") or os.path.join(PROJECT_DIR, "sources.json")

DEFAULT_CAPS = {"tool_output_chars": 4096, "tool_args_chars": 262144, "injected_chars": 4096}


@dataclass
class Root:
    root_id: str
    adapter: str
    path: str
    label: str
    rank: int = 5
    is_test: bool = False
    extra: dict = field(default_factory=dict)


def load_config(path: str | None = None) -> dict:
    p = path or SOURCES_PATH
    with open(p, encoding="utf-8") as fh:
        cfg = json.load(fh)
    caps = dict(DEFAULT_CAPS)
    caps.update(cfg.get("caps") or {})
    cfg["caps"] = caps
    return cfg


def expand_roots(cfg: dict) -> list[Root]:
    out: list[Root] = []
    seen: set[str] = set()
    skip = [s.lower() for s in cfg.get("skip_path_substrings") or []]
    for ent in cfg.get("roots") or []:
        if ent.get("enabled") is False:
            continue
        paths: list[str]
        if ent.get("glob"):
            paths = sorted(glob.glob(expand(ent["glob"])))
        else:
            paths = [expand(ent["path"])]
        for p in paths:
            if not os.path.isdir(p):
                continue
            if any(s in p.lower() for s in skip):
                continue
            name = os.path.basename(p)
            parent = os.path.basename(os.path.dirname(p))
            label = ent.get("label", name).format(name=name, parent=parent, parent8=parent[:8])
            rid = f"{ent['adapter']}:{label}"
            if rid in seen:
                continue
            seen.add(rid)
            out.append(Root(rid, ent["adapter"], p, label, int(ent.get("rank", 5)), bool(ent.get("is_test")),
                            {k: v for k, v in ent.items() if k not in ("adapter", "path", "glob", "label", "rank", "is_test")}))
    return out
