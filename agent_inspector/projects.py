"""Derive a project from a session cwd. Always marked as derived (sessions.project_method).

Methods, in order:
  git-worktree : cwd is inside a git worktree whose .git file points at <main>/.git/worktrees/<name>
  git          : nearest ancestor containing a .git directory
  pattern      : path no longer exists; '<base>/.claude/worktrees/<x>', '<base>/.worktrees/<x>',
                 '<base>-worktrees/<x>' are mapped to <base>
  cwd          : fallback, the cwd itself
"""
from __future__ import annotations

import os
import re

from .util import norm_path

_cache: dict[str, tuple] = {}
_PATTERNS = [
    re.compile(r"^(.*?)/\.claude/worktrees/[^/]+"),
    re.compile(r"^(.*?)/\.worktrees/[^/]+"),
    re.compile(r"^(.*?)/\.codex/worktrees/[^/]+"),
    re.compile(r"^(.*?)-worktrees/[^/]+"),
]


def _git_root(p: str):
    cur = p
    for _ in range(12):
        g = os.path.join(cur, ".git")
        try:
            if os.path.isdir(g):
                return cur, "git"
            if os.path.isfile(g):
                try:
                    with open(g, encoding="utf-8", errors="replace") as fh:
                        line = fh.readline().strip()
                except OSError:
                    line = ""
                if line.startswith("gitdir:"):
                    gd = norm_path(line[7:].strip())
                    m = re.match(r"^(.*)/\.git/worktrees/[^/]+$", gd or "")
                    if m:
                        return m.group(1), "git-worktree"
                return cur, "git"
        except OSError:
            return None
        parent = os.path.dirname(cur)
        if parent == cur:
            break
        cur = parent
    return None


def derive(cwd: str | None, fallback: str | None = None):
    """Return (project_name, project_root, method)."""
    n = norm_path(cwd)
    if not n:
        return (fallback, None, "slug" if fallback else None)
    if n in _cache:
        return _cache[n]
    res = None
    if os.path.isdir(n):
        g = _git_root(n)
        if g:
            root = norm_path(g[0])
            res = (os.path.basename(root), root, g[1])
    if res is None:
        for pat in _PATTERNS:
            m = pat.match(n)
            if m:
                base = m.group(1)
                res = (os.path.basename(base), base, "pattern")
                break
    if res is None:
        res = (os.path.basename(n) or n, n, "cwd")
    _cache[n] = res
    return res
