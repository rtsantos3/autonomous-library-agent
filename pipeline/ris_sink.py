"""RIS sink: append resolved papers to a .ris file.

Independent of Trellis. Two entry points share one code path:

- Callable:  ``RisSink(path).append(paper)``. ``append`` is a bound method, so
  it can be handed to any producer as a callback.
- Intake:    ``ingest_batch(items, ris_sink=RisSink())`` writes every paper the
  pipeline resolved. With no ``ris_sink`` argument the hook is active only when
  ``RIS_SINK_PATH`` is set, so existing callers are unchanged.

Path precedence: explicit ``path`` -> ``RIS_SINK_PATH`` env -> DEFAULT_PATH.
Records are deduped against the file by DOI, else by normalized title, so the
file can be appended to across runs.
"""

from __future__ import annotations

import os
import re
import threading
from pathlib import Path
from typing import Any, Optional

from pipeline.trellis import _norm_title

DEFAULT_PATH = Path.home() / "git_repos" / "vault" / "references" / "library.ris"

_TAG = re.compile(r"^([A-Z][A-Z0-9])  - (.*)$")


def _get(paper: Any, name: str):
    """Read a field from a ResolveResult/ParseResult-like object or a dict."""
    if isinstance(paper, dict):
        return paper.get(name)
    return getattr(paper, name, None)


def _ris_author(name: str) -> str:
    """RIS AU wants 'Family, Given'. Keep names that already have a comma."""
    name = " ".join(str(name).split())
    if "," in name or " " not in name:
        return name
    given, family = name.rsplit(" ", 1)
    return f"{family}, {given}"


def _one_line(value: Any) -> str:
    return " ".join(str(value).split())


def format_record(paper: Any) -> str:
    """Render one paper as an RIS record (TY .. ER)."""
    title = _one_line(_get(paper, "title") or "")
    if not title:
        raise ValueError("RIS record needs a title")
    lines = ["TY  - JOUR", f"TI  - {title}"]
    for author in _get(paper, "authors") or []:
        if author:
            lines.append(f"AU  - {_ris_author(author)}")
    for tag, field in (("PY", "year"), ("JO", "venue"), ("DO", "doi")):
        value = _get(paper, field)
        if value:
            lines.append(f"{tag}  - {_one_line(value)}")
    doi = _get(paper, "doi")
    if doi:
        lines.append(f"UR  - https://doi.org/{_one_line(doi)}")
    pmid = _get(paper, "pmid")
    if pmid:
        lines.append(f"AN  - PMID:{_one_line(pmid)}")
    for keyword in _get(paper, "keywords") or []:
        if keyword:
            lines.append(f"KW  - {_one_line(keyword)}")
    abstract = _get(paper, "abstract")
    if abstract:
        lines.append(f"AB  - {_one_line(abstract)}")
    lines.append("ER  - ")
    return "\n".join(lines) + "\n\n"


def _bare_doi(doi: Optional[str]) -> str:
    """Normalize DOI spellings (doi:, doi.org URLs, case) to one dedup key."""
    value = (doi or "").strip()
    value = re.sub(r"^doi:\s*", "", value, flags=re.I)
    value = re.sub(r"^https?://(dx\.)?doi\.org/", "", value, flags=re.I)
    return value.strip().rstrip(" .;,").lower()


def _record_key(doi: Optional[str], title: Optional[str]) -> str:
    bare = _bare_doi(doi)
    if bare:
        return "doi:" + bare
    return "title:" + _norm_title(title or "")


class RisSink:
    """Append-only, deduping RIS writer. Safe to share across threads."""

    def __init__(self, path: Optional[os.PathLike | str] = None):
        self.path = Path(path or os.environ.get("RIS_SINK_PATH") or DEFAULT_PATH)
        self._lock = threading.Lock()
        self._keys: Optional[set[str]] = None

    def _load_keys(self) -> set[str]:
        keys: set[str] = set()
        if not self.path.exists():
            return keys
        doi = title = None
        for line in self.path.read_text(encoding="utf-8").splitlines():
            match = _TAG.match(line)
            if match and match.group(1) == "DO":
                doi = match.group(2).strip()
            elif match and match.group(1) in ("TI", "T1"):
                title = match.group(2).strip()
            elif line.startswith("ER  -"):
                keys.add(_record_key(doi, title))
                doi = title = None
        return keys

    def append(self, paper: Any) -> bool:
        """Write ``paper`` unless the file already has it. Returns True if written."""
        key = _record_key(_get(paper, "doi"), _get(paper, "title"))
        record = format_record(paper)
        with self._lock:
            if self._keys is None:
                self._keys = self._load_keys()
            if key in self._keys:
                return False
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(record)
            self._keys.add(key)
            return True

    __call__ = append


def sink_from_env() -> Optional[RisSink]:
    """The intake default: a sink only when RIS_SINK_PATH is set."""
    return RisSink() if os.environ.get("RIS_SINK_PATH") else None
