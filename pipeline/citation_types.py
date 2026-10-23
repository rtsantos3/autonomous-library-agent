"""Provider-independent outbound citation records."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class CitationItem:
    doi: Optional[str]
    pmid: Optional[str]
    s2_id: Optional[str]
    title: str
    year: Optional[int]
    sources: list[str] = field(default_factory=list)
    openalex_id: Optional[str] = None
    omid: Optional[str] = None


@dataclass
class CitationResult:
    source: str
    retrieved_at: str
    items: list[CitationItem]
    providers: dict[str, dict] = field(default_factory=dict)


def text(value) -> str:
    return value.strip() if isinstance(value, str) else ""


def year(value) -> Optional[int]:
    if isinstance(value, (int, str)) and str(value).isdigit():
        return int(value)
    return None
