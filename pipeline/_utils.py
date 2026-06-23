from __future__ import annotations

import re
from typing import List, Optional

__all__: List[str] = ["slugify", "extend_unique", "bare_doi"]


def slugify(text) -> Optional[str]:
    if text is None:
        return None
    slug = re.sub(r"[^0-9a-z]+", "-", str(text).strip().lower()).strip("-")
    return slug or None


def extend_unique(target: list, incoming) -> list:
    # Centralize the primitive: ingestion.py and aggregator.py had duplicated
    # variants with divergent signatures, so callers now adapt to this list form.
    seen = set(target)
    for value in incoming or []:
        if value in (None, "", []):
            continue
        text = str(value).strip()
        if text and text not in seen:
            target.append(text)
            seen.add(text)
    return target


def bare_doi(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
    lower = value.lower()
    for prefix in ("doi:", "https://doi.org/", "http://dx.doi.org/"):
        if lower.startswith(prefix):
            value = value[len(prefix):]
            break
    return value.strip().lower() or None
