from __future__ import annotations

import re
from typing import List, Optional

__all__: List[str] = [
    "slugify",
    "pub_type_slug",
    "canonical_type_tag",
    "extend_unique",
    "bare_doi",
]

# camelCase publication-type slugs — as historically stored when plain slugify()
# ran on Semantic Scholar's camelCase types — mapped to the canonical hyphenated
# slug that pub_type_slug now emits. Used to heal type:* tags carried forward
# from older ingests so one node never holds both "type:journalarticle" and
# "type:journal-article". Only unambiguous camelCase collapses are listed;
# PubMed plural/variant forms (e.g. "case-reports", "letter") are left untouched.
_CAMEL_TYPE_SLUGS = {
    "journalarticle": "journal-article",
    "clinicaltrial": "clinical-trial",
    "metaanalysis": "meta-analysis",
    "casereport": "case-report",
    "lettersandcomments": "letters-and-comments",
}


def slugify(text) -> Optional[str]:
    if text is None:
        return None
    slug = re.sub(r"[^0-9a-z]+", "-", str(text).strip().lower()).strip("-")
    return slug or None


def pub_type_slug(text) -> Optional[str]:
    # Publication types arrive in two shapes from different sources: Semantic
    # Scholar emits camelCase ("JournalArticle") while PubMed/Crossref emit
    # spaced ("Journal Article"). Plain slugify() collapses these to divergent
    # slugs ("journalarticle" vs "journal-article"), so the same node accrues
    # two type:* tags for one concept. Splitting camelCase boundaries first
    # canonicalizes both shapes to the same slug.
    if text is None:
        return None
    spaced = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", str(text))
    return slugify(spaced)


def canonical_type_tag(tag) -> str:
    # Heal a stale camelCase type:* tag in place; identity for everything else.
    # Lets the pipeline normalize type tags it carries forward from older nodes
    # without re-deriving from source metadata.
    text = str(tag)
    if text.startswith("type:"):
        canonical = _CAMEL_TYPE_SLUGS.get(text[len("type:"):])
        if canonical:
            return f"type:{canonical}"
    return text


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
