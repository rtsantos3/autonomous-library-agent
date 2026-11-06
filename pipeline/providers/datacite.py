"""DataCite work metadata: the registry for arXiv, Zenodo, and thesis DOIs.

Crossref holds only publisher-registered DOIs, so DataCite-registered DOIs
(e.g. ``10.48550/arXiv.*``, university repositories) resolve no title there.
"""

from __future__ import annotations

import logging
from typing import Optional
from urllib.parse import quote

import requests

from pipeline._http import DATACITE_LIMITER, http_get
from pipeline._utils import bare_doi
from pipeline.citation_types import text, year

logger = logging.getLogger(__name__)


def parse_metadata(payload) -> Optional[dict]:
    """Normalize a DataCite /dois/<doi> response to resolve_identity fields."""
    data = payload.get("data") if isinstance(payload, dict) else None
    attrs = data.get("attributes") if isinstance(data, dict) else None
    if not isinstance(attrs, dict):
        return None
    titles = [
        text(t.get("title")) for t in attrs.get("titles") or [] if isinstance(t, dict)
    ]
    authors = []
    for creator in attrs.get("creators") or []:
        if not isinstance(creator, dict):
            continue
        given, family = text(creator.get("givenName")), text(creator.get("familyName"))
        name = f"{given} {family}".strip() if family else text(creator.get("name"))
        if name:
            authors.append(name)
    abstracts = [
        text(d.get("description"))
        for d in attrs.get("descriptions") or []
        if isinstance(d, dict) and d.get("descriptionType") == "Abstract"
    ]
    container = (
        attrs.get("container") if isinstance(attrs.get("container"), dict) else {}
    )
    published = year(attrs.get("publicationYear"))
    return {
        "title": next((t for t in titles if t), None),
        "doi": bare_doi(text(attrs.get("doi"))),
        "authors": authors,
        "year": str(published) if published else None,
        "venue": text(container.get("title")) or text(attrs.get("publisher")) or None,
        "abstract": next((a for a in abstracts if a), None),
    }


def metadata(doi: str) -> Optional[dict]:
    try:
        response = http_get(
            f"https://api.datacite.org/dois/{quote(bare_doi(doi) or '', safe='/')}",
            limiter=DATACITE_LIMITER,
            timeout=30,
        )
        return parse_metadata(response.json())
    except (requests.RequestException, ValueError):
        logger.warning("DataCite metadata doi=%s failed", doi)
        return None
