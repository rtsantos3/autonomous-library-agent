"""
Scaffold academic papers into the Trellis knowledge graph.

Usage:
    python ingest.py --doi 10.1038/nature11225
    python ingest.py --pmid 22722865
    python ingest.py --title "Dietary-fat-induced taurocholic acid"
    python ingest.py --batch-file papers.txt
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
except ImportError:
    pass

import requests

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TRELLIS_BIN = "/home/articulatus/.nvm/versions/node/v22.17.0/bin/trellis"
PROJECT_ROOT = "/home/articulatus/git_repos/autonomous_library_agent"
PARENT_SLUG = "microbiome-research-library"
ACTOR_ID = "ingest-pipeline"

NCBI_API_KEY = os.environ.get("NCBI_API_KEY", "")
S2_API_KEY = os.environ.get("S2_API_KEY", "")
CROSSREF_EMAIL = os.environ.get("CROSSREF_EMAIL", "")

MAX_DEPTH = 2

# Rate-limit intervals (seconds between requests)
RATE_S2 = 0.1 if S2_API_KEY else 1.0
RATE_PUBMED = 0.1 if NCBI_API_KEY else 0.34
RATE_CROSSREF = 0.02 if CROSSREF_EMAIL else 0.5
RATE_ARXIV = 3.0
RATE_OPENALEX = 0.1

_last_call = {}


def _rate_limit(source, interval):
    """Sleep if needed to respect rate limits for a given source."""
    now = time.monotonic()
    prev = _last_call.get(source, 0)
    elapsed = now - prev
    if elapsed < interval:
        time.sleep(interval - elapsed)
    _last_call[source] = time.monotonic()


def _backoff_get(url, headers=None, params=None, source="generic", interval=1.0, max_retries=5):
    """GET with exponential backoff on 429/503."""
    _rate_limit(source, interval)
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=30)
        except requests.RequestException as exc:
            print(f"  [warn] request failed ({exc}), retry {attempt+1}/{max_retries}")
            time.sleep(2 ** attempt)
            continue
        if resp.status_code in (429, 503):
            wait = 2 ** attempt
            print(f"  [warn] {resp.status_code} from {source}, backing off {wait}s")
            time.sleep(wait)
            continue
        return resp
    return None


# ---------------------------------------------------------------------------
# Trellis helpers
# ---------------------------------------------------------------------------

def trellis_run(*args):
    result = subprocess.run(
        [TRELLIS_BIN, *args],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"trellis {args[0]!r} failed (exit {result.returncode}):\n"
            f"  stdout: {result.stdout.strip()}\n"
            f"  stderr: {result.stderr.strip()}"
        )
    return result.stdout.strip()


def trellis_json(*args):
    out = trellis_run(*args)
    return json.loads(out) if out else None


def find_by_uri(uri):
    """Check if a node with this exact URI already exists."""
    try:
        out = trellis_run("find", "--text", uri, "--json")
    except RuntimeError:
        return None
    if not out:
        return None
    parsed = json.loads(out)
    nodes = parsed if isinstance(parsed, list) else parsed.get("results", parsed.get("nodes", []))
    for n in nodes:
        if n.get("uri") == uri:
            return n
    return None


def find_by_title(title):
    """Check if a node with this exact title already exists."""
    try:
        out = trellis_run("find", "--text", title[:80], "--json")
    except RuntimeError:
        return None
    if not out:
        return None
    parsed = json.loads(out)
    nodes = parsed if isinstance(parsed, list) else parsed.get("results", parsed.get("nodes", []))
    for n in nodes:
        if n.get("title", "").lower().strip() == title.lower().strip():
            return n
    return None


def node_exists(doi=None, title=None):
    """Return existing node dict or None."""
    if doi:
        uri = f"https://doi.org/{doi}"
        hit = find_by_uri(uri)
        if hit:
            return hit
    if title:
        return find_by_title(title)
    return None


def add_paper_node(title, abstract=None, doi=None, year=None, tags_extra=None, depth=0):
    """Create a custom node in Trellis. Returns the node dict."""
    uri = f"https://doi.org/{doi}" if doi else None

    tag_parts = []
    if tags_extra:
        tag_parts.extend(tags_extra)
    if year:
        tag_parts.append(f"year:{year}")
    tag_parts.append(f"depth:{depth}")

    args = ["add", "custom", title]
    if abstract:
        # Truncate very long abstracts to avoid CLI arg limits
        args += ["--description", abstract[:4000]]
    if uri:
        args += ["--uri", uri]
    if tag_parts:
        args += ["--tags", ",".join(tag_parts)]
    args += ["--parent", PARENT_SLUG]
    args += ["--actor-id", ACTOR_ID]
    args.append("--json")

    result = trellis_json(*args)
    # trellis add wraps the node in a response object
    if isinstance(result, dict) and "node" in result:
        return result["node"]
    return result


def link_papers(source_slug, target_slug):
    """Link source -> target with 'references' relation and 'cites' label."""
    try:
        return trellis_json(
            "link", source_slug, target_slug, "references",
            "--label", "cites",
            "--actor-id", ACTOR_ID,
            "--json",
        )
    except RuntimeError as exc:
        print(f"  [warn] failed to link {source_slug} -> {target_slug}: {exc}")
        return None


# ---------------------------------------------------------------------------
# Metadata fetchers
# ---------------------------------------------------------------------------

def _normalize_meta(meta):
    """Ensure meta dict has keys: title, abstract, doi, year, authors, pmid."""
    meta.setdefault("title", None)
    meta.setdefault("abstract", None)
    meta.setdefault("doi", None)
    meta.setdefault("year", None)
    meta.setdefault("authors", [])
    meta.setdefault("pmid", None)
    return meta


def fetch_pubmed(identifier):
    """Fetch metadata from PubMed E-utilities. identifier can be DOI or PMID."""
    params = {"db": "pubmed", "retmode": "xml"}
    if NCBI_API_KEY:
        params["api_key"] = NCBI_API_KEY

    # Resolve to PMID if needed
    pmid = None
    if identifier.isdigit():
        pmid = identifier
    else:
        # Search by DOI or title
        params_search = {**params, "term": identifier, "retmax": "1"}
        resp = _backoff_get(
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
            params=params_search, source="pubmed", interval=RATE_PUBMED,
        )
        if resp and resp.ok:
            root = ET.fromstring(resp.text)
            id_el = root.find(".//Id")
            if id_el is not None:
                pmid = id_el.text

    if not pmid:
        return None

    # Fetch full record
    resp = _backoff_get(
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi",
        params={**params, "id": pmid},
        source="pubmed", interval=RATE_PUBMED,
    )
    if not resp or not resp.ok:
        return None

    root = ET.fromstring(resp.text)
    article = root.find(".//Article")
    if article is None:
        return None

    meta = _normalize_meta({})
    meta["pmid"] = pmid

    title_el = article.find(".//ArticleTitle")
    if title_el is not None and title_el.text:
        meta["title"] = title_el.text.strip()

    abs_el = article.find(".//AbstractText")
    if abs_el is not None and abs_el.text:
        meta["abstract"] = abs_el.text.strip()

    # DOI
    for aid in root.findall(".//ArticleId"):
        if aid.get("IdType") == "doi" and aid.text:
            meta["doi"] = aid.text.strip()
            break

    # Year
    pd = article.find(".//PubDate/Year")
    if pd is not None and pd.text:
        meta["year"] = pd.text.strip()

    return meta if meta["title"] else None


def fetch_semantic_scholar(identifier):
    """Fetch metadata from Semantic Scholar."""
    headers = {}
    if S2_API_KEY:
        headers["x-api-key"] = S2_API_KEY

    # Try DOI first, then title search
    if re.match(r"^10\.\d{4,}/", identifier):
        url = f"https://api.semanticscholar.org/graph/v1/paper/DOI:{identifier}"
    elif identifier.isdigit():
        url = f"https://api.semanticscholar.org/graph/v1/paper/PMID:{identifier}"
    else:
        # Search by title
        resp = _backoff_get(
            "https://api.semanticscholar.org/graph/v1/paper/search",
            headers=headers,
            params={"query": identifier, "limit": "1", "fields": "title,abstract,externalIds,year,authors"},
            source="s2", interval=RATE_S2,
        )
        if not resp or not resp.ok:
            return None
        data = resp.json()
        papers = data.get("data", [])
        if not papers:
            return None
        pid = papers[0].get("paperId")
        if not pid:
            return None
        url = f"https://api.semanticscholar.org/graph/v1/paper/{pid}"

    resp = _backoff_get(
        url, headers=headers,
        params={"fields": "title,abstract,externalIds,year,authors"},
        source="s2", interval=RATE_S2,
    )
    if not resp or not resp.ok:
        return None

    data = resp.json()
    meta = _normalize_meta({})
    meta["title"] = data.get("title")
    meta["abstract"] = data.get("abstract")
    meta["year"] = str(data["year"]) if data.get("year") else None
    ext = data.get("externalIds", {})
    meta["doi"] = ext.get("DOI")
    meta["pmid"] = ext.get("PubMed")
    meta["s2_id"] = data.get("paperId")

    return meta if meta["title"] else None


def fetch_crossref(identifier):
    """Fetch metadata from Crossref. identifier should be a DOI."""
    if not re.match(r"^10\.\d{4,}/", identifier):
        return None

    params = {}
    if CROSSREF_EMAIL:
        params["mailto"] = CROSSREF_EMAIL

    resp = _backoff_get(
        f"https://api.crossref.org/works/{identifier}",
        params=params, source="crossref", interval=RATE_CROSSREF,
    )
    if not resp or not resp.ok:
        return None

    msg = resp.json().get("message", {})
    meta = _normalize_meta({})
    titles = msg.get("title", [])
    meta["title"] = titles[0] if titles else None
    meta["abstract"] = msg.get("abstract")  # may contain HTML tags
    if meta["abstract"]:
        meta["abstract"] = re.sub(r"<[^>]+>", "", meta["abstract"]).strip()
    meta["doi"] = msg.get("DOI")
    issued = msg.get("issued", {}).get("date-parts", [[]])
    if issued and issued[0]:
        meta["year"] = str(issued[0][0])

    return meta if meta["title"] else None


def fetch_arxiv(identifier):
    """Fetch metadata from arXiv API. Works best with arXiv IDs or titles."""
    if re.match(r"^10\.\d{4,}/", identifier):
        query = f"doi:{identifier}"
    elif identifier.isdigit():
        return None  # PMIDs not useful for arXiv
    else:
        query = f"ti:{identifier}"

    resp = _backoff_get(
        "http://export.arxiv.org/api/query",
        params={"search_query": query, "max_results": "1"},
        source="arxiv", interval=RATE_ARXIV,
    )
    if not resp or not resp.ok:
        return None

    ns = {"atom": "http://www.w3.org/2005/Atom"}
    root = ET.fromstring(resp.text)
    entry = root.find("atom:entry", ns)
    if entry is None:
        return None

    meta = _normalize_meta({})
    t = entry.find("atom:title", ns)
    meta["title"] = t.text.strip() if t is not None and t.text else None
    s = entry.find("atom:summary", ns)
    meta["abstract"] = s.text.strip() if s is not None and s.text else None
    pub = entry.find("atom:published", ns)
    if pub is not None and pub.text:
        meta["year"] = pub.text[:4]

    # Try to get DOI from arxiv links
    for link in entry.findall("atom:link", ns):
        href = link.get("href", "")
        if "doi.org" in href:
            meta["doi"] = href.replace("https://doi.org/", "").replace("http://doi.org/", "")

    return meta if meta["title"] else None


def fetch_openalex(identifier):
    """Fetch metadata from OpenAlex."""
    if re.match(r"^10\.\d{4,}/", identifier):
        url = f"https://api.openalex.org/works/doi:{identifier}"
    elif identifier.isdigit():
        url = f"https://api.openalex.org/works/pmid:{identifier}"
    else:
        resp = _backoff_get(
            "https://api.openalex.org/works",
            params={"search": identifier, "per_page": "1"},
            source="openalex", interval=RATE_OPENALEX,
        )
        if not resp or not resp.ok:
            return None
        results = resp.json().get("results", [])
        if not results:
            return None
        url = results[0].get("id")
        if not url:
            return None

    resp = _backoff_get(url, source="openalex", interval=RATE_OPENALEX)
    if not resp or not resp.ok:
        return None

    data = resp.json()
    meta = _normalize_meta({})
    meta["title"] = data.get("display_name") or data.get("title")
    # Abstract from inverted index
    inv = data.get("abstract_inverted_index")
    if inv:
        words = sorted(
            ((pos, word) for word, positions in inv.items() for pos in positions),
            key=lambda x: x[0],
        )
        meta["abstract"] = " ".join(w for _, w in words)
    meta["doi"] = (data.get("doi") or "").replace("https://doi.org/", "") or None
    meta["year"] = str(data["publication_year"]) if data.get("publication_year") else None

    return meta if meta["title"] else None


# ---------------------------------------------------------------------------
# Metadata resolution (try sources in priority order)
# ---------------------------------------------------------------------------

FETCHERS = [
    ("PubMed", fetch_pubmed),
    ("Semantic Scholar", fetch_semantic_scholar),
    ("Crossref", fetch_crossref),
    ("arXiv", fetch_arxiv),
    ("OpenAlex", fetch_openalex),
]


def resolve_metadata(identifier):
    """Try each source in order, return first successful metadata dict."""
    for name, fetcher in FETCHERS:
        print(f"  Trying {name}...", end=" ", flush=True)
        try:
            meta = fetcher(identifier)
        except Exception as exc:
            print(f"error: {exc}")
            continue
        if meta and meta.get("title"):
            print(f"OK ({meta['title'][:60]})")
            return meta
        print("no result")
    return None


# ---------------------------------------------------------------------------
# Citation graph expansion
# ---------------------------------------------------------------------------

def resolve_s2_id(doi):
    """Get Semantic Scholar paper ID from DOI."""
    if not doi:
        return None
    headers = {"x-api-key": S2_API_KEY} if S2_API_KEY else {}
    resp = _backoff_get(
        f"https://api.semanticscholar.org/graph/v1/paper/DOI:{doi}",
        headers=headers,
        params={"fields": "paperId"},
        source="s2", interval=RATE_S2,
    )
    if resp and resp.ok:
        return resp.json().get("paperId")
    return None


def _fetch_s2_edges(s2_id, direction):
    """Fetch references or citations from S2. direction = 'references' or 'citations'."""
    headers = {"x-api-key": S2_API_KEY} if S2_API_KEY else {}
    fields = "title,externalIds,year"
    resp = _backoff_get(
        f"https://api.semanticscholar.org/graph/v1/paper/{s2_id}/{direction}",
        headers=headers,
        params={"fields": fields, "limit": "100"},
        source="s2", interval=RATE_S2,
    )
    if not resp or not resp.ok:
        return []
    data = resp.json().get("data", [])
    results = []
    for item in data:
        paper = item.get("citedPaper" if direction == "references" else "citingPaper", {})
        if paper and paper.get("title"):
            ext = paper.get("externalIds") or {}
            results.append({
                "title": paper["title"],
                "doi": ext.get("DOI"),
                "year": str(paper["year"]) if paper.get("year") else None,
                "s2_id": paper.get("paperId"),
            })
    return results


def expand_citations(source_slug, source_doi, current_depth):
    """
    Expand citation graph from a paper. Creates queued nodes for
    cited/citing papers and links them.
    """
    if current_depth >= MAX_DEPTH:
        return

    s2_id = resolve_s2_id(source_doi)
    if not s2_id:
        print(f"  Could not resolve S2 ID for DOI {source_doi}, skipping citation expansion")
        return

    # Outbound: papers this paper references
    print(f"  Fetching outbound references (depth {current_depth})...")
    refs = _fetch_s2_edges(s2_id, "references")
    print(f"  Found {len(refs)} references")
    for ref in refs:
        target_slug = _ensure_queued_node(ref, current_depth + 1)
        if target_slug:
            link_papers(source_slug, target_slug)

    # Inbound: papers that cite this paper
    print(f"  Fetching inbound citations (depth {current_depth})...")
    cits = _fetch_s2_edges(s2_id, "citations")
    print(f"  Found {len(cits)} citations")
    for cit in cits:
        target_slug = _ensure_queued_node(cit, current_depth + 1)
        if target_slug:
            link_papers(target_slug, source_slug)


def _ensure_queued_node(paper_info, depth):
    """
    Check if a paper already exists in Trellis; if not, create a minimal
    queued node. Returns the node slug or None.
    """
    doi = paper_info.get("doi")
    title = paper_info.get("title", "")

    existing = node_exists(doi=doi, title=title)
    if existing:
        return existing.get("slug")

    if not title:
        return None

    try:
        node = add_paper_node(
            title=title,
            doi=doi,
            year=paper_info.get("year"),
            tags_extra=["pipeline:queued"],
            depth=depth,
        )
        slug = node.get("slug")
        print(f"    + queued: {title[:60]} (depth:{depth})")
        return slug
    except RuntimeError as exc:
        print(f"    [warn] failed to add queued node '{title[:40]}': {exc}")
        return None


# ---------------------------------------------------------------------------
# Main ingestion logic
# ---------------------------------------------------------------------------

def ingest_paper(identifier, depth=0):
    """
    Full ingestion of a single paper: dedup, fetch metadata, create node,
    expand citation graph.
    """
    identifier = identifier.strip()
    if not identifier:
        return

    print(f"\n{'='*70}")
    print(f"Ingesting: {identifier}")
    print(f"{'='*70}")

    # Determine identifier type for dedup check
    doi_match = re.match(r"^(10\.\d{4,}/.+)$", identifier)

    # Dedup check
    if doi_match:
        uri = f"https://doi.org/{doi_match.group(1)}"
        existing = find_by_uri(uri)
        if existing:
            print(f"  SKIP: already exists as '{existing.get('title', '?')}' (slug: {existing.get('slug')})")
            return existing.get("slug")
    else:
        existing = find_by_title(identifier) if not identifier.isdigit() else None
        if existing:
            print(f"  SKIP: already exists as '{existing.get('title', '?')}' (slug: {existing.get('slug')})")
            return existing.get("slug")

    # Fetch metadata
    print("  Resolving metadata...")
    meta = resolve_metadata(identifier)
    if not meta:
        print("  ERROR: could not resolve metadata from any source. Skipping.")
        return None

    # If we got a DOI from metadata, do another dedup check
    if meta.get("doi") and not doi_match:
        uri = f"https://doi.org/{meta['doi']}"
        existing = find_by_uri(uri)
        if existing:
            print(f"  SKIP (post-resolve): already exists (slug: {existing.get('slug')})")
            return existing.get("slug")

    # Create the node
    print(f"  Creating node: {meta['title'][:70]}")
    try:
        node = add_paper_node(
            title=meta["title"],
            abstract=meta.get("abstract"),
            doi=meta.get("doi"),
            year=meta.get("year"),
            tags_extra=["pipeline:scaffolded"],
            depth=depth,
        )
    except RuntimeError as exc:
        print(f"  ERROR creating node: {exc}")
        return None

    slug = node.get("slug")
    print(f"  Created: slug={slug}")

    # Citation graph expansion
    if meta.get("doi"):
        expand_citations(slug, meta["doi"], depth)
    else:
        print("  No DOI available, skipping citation expansion")

    return slug


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Scaffold academic papers into the Trellis knowledge graph.",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--doi", help="Paper DOI (e.g. 10.1038/nature11225)")
    group.add_argument("--pmid", help="PubMed ID (e.g. 22722865)")
    group.add_argument("--title", help="Paper title to search for")
    group.add_argument("--batch-file", help="File with one identifier per line")

    args = parser.parse_args()

    if args.batch_file:
        path = Path(args.batch_file)
        if not path.exists():
            print(f"Error: batch file not found: {path}", file=sys.stderr)
            sys.exit(1)
        identifiers = [
            line.strip() for line in path.read_text().splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]
        print(f"Batch mode: {len(identifiers)} identifiers")
        for ident in identifiers:
            try:
                ingest_paper(ident)
            except Exception as exc:
                print(f"  FATAL ERROR on '{ident}': {exc}")
                continue
    else:
        identifier = args.doi or args.pmid or args.title
        ingest_paper(identifier)

    print("\nDone.")


if __name__ == "__main__":
    main()
