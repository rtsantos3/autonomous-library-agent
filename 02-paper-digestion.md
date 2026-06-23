# Paper Digestion

## Goal
Extract structured, machine-readable content from scaffolded papers — turning raw PDFs or HTML into rich data attached to existing graph nodes.

## Overview
Digestion is the second pass. By this point, paper nodes exist in the graph from the ingestion phase. Now we pull the actual content — full text, sections, figures, tables, equations, and references — and store it in a structured format tied to each Paper node.

## What Gets Extracted

### 1. Document Structure
- **Sections** — Introduction, Methods, Results, Discussion, Conclusion, etc.
  - Each section as a discrete block with title, order, and content
- **Paragraphs** — individual paragraphs within sections
- **Headings** — hierarchical (H1, H2, H3)

### 2. Rich Content
- **Figures** — image data + caption + referenced section context
- **Tables** — structured table data (rows/columns) + caption
- **Equations** — LaTeX representation where extractable
- **Code / Algorithms** — pseudocode or actual code blocks

### 3. References (Structured)
- Parse the bibliography into structured entries
- Link each to existing scaffolded Paper nodes where possible
- Capture in-text citation locations (which reference appears in which paragraph)

### 4. Metadata Enrichment
- **Abstract** — fill in if missing from ingestion
- **Keywords** — extract or infer from content
- **Contributions** — summarized bullet points of key claims
- **Methodology type** — empirical, theoretical, survey, meta-analysis, etc.

## Storage Schema

### Additional Node Properties (on Paper)
- `full_text` — raw extracted text
- `sections` — JSON array of {title, content, order}
- `figures` — JSON array of {caption, path, section_ref}
- `tables` — JSON array of {caption, data, section_ref}
- `equations` — JSON array of {latex, section_ref}
- `structured_references` — JSON array of parsed bib entries
- `contributions` — list of extracted contribution statements
- `methodology_type` — inferred type
- `status` — updated to `digested`

### New Edge Types
- **CITES_WITH_CONTEXT** — replaces or enriches CITES edges
  - `in_text_location` — paragraph/section where citation appears
  - `surrounding_text` — context window around the citation
  - `purpose` — (optional, inferred) e.g. "extends", "contradicts", "builds on", "compares with"

## Digestion Pipeline Steps

1. **Source Acquisition**
   - Download PDF from arXiv, publisher, or open access
   - Fallback: Semantic Scholar hosted PDF, Unpaywall
   - For HTML papers (e.g. PMLR, NeurIPS proceedings): scrape structured HTML directly

2. **Content Extraction**
   - **PDF path**: Use GROBID or marker for structured extraction
   - **HTML path**: Parse DOM directly — cleaner output
   - Extract sections, paragraphs, figures, tables, equations
   - Normalize into schema format

3. **Reference Parsing**
   - Extract bibliography entries
   - Match each to existing Paper nodes in graph (by DOI, title fuzzy match)
   - Create CITES_WITH_CONTEXT edges with in-text locations

4. **Metadata Enrichment**
   - Extract or infer keywords from content
   - Identify contribution statements (usually in Introduction)
   - Classify methodology type
   - Fill in abstract if still missing

5. **Quality Check**
   - Validate section structure (did extraction produce garbage?)
   - Check reference linkage rate (% of bib entries matched to graph nodes)
   - Flag papers with low extraction quality for review

6. **Persist**
   - Update Paper node properties with all extracted data
   - Create/update CITES_WITH_CONTEXT edges
   - Set paper status = `digested`
   - Log digestion summary

## Extraction Tools
- **GROBID** — best for PDF structure extraction (sections, references, headers)
- **marker** (PDF -> markdown) — fallback for text-heavy papers
- **pymupdf / PyMuPDF** — raw text extraction, figure extraction
- **BeautifulSoup / lxml** — HTML parsing for proceedings
- **Nougat** — OCR-based extraction for scanned papers

## Error Handling
- PDF unavailable: mark as `digestion_failed`, log source URL for retry
- Garbage extraction: fall back to raw text mode, flag for review
- Partial extraction: store what we got, set status to `partially_digested`
- Timeout: set generous timeouts on large PDFs, skip and retry later

## Idempotency
- Papers with status `digested` are skipped unless forced
- Partially digested papers resume from failure point
- Re-digestion replaces all extracted content (full overwrite, not append)

## Output
- Paper nodes enriched with full structured content
- CITES_WITH_CONTEXT edges with citation locations
- All digested papers marked `status: digested`
- Ready for downstream use: RAG, analysis, summarization, graph queries
