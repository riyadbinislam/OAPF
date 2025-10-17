"""
Open‑Access Paper Finder — Streamlit Web App (Dark‑mode friendly) + PubMed + Keyword Analytics

For NCBI, include an email to be a good API citizen: set `NCBI_EMAIL` in Streamlit secrets or edit HEADERS.
"""

from __future__ import annotations
import os
import re
import time
import json
from collections import Counter
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

import requests
import pandas as pd
import streamlit as st

# -----------------------------
# Constants & Config
# -----------------------------
APP_TITLE = "Open‑Access Paper Finder"
OPENALEX_BASE = "https://api.openalex.org/works"
ARXIV_BASE = "http://export.arxiv.org/api/query"
NCBI_EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"

CONTACT_EMAIL = st.secrets.get("NCBI_EMAIL", "youremail@example.com")

HEADERS = {
    "User-Agent": f"OpenAccessFinder/1.3 (mailto:{CONTACT_EMAIL})"
}

st.set_page_config(page_title=APP_TITLE, page_icon="📚", layout="wide")

# -----------------------------
# Styling (dark/light adaptive)
# -----------------------------
CUSTOM_CSS = """
<style>
:root { --card-bg: rgba(255,255,255,0.06); --card-border: rgba(255,255,255,0.12); --muted:#aaa; }
@media (prefers-color-scheme: light){ :root{ --card-bg:#ffffff; --card-border:#e8e8e8; --muted:#555; } }

.header-box { padding:1rem; border-radius:16px; background: var(--card-bg); border:1px solid var(--card-border); box-shadow: 0 6px 24px rgba(0,0,0,.12); }
.header-title { font-size:2rem; margin:0; font-weight:800; }
.header-sub { margin:.25rem 0 0 0; color: var(--muted); }

.result-card { padding:12px 14px; margin-bottom:10px; border:1px solid var(--card-border); border-radius:14px; background:var(--card-bg); }
.result-title a { text-decoration:none; font-weight:700; font-size:1.05rem; }
.meta { color: var(--muted); font-size:.92rem; margin-top:2px; }
.badge { background:#eef; color:#334; padding:2px 6px; border-radius:8px; font-size:.85rem; }
.linkline a { text-decoration:none; }

/* tighten top spacing */
.block-container { padding-top: 1.2rem; }
</style>
"""

st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

# Header (theme-aware)
st.markdown(
    f"""
    <div class="header-box">
      <h1 class="header-title">📚 {APP_TITLE}</h1>
      <p class="header-sub">Find <b>free / open‑access PDFs</b> by keyword using <b>OpenAlex</b>, <b>arXiv</b>, and <b>PubMed</b> (PMC). Export results to CSV/JSON — and analyze top keywords.</p>
    </div>
    """,
    unsafe_allow_html=True,
)

# -----------------------------
# Helpers for keyword analytics
# -----------------------------
STOPWORDS = set(
    '''a an and are as at be but by for from in into is it its of on or s the to with we our this that these those over under using used via new study review approach method results conclusion conclusions based between among toward towards among within across without has have had were was being been into about against among each other more most many much further less least than then there their they them he she you your i we us such while during before after according however therefore whereas whereas overall background objective objectives materials methods discussion discussions result results conclusion conclusions paper article preprint open access data code model models figure figures table tables supplementary supplementarymaterial materials available online link links'''.split()
)

TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z\-]{2,}")


def normalize_text(text: str) -> List[str]:
    words = [w.lower() for w in TOKEN_RE.findall(text or "")]
    words = [w.strip("-'") for w in words]
    return [w for w in words if w and w not in STOPWORDS]


def abstract_from_openalex(inv_idx: Optional[Dict[str, List[int]]]) -> str:
    """Reconstruct OpenAlex abstract text from inverted index map."""
    if not inv_idx:
        return ""
    # inv_idx maps token -> positions
    # Build by placing each token at its positions, then join by spaces
    maxpos = 0
    for positions in inv_idx.values():
        if positions:
            maxpos = max(maxpos, max(positions))
    out = [""] * (maxpos + 1)
    for token, positions in inv_idx.items():
        for p in positions:
            if 0 <= p < len(out):
                out[p] = token
    return " ".join([t for t in out if t])


# -----------------------------
# Search backends
# -----------------------------
@st.cache_data(show_spinner=False)
def search_openalex(
    query: str,
    year_from: Optional[int],
    year_to: Optional[int],
    max_results: int,
    sort: str = "relevance_score:desc",
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    per_page = 50
    retrieved = 0

    filters = ["is_oa:true"]
    if year_from:
        filters.append(f"from_publication_date:{year_from}-01-01")
    if year_to:
        filters.append(f"to_publication_date:{year_to}-12-31")

    cursor = "*"
    while retrieved < max_results:
        remaining = max_results - retrieved
        page_size = per_page if remaining > per_page else remaining
        params = {
            "search": query,
            "filter": ",".join(filters),
            "sort": sort,
            "per-page": page_size,
            "cursor": cursor,
        }
        resp = requests.get(OPENALEX_BASE, params=params, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        works = data.get("results", [])
        if not works:
            break

        for w in works:
            best_pdf = None
            try:
                best_loc = w.get("best_oa_location") or {}
                best_pdf = best_loc.get("pdf_url") or best_loc.get("url")
            except Exception:
                pass

            authors = ", ".join(
                a.get("author", {}).get("display_name", "") for a in (w.get("authorships") or [])
            )
            host = (w.get("host_venue") or {}).get("display_name")
            primary = (w.get("primary_location") or {})
            url_landing = (
                primary.get("landing_page_url")
                or primary.get("pdf_url")
                or (primary.get("source") or {}).get("host_venue_url")
                or (primary.get("source") or {}).get("url")
                or w.get("doi")
                or w.get("id")
            )

            results.append(
                {
                    "title": w.get("title"),
                    "authors": authors,
                    "year": w.get("publication_year"),
                    "venue": host,
                    "doi": (w.get("doi") or "").replace("https://doi.org/", ""),
                    "url_pdf": best_pdf,
                    "url_landing": url_landing,
                    "source": "OpenAlex",
                    "_abstract": abstract_from_openalex(w.get("abstract_inverted_index")),
                }
            )

        retrieved += len(works)
        cursor = data.get("meta", {}).get("next_cursor")
        if not cursor:
            break
        time.sleep(0.2)

    return results


@st.cache_data(show_spinner=False)
def search_arxiv(
    query: str,
    year_from: Optional[int],
    year_to: Optional[int],
    max_results: int,
    sort: str = "relevance:desc",
) -> List[Dict[str, Any]]:
    """Search arXiv for papers; lazily import feedparser to avoid hard dependency."""
    try:
        import feedparser  # type: ignore
    except Exception:
        st.error("arXiv support requires the 'feedparser' package. Add 'feedparser' to requirements.txt or pip install it locally.")
        return []

    date_filter = None
    if year_from or year_to:
        start = f"{year_from or 1900}01010000"
        end = f"{year_to or datetime.now().year}12312359"
        date_filter = f"submittedDate:[{start} TO {end}]"

    q_terms = [f"all:\"{query}\""]
    if date_filter:
        q_terms.append(date_filter)
    q = " AND ".join(q_terms)

    per_page = 50
    results: List[Dict[str, Any]] = []
    start_i = 0

    sortBy = sort.replace(":desc", "").replace(":asc", "")
    sortOrder = "descending" if ":desc" in sort else ("ascending" if ":asc" in sort else "descending")

    while len(results) < max_results:
        count = min(per_page, max_results - len(results))
        params = {
            "search_query": q,
            "start": start_i,
            "max_results": count,
            "sortBy": sortBy,
            "sortOrder": sortOrder,
        }
        resp = requests.get(ARXIV_BASE, params=params, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        feed = feedparser.parse(resp.text)
        entries = feed.get("entries", [])
        if not entries:
            break

        for e in entries:
            title = (e.get("title") or "").strip().replace("\n", " ")
            year = None
            try:
                year = datetime(*e.published_parsed[:6]).year if e.get("published_parsed") else None
            except Exception:
                pass
            pdf_url = None
            landing_url = e.get("link")
            for link in e.get("links", []):
                if link.get("type") == "application/pdf" or link.get("title") == "pdf":
                    pdf_url = link.get("href")
                    break
            authors = ", ".join(a.get("name") for a in e.get("authors", []))
            results.append(
                {
                    "title": title,
                    "authors": authors,
                    "year": year,
                    "venue": "arXiv",
                    "doi": e.get("arxiv_doi") or "",
                    "url_pdf": pdf_url,
                    "url_landing": landing_url,
                    "source": "arXiv",
                    "_abstract": (e.get("summary") or "").strip(),
                }
            )
        start_i += len(entries)
        if len(entries) < count:
            break
        time.sleep(0.2)

    return results


@st.cache_data(show_spinner=False)
def search_pubmed(
    query: str,
    year_from: Optional[int],
    year_to: Optional[int],
    max_results: int,
    sort: str = "relevance",
) -> List[Dict[str, Any]]:
    """PubMed free full text; includes PMCID PDF and fetches abstracts via EFetch (batched)."""
    results: List[Dict[str, Any]] = []

    term = query
    if year_from or year_to:
        yf = year_from or 1900
        yt = year_to or datetime.now().year
        term += f" AND (\"{yf}\"[Date - Publication] : \"{yt}\"[Date - Publication])"
    term += " AND free full text[Filter]"

    retstart = 0
    per_page = 100
    collected_pmids: List[str] = []

    while len(collected_pmids) < max_results:
        count = min(per_page, max_results - len(collected_pmids))
        params = {
            "db": "pubmed",
            "term": term,
            "retmode": "json",
            "retmax": count,
            "retstart": retstart,
            "sort": sort,
            "email": CONTACT_EMAIL,
            "tool": "OpenAccessFinder",
        }
        es = requests.get(f"{NCBI_EUTILS}/esearch.fcgi", params=params, headers=HEADERS, timeout=30)
        es.raise_for_status()
        esj = es.json()
        idlist = esj.get("esearchresult", {}).get("idlist", [])
        if not idlist:
            break
        collected_pmids.extend(idlist)
        retstart += len(idlist)
        time.sleep(0.34)

    if not collected_pmids:
        return results

    # ESummary for metadata (batch)
    for i in range(0, len(collected_pmids), 200):
        batch = collected_pmids[i:i+200]
        esum_params = {
            "db": "pubmed",
            "id": ",".join(batch),
            "retmode": "json",
            "email": CONTACT_EMAIL,
            "tool": "OpenAccessFinder",
        }
        sm = requests.get(f"{NCBI_EUTILS}/esummary.fcgi", params=esum_params, headers=HEADERS, timeout=30)
        sm.raise_for_status()
        smj = sm.json()
        res = smj.get("result", {})
        for pmid in batch:
            item = res.get(pmid)
            if not item:
                continue
            pmcid = None
            doi = ""
            for aid in item.get("articleids", []):
                if aid.get("idtype") == "pmcid":
                    pmcid = aid.get("value")
                if aid.get("idtype") == "doi":
                    doi = aid.get("value")
            pdf_url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmcid}/pdf" if pmcid else None
            if not pdf_url:
                continue
            authors = ", ".join([a.get('name') for a in item.get("authors", []) if a.get("name")])
            pubdate = item.get("pubdate", "")
            year = None
            for token in str(pubdate).split():
                if token.isdigit() and len(token) == 4:
                    year = int(token)
                    break
            results.append(
                {
                    "title": item.get("title"),
                    "authors": authors,
                    "year": year,
                    "venue": item.get("fulljournalname") or item.get("source") or "PubMed",
                    "doi": doi,
                    "url_pdf": pdf_url,
                    "url_landing": f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/",
                    "source": "PubMed",
                    "_pmid": pmid,
                }
            )
        time.sleep(0.34)

    # Fetch abstracts (optional, batched; keep light)
    pmids_with_abs = [r["_pmid"] for r in results if r.get("_pmid")]
    for i in range(0, len(pmids_with_abs), 100):
        batch = pmids_with_abs[i:i+100]
        ef_params = {
            "db": "pubmed",
            "id": ",".join(batch),
            "retmode": "xml",
            "rettype": "abstract",
            "email": CONTACT_EMAIL,
            "tool": "OpenAccessFinder",
        }
        ef = requests.get(f"{NCBI_EUTILS}/efetch.fcgi", params=ef_params, headers=HEADERS, timeout=30)
        ef.raise_for_status()
        xml = ef.text
        # very light parsing: pull \n between <AbstractText> tags
        abs_texts: Dict[str, str] = {}
        for pmid in batch:
            # naive extraction per PMID block
            block_match = re.search(rf"<PubmedArticle>[\s\S]*?<PMID[^>]*>{pmid}</PMID>[\s\S]*?</PubmedArticle>", xml)
            if not block_match:
                continue
            block = block_match.group(0)
            texts = re.findall(r"<AbstractText[^>]*>([\s\S]*?)</AbstractText>", block)
            clean = re.sub(r"<[^>]+>", " ", " \n ".join(texts))
            abs_texts[pmid] = clean
        for r in results:
            if r.get("_pmid") in abs_texts:
                r["_abstract"] = abs_texts[r["_pmid"]]
        time.sleep(0.34)

    return results


# -----------------------------
# Utilities
# -----------------------------

def dedupe_records(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen_doi, seen_pdf, seen_landing = set(), set(), set()
    out = []
    for r in rows:
        doi = (r.get("doi") or "").lower().strip()
        pdf = (r.get("url_pdf") or "").strip()
        land = (r.get("url_landing") or "").strip()
        if doi and doi in seen_doi:
            continue
        if pdf and pdf in seen_pdf:
            continue
        if land and land in seen_landing:
            continue
        if doi:
            seen_doi.add(doi)
        if pdf:
            seen_pdf.add(pdf)
        if land:
            seen_landing.add(land)
        out.append(r)
    return out


def to_dataframe(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["title", "authors", "year", "venue", "doi", "url_pdf", "url_landing", "source"])
    df = pd.DataFrame(rows)
    cols = ["title", "authors", "year", "venue", "doi", "url_pdf", "url_landing", "source"]
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    return df[cols]


def extract_keywords(rows: List[Dict[str, Any]], include_abstracts: bool = True) -> Tuple[Counter, List[str]]:
    """Return (frequency Counter, uncommon_terms list).
    Uncommon terms are words that occur once (hapax) and length ≥ 6.
    """
    tokens: List[str] = []
    for r in rows:
        title = r.get("title") or ""
        tokens.extend(normalize_text(title))
        if include_abstracts and r.get("_abstract"):
            tokens.extend(normalize_text(r.get("_abstract")))
    freq = Counter(tokens)
    uncommon = sorted([w for w, c in freq.items() if c == 1 and len(w) >= 6])
    return freq, uncommon


# -----------------------------
# UI Controls
# -----------------------------
with st.container():
    c1, c2, c3, c4 = st.columns([3, 1, 1, 1.2])
    query = c1.text_input("Keywords", placeholder="e.g. neurodegeneration, anomaly detection, LSTM", value="")

    current_year = datetime.now().year
    year_from = c2.number_input("Year from", min_value=1900, max_value=current_year, value=max(2018, current_year-7))
    year_to = c3.number_input("Year to", min_value=1900, max_value=current_year, value=current_year)
    max_results = c4.slider("Max results", min_value=10, max_value=500, value=100, step=10)

sources = st.multiselect("Sources", ["OpenAlex", "arXiv", "PubMed"], default=["OpenAlex", "arXiv", "PubMed"])

sort_map = {
    "Best match (OpenAlex)": ("openalex", "relevance_score:desc"),
    "Newest (OpenAlex)": ("openalex", "publication_year:desc"),
    "Most cited (OpenAlex)": ("openalex", "cited_by_count:desc"),
    "Best match (arXiv)": ("arxiv", "relevance:desc"),
    "Newest updated (arXiv)": ("arxiv", "lastUpdatedDate:desc"),
    "Newest submitted (arXiv)": ("arxiv", "submittedDate:desc"),
    "Best match (PubMed)": ("pubmed", "relevance"),
    "Most recent (PubMed)": ("pubmed", "pub+date"),
}

sort_choice = st.selectbox("Sort by", list(sort_map.keys()), index=0)
run = st.button("🔎 Search", use_container_width=True, type="primary")

st.divider()

# -----------------------------
# Execute search & render
# -----------------------------
rows: List[Dict[str, Any]] = []
if run:
    if not query.strip():
        st.warning("Please enter some keywords to search.")
        st.stop()

    with st.spinner("Searching open‑access sources…"):
        all_rows: List[Dict[str, Any]] = []
        which, sort_value = sort_map[sort_choice]
        per_source = max(10, max_results // max(1, len(sources)))

        if "OpenAlex" in sources:
            oa_rows = search_openalex(
                query, int(year_from), int(year_to),
                per_source if which != "openalex" else max_results,
                sort=sort_value if which == "openalex" else "relevance_score:desc",
            )
            all_rows.extend(oa_rows)

        if "arXiv" in sources:
            ax_rows = search_arxiv(
                query, int(year_from), int(year_to),
                per_source if which != "arxiv" else max_results,
                sort=sort_value if which == "arxiv" else "relevance:desc",
            )
            all_rows.extend(ax_rows)

        if "PubMed" in sources:
            pm_rows = search_pubmed(
                query, int(year_from), int(year_to),
                per_source if which != "pubmed" else max_results,
                sort=sort_value if which == "pubmed" else "relevance",
            )
            all_rows.extend(pm_rows)

        rows = dedupe_records(all_rows)

    st.success(f"Found {len(rows)} unique open/free items.")

    if not rows:
        st.info("No results. Try broadening keywords or extending the year range.")
        st.stop()

    # Results list
    for r in rows:
        st.markdown(
            f"""
            <div class='result-card'>
              <div class='result-title'>
                <a href="{r.get('url_landing') or r.get('url_pdf') or ''}" target="_blank">{r.get('title') or '(untitled)'}</a>
              </div>
              <div style='color:#bbb'>{r.get('authors') or ''}</div>
              <div class='meta'>
                {r.get('venue') or ''} • {r.get('year') or ''} • <span class='badge'>{r.get('source')}</span>
              </div>
              <div class='linkline' style='margin-top:6px;'>
                {f"<a href='{r.get('url_pdf')}' target='_blank'>📄 PDF</a>" if r.get('url_pdf') else ""}
                {" • " if r.get('url_pdf') and r.get('doi') else ""}
                {f"<a href='https://doi.org/{r.get('doi')}' target='_blank'>DOI</a>" if r.get('doi') else ""}
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # Export
    df = to_dataframe(rows)
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    json_bytes = json.dumps(rows, indent=2).encode("utf-8")

    exp_cols = st.columns(2)
    exp_cols[0].download_button(
        "⬇️ Download CSV", csv_bytes, file_name="open_access_results.csv", mime="text/csv", use_container_width=True
    )
    exp_cols[1].download_button(
        "⬇️ Download JSON", json_bytes, file_name="open_access_results.json", mime="application/json", use_container_width=True
    )

    st.divider()

    # -------------------------
    # Keyword analytics section
    # -------------------------
    st.subheader("🧠 Keyword analytics")
    use_abs = st.checkbox("Include abstracts (slower, but better)", value=True)
    top_n = st.slider("Top N words for bar chart", 5, 40, 20)

    with st.spinner("Computing keyword frequencies…"):
        freq, uncommon = extract_keywords(rows, include_abstracts=use_abs)
        if not freq:
            st.info("No text available to analyze (try enabling abstracts or widening sources).")
        else:
            top_items = freq.most_common(top_n)
            top_df = pd.DataFrame(top_items, columns=["word", "count"]).set_index("word")
            st.bar_chart(top_df)

            st.markdown("**Uncommon keywords (appear once, length ≥ 6)**")
            if uncommon:
                st.write(", ".join(uncommon[:200]))  # cap display
            else:
                st.write("— none —")

# Footer
st.markdown(
    """
    <div style="margin-top:1rem; font-size:0.9rem; color:#888;">
    ⚖️ This app avoids scraping Google Scholar directly. It queries OpenAlex & arXiv APIs and NCBI E‑utilities for PubMed, preferring PubMed Central PDFs. Keyword analytics use titles and (where available) abstracts.
    </div>
    """,
    unsafe_allow_html=True,
)
