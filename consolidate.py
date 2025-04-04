import os
import sys
import glob
import re
import json
import asyncio
import aiohttp
import time
import urllib.parse
import argparse
from lxml import etree
from bs4 import BeautifulSoup
from difflib import SequenceMatcher
import bibtexparser
from bibtexparser.bwriter import BibTexWriter
from bibtexparser.bparser import BibTexParser
from bibtexparser.customization import homogenize_latex_encoding, latex_to_unicode
import io
import contextlib

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    class tqdm:
        def __init__(self, *args, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def update(self, n=1): pass
        def write(self, s, file=sys.stdout, end="\n"): file.write(s + end)
        def refresh(self): pass
        def close(self): pass
        def set_description(self, desc=None, refresh=True): pass

# --- Configuration ---
OUTPUT_SUFFIX = "_consolidated"
PREVIOUS_OUTPUT_SUFFIX = OUTPUT_SUFFIX
EMAIL_ADDRESS = "your.email@example.com"
SIMILARITY_SCORE = 0.80
# --- API & Rate Limiting Settings ---
CROSSREF_API_URL = "https://api.crossref.org/works"
NCBI_ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
NCBI_EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"

BASE_DOMAIN_DELAYS = {
    "api.crossref.org": 0.1, "eutils.ncbi.nlm.nih.gov": 0.35,
    "sciencedirect.com": 2.0, "onlinelibrary.wiley.com": 2.0,
    "link.springer.com": 2.0, "springer.com": 2.0, "nature.com": 2.0,
    "pubs.acs.org": 2.5, "cell.com": 2.0, "journals.plos.org": 1.5,
    "pnas.org": 2.0, "science.org": 2.5, "academic.oup.com": 2.0,
    "doi.org": 0.5, "DEFAULT": 3.0
}
MAX_CONCURRENT_REQUESTS = 5
REQUEST_TIMEOUT = 25
MAX_RETRIES = 2
REQ_USER_AGENT = f"PythonRefConsolidator/1.1 (mailto:{EMAIL_ADDRESS})"

current_domain_delays = BASE_DOMAIN_DELAYS.copy()
domain_locks = {}

class TqdmStderrWrapper:
    def __init__(self, tqdm_instance):
        self._tqdm = tqdm_instance
        self._file = sys.stderr
    def write(self, buf):
        self._tqdm.write(buf.rstrip(), file=self._file, end='\n')
    def flush(self):
        return self._file.flush()

# --- Helper Functions ---
def get_domain(url):
    try: return urllib.parse.urlsplit(url).netloc.lower()
    except Exception: return None

def clean_text(text):
    if not text: return ""
    text = str(text)
    text = re.sub(r'\s+', ' ', text).strip()
    text = text.replace("{", "").replace("}", "").replace("\\%", "%")
    return text

def bibtex_unicode_decode(record):
    record = homogenize_latex_encoding(record)
    record = latex_to_unicode(record)
    return record

def simple_similarity(a, b):
    if not a or not b: return 0.0
    return SequenceMatcher(None, str(a).lower(), str(b).lower()).ratio()

def is_valid_doi(doi):
    if not doi or not isinstance(doi, str): return False
    return bool(re.match(r'^10\.\d{4,9}/[-._;()/:A-Z0-9]+$', doi, re.IGNORECASE))

def is_valid_pmid(pmid):
    if not pmid: return False
    return str(pmid).isdigit()

# --- Input Parsing ---
def parse_endnote_text(filepath):
    print(f"Parsing EndNote text file: {filepath}")
    entries = []
    current_entry = None
    current_abstract_lines = []
    entry_count = 0
    try:
        with open(filepath, 'r', encoding='utf-8') as infile:
            for line_num, line in enumerate(infile):
                stripped_line = line.strip()
                if not stripped_line: continue
                is_abstract_line = line.startswith('\t')
                if not is_abstract_line:
                    if current_entry:
                        current_entry['fields']['abstract'] = ' '.join(current_abstract_lines).strip()
                        entries.append(current_entry)
                    entry_count += 1
                    parsed_line = parse_reference_line_for_text(line)
                    current_entry = {
                        'entry_id': f"text_entry_{entry_count}", 'entry_type': 'text_record',
                        'fields': parsed_line, 'original_format': 'text', 'line_num': line_num + 1
                    }
                    current_abstract_lines = []
                elif current_entry:
                    current_abstract_lines.append(line.lstrip('\t').strip())
                elif is_abstract_line and not current_entry:
                     if TQDM_AVAILABLE: tqdm.write(f"Warning: Found abstract line at {line_num+1} before a reference line: {line.strip()[:80]}...", file=sys.stderr)
                     else: print(f"Warning: Found abstract line at {line_num+1} before a reference line: {line.strip()[:80]}...", file=sys.stderr)

            if current_entry:
                current_entry['fields']['abstract'] = ' '.join(current_abstract_lines).strip()
                entries.append(current_entry)
    except Exception as e:
        print(f"Error parsing EndNote text file: {e}", file=sys.stderr); return None
    return entries

def parse_reference_line_for_text(line):
    fields = {'original_line': line.strip()}
    year_match = re.search(r'\(([12]\d{3})\)', line)
    if year_match:
        fields["year"] = year_match.group(1)
        year_start, year_end = year_match.span()
        fields["author_str"] = line[:year_start].strip().rstrip('.').strip()
        remaining = line[year_end:]
        title_match = re.search(r'"([^"]+)"', remaining)
        if title_match:
            fields["title"] = title_match.group(1).strip()
            fields["source_str"] = remaining[title_match.end():].strip().lstrip('.').strip()
        else:
            fields["source_str"] = remaining.strip().lstrip('.').strip()
    else:
         title_match = re.search(r'"([^"]+)"', line)
         if title_match:
             fields["title"] = title_match.group(1).strip()
             title_start, title_end = title_match.span()
             fields["author_str"] = line[:title_start].strip().rstrip('.').strip()
             fields["source_str"] = line[title_end:].strip().lstrip('.').strip()

    doi_match = re.search(r'(?:doi|DOI)\s*[:\s]\s*(10\.\d{4,9}/[-._;()/:A-Z0-9]+)', line, re.IGNORECASE)
    if doi_match:
        potential_doi = doi_match.group(1)
        if is_valid_doi(potential_doi): fields['doi'] = potential_doi
    pmid_match = re.search(r'(?:pmid|PMID)\s*[:\s]\s*(\d+)', line, re.IGNORECASE)
    if pmid_match:
        potential_pmid = pmid_match.group(1)
        if is_valid_pmid(potential_pmid): fields['pmid'] = potential_pmid
    if not fields.get("title") and not year_match:
         parts = line.split('.')
         if len(parts) > 1:
             fields["author_str"] = parts[0].strip()
             remaining_title = " ".join(parts[1:]).strip()
             if not re.search(r'\b(vol|iss|pp|pg|ed|comp)\b', remaining_title, re.I): fields["title"] = remaining_title
             else: fields["source_str"] = remaining_title
         else: fields["title"] = line.strip()
    return fields

def parse_bibtex(filepath):
    print(f"Parsing BibTeX file: {filepath}")
    try:
        with open(filepath, 'r', encoding='utf-8') as bibfile:
            parser = BibTexParser(common_strings=True)
            parser.customization = bibtex_unicode_decode
            bib_database = bibtexparser.load(bibfile, parser=parser)
        entries = []
        entry_count = 0
        for entry in bib_database.entries:
            entry_count += 1
            cleaned_fields = {k.lower().strip(): clean_text(v) for k, v in entry.items() if k.lower() not in ['entrytype', 'id']}
            internal_entry = {
                'entry_id': entry.get('ID', f"bibtex_entry_{entry_count}"),
                'entry_type': entry.get('ENTRYTYPE', 'misc').lower(),
                'fields': cleaned_fields, 'original_format': 'bibtex'
            }
            entries.append(internal_entry)
        return entries
    except Exception as e:
        print(f"Error parsing BibTeX file: {e}", file=sys.stderr); return None

# --- Fetching Logic ---
async def async_fetch_resource(session, semaphore, url, params=None, response_type='text', retries=MAX_RETRIES):
    """Handles actual HTTP request with dynamic delay and error handling."""
    domain = get_domain(url)
    if not domain:
         tqdm.write(f"Warning: Invalid domain for URL {url}. Skipping fetch.", file=sys.stderr)
         return None
    if domain not in domain_locks: domain_locks[domain] = asyncio.Lock()
    lock = domain_locks[domain]
    async with lock:
        delay = current_domain_delays.get(domain, BASE_DOMAIN_DELAYS["DEFAULT"])
        await asyncio.sleep(delay)
    headers = {'User-Agent': REQ_USER_AGENT}
    if response_type == 'json': headers['Accept'] = 'application/json'
    for attempt in range(retries):
        try:
            async with session.get(url, params=params, timeout=REQUEST_TIMEOUT, headers=headers, allow_redirects=True) as response:
                if response.status == 429:
                    tqdm.write(f"Warning: Received 429 (Too Many Requests) from {domain}. Increasing delay.", file=sys.stderr)
                    async with lock: current_domain_delays[domain] = current_domain_delays.get(domain, BASE_DOMAIN_DELAYS["DEFAULT"]) * 1.5 + 0.5
                    await asyncio.sleep(2 + delay); continue
                elif response.status in [403, 503]:
                     tqdm.write(f"Warning: Received {response.status} from {domain}. Likely blocked. Skipping request for {url}.", file=sys.stderr); return None
                response.raise_for_status()
                if response_type == 'json': return await response.json()
                elif response_type == 'xml':
                    content_bytes = await response.read()
                    return etree.fromstring(content_bytes) if content_bytes else None
                else: return await response.text()
        except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError, etree.XMLSyntaxError) as e:
            tqdm.write(f"Warning: Fetch/Parse error for {url} (Attempt {attempt + 1}/{retries}): {type(e).__name__}", file=sys.stderr)
            if attempt < retries - 1: await asyncio.sleep(1.5 ** attempt)
            else: tqdm.write(f"Error: Failed permanently for {url} after {retries} attempts.", file=sys.stderr)
        except Exception as e:
            tqdm.write(f"Warning: Unexpected error during fetch for {url} (Attempt {attempt + 1}/{retries}): {e}", file=sys.stderr); break
    return None

# CORRECTED get_doi_from_crossref with fallbacks and fixed break logic
async def get_doi_from_crossref(session, semaphore, entry):
    """Query Crossref API for DOI using multiple fallback strategies and check title similarity."""
    title = entry['fields'].get('title')
    author_str = entry['fields'].get('author') or entry['fields'].get('author_str')
    year = entry['fields'].get('year')
    status = {"doi_fetched": False, "doi_error": "No suitable search performed",
              "doi_title_score": 0.0, "fetched_title": None, "doi_method": None}

    if not title:
        status["doi_error"] = "Missing title"
        return None, status

    search_attempts = []
    base_params = {'mailto': EMAIL_ADDRESS, 'rows': 1}

    # Attempt 1: Title + Author + Year Filter
    params1 = base_params.copy()
    query_parts1 = [f'"{title}"']
    if author_str:
        first_author = author_str.split(',')[0].split(' and ')[0].strip()
        last_name = first_author.split()[-1] if first_author else None
        if last_name: query_parts1.append(f'author:"{last_name}"')
    params1['query'] = " ".join(query_parts1)
    if year: params1['filter'] = f'from-pub-date:{year},until-pub-date:{year}'
    search_attempts.append({"params": params1, "method": "Title/Author/Year"})

    # Attempt 2: Title + Year Filter
    if year:
        params2 = base_params.copy()
        params2['query'] = f'"{title}"'
        params2['filter'] = f'from-pub-date:{year},until-pub-date:{year}'
        search_attempts.append({"params": params2, "method": "Title/Year"})

    # Attempt 3: Title Only
    params3 = base_params.copy()
    params3['query'] = f'"{title}"'
    search_attempts.append({"params": params3, "method": "Title Only"})

    doi = None
    last_error = "No attempts succeeded" # Default error

    for attempt_info in search_attempts:
        tqdm.write(f"  Attempting DOI via {attempt_info['method']} for '{title[:40]}...'", file=sys.stderr)
        data = await async_fetch_resource(session, semaphore, CROSSREF_API_URL, params=attempt_info["params"], response_type='json')

        if data and data.get('status') == 'ok' and data['message']['items']:
            item = data['message']['items'][0]
            potential_doi = item.get('DOI')
            fetched_title_list = item.get('title')
            fetched_title = fetched_title_list[0] if fetched_title_list else None
            score = simple_similarity(title, fetched_title)

            if potential_doi and is_valid_doi(potential_doi) and score >= SIMILARITY_SCORE:
                doi = potential_doi
                status["doi_fetched"] = True; status["doi_title_score"] = round(score, 3)
                status["fetched_title"] = fetched_title; status["doi_method"] = attempt_info["method"]; status["doi_error"] = None
                tqdm.write(f"    >> DOI Found via {attempt_info['method']}: {doi}", file=sys.stderr)
                break # Stop on first good match
            elif potential_doi:
                 last_error = f"Title mismatch (Score: {score:.2f} via {attempt_info['method']})"
                 status["fetched_title"] = fetched_title
                 tqdm.write(f"    -> DOI found ({potential_doi}) but title mismatch (Score: {score:.2f}). Trying next method.", file=sys.stderr)
                 # Continue implicitly
            else:
                 last_error = f"DOI not present in result via {attempt_info['method']}"
                 tqdm.write(f"    -> Result found but no DOI via {attempt_info['method']}. Trying next method.", file=sys.stderr)
                 # Continue implicitly
        elif data and data.get('status') != 'ok':
             last_error = f"API Error ({attempt_info['method']}): {data.get('message', {}).get('type', 'Unknown')}"
             tqdm.write(f"    -> API Error via {attempt_info['method']}. Trying next method.", file=sys.stderr)
             # Continue implicitly
        elif not data:
             last_error = f"Fetch failed ({attempt_info['method']})"
             tqdm.write(f"    -> Fetch failed via {attempt_info['method']}. Trying next method (if any).", file=sys.stderr)
             # *** REMOVED break: Allow trying next method even if fetch failed ***
        else: # No items found
             last_error = f"No results via {attempt_info['method']}"
             tqdm.write(f"    -> No results via {attempt_info['method']}. Trying next method.", file=sys.stderr)
             # Continue implicitly

    if not status["doi_fetched"]:
        status["doi_error"] = last_error

    return doi, status


async def get_pmid_from_pubmed(session, semaphore, entry):
    """Fetch PMID using NCBI ESearch (DOI preferred)."""
    doi = entry['fields'].get('doi')
    title = entry['fields'].get('title')
    author_str = entry['fields'].get('author') or entry['fields'].get('author_str')
    year = entry['fields'].get('year')
    status = {"pmid_fetched": False, "pmid_error": None, "pmid_method": None}
    if not doi and not title: status["pmid_error"] = "No DOI or Title"; return None, status
    params = {'db': 'pubmed', 'retmax': 1, 'retmode': 'xml', 'email': EMAIL_ADDRESS}
    search_term = None
    if doi and is_valid_doi(doi):
        search_term = f"{doi}[DOI]"; status["pmid_method"] = "DOI"
    elif title:
        query_parts = [f'"{title}"[Title]']
        if author_str:
            first_author = author_str.split(',')[0].split(' and ')[0].strip()
            last_name = first_author.split()[-1] if first_author else None
            if last_name: query_parts.append(f'"{last_name}"[First Author]')
        if year: query_parts.append(f'{year}[Publication Date]')
        search_term = " AND ".join(query_parts); status["pmid_method"] = "Title/Author/Year"
    if not search_term: status["pmid_error"] = "Could not construct search term"; return None, status
    params['term'] = search_term
    root = await async_fetch_resource(session, semaphore, NCBI_ESEARCH_URL, params=params, response_type='xml')
    if root is not None:
        id_list = root.xpath('//IdList/Id/text()')
        if id_list:
            pmid = id_list[0]
            if is_valid_pmid(pmid): status["pmid_fetched"] = True; return pmid, status
            else: status["pmid_error"] = f"Invalid PMID format: {pmid}"
        else: status["pmid_error"] = "No PMID found in result"
    else: status["pmid_error"] = "Fetch failed or blocked"
    return None, status

async def fetch_abstract_for_entry(session, semaphore, entry, enable_scraping=False):
    """Fetch Abstract using NCBI EFetch (preferred) or scraping (fallback)."""
    pmid = entry['fields'].get('pmid')
    doi = entry['fields'].get('doi')
    status = {"abstract_fetched": False, "abstract_error": None, "abstract_method": None}
    if pmid and is_valid_pmid(pmid):
        status["abstract_method"] = "EFetch (PMID)"
        params = {"db": "pubmed", "id": pmid, "rettype": "abstract", "retmode": "xml", "email": EMAIL_ADDRESS}
        root = await async_fetch_resource(session, semaphore, NCBI_EFETCH_URL, params=params, response_type='xml')
        if root is not None:
            abstract_parts = root.xpath('//Abstract/AbstractText//text()')
            if not abstract_parts: abstract_parts = root.xpath('//ArticleTitle//text()')
            if abstract_parts:
                full_abstract = ' '.join(part.strip() for part in abstract_parts).strip()
                cleaned_abstract = clean_text(full_abstract)
                if cleaned_abstract and len(cleaned_abstract) > 20 and "no abstract available" not in cleaned_abstract.lower():
                    status["abstract_fetched"] = True; return cleaned_abstract, status
                else: status["abstract_error"] = "Abstract empty/placeholder in EFetch result"
            else: status["abstract_error"] = "Abstract not found in EFetch result"
        else: status["abstract_error"] = "EFetch failed or blocked"

    if not status["abstract_fetched"] and enable_scraping and doi and is_valid_doi(doi):
        status["abstract_method"] = "Scraping (DOI)"
        tqdm.write(f"  -> Attempting abstract scrape via DOI {doi} for {entry.get('entry_id', 'Unknown')}", file=sys.stderr)
        doi_url = f"https://doi.org/{urllib.parse.quote_plus(doi)}"
        html_content, final_url = await async_fetch_resource(session, semaphore, doi_url, response_type='text')
        if html_content:
            final_domain = get_domain(final_url) or "unknown"
            abstract = extract_abstract_from_html(html_content, final_domain)
            if abstract and not abstract.startswith("["):
                status["abstract_fetched"] = True; return abstract, status
            else: status["abstract_error"] = f"Scrape failed: {abstract}"
        else: status["abstract_error"] = "Scrape fetch failed or blocked"
    elif not pmid and not doi: status["abstract_error"] = "No PMID or DOI available"
    elif not pmid and not enable_scraping: status["abstract_error"] = "No PMID and scraping disabled"

    return None, status


def extract_abstract_from_html(html_content, domain):
    """Tries various selectors based on domain to extract abstract text."""
    if not html_content: return "[FETCH FAILED OR BLOCKED]"
    try:
        soup = BeautifulSoup(html_content, 'lxml')
        abstract_text = None
        domain_selectors = [ # List of (substring, [selectors])
             ("sciencedirect.com", ['div.Abstracts section p', 'section[data-title="Abstract"] p', 'div.abstract p', 'div#abstracts p', 'section#abstract p', 'div.Abstract p']),
             ("cell.com", ['div.Abstracts section p', 'section[data-title="Abstract"] p', 'div.abstract.author p', 'div.abstract p', 'div#abstracts p', 'section#abstract p']),
             ("wiley.com", ['section.article-section__abstract p', 'div.abstract-group p', 'div#abstract p', 'div.abstract p', '.abstract-text p']),
             ("springer.com", ['section[data-title="Abstract"] p', 'div.c-article-section__content#Abs1-content p', 'div.Abstract p', 'section.Abstract p']),
             ("nature.com", ['section[aria-labelledby="abstract-heading"] p', 'div#Abs1-content p', 'div.c-article-abstract p', 'article[itemprop="description"] p']),
             ("pubs.acs.org", ['div.abstractSection p', 'p.abstract-text', 'div.NLM_abstract']),
             ("pubs.rsc.org", ['div.article__abstract p', 'p.abstract']),
             ("journals.plos.org", ['div.abstract-content p', 'section[id^="abstract"] p', 'article.article-text p']),
             ("pnas.org", ['div#abstract-1 p', 'div.section.abstract p']),
             ("science.org", ['div#abstracts p', 'div.abstract.module p', 'section.abstract p']),
             ("tandfonline.com", ['div.abstractSection div p', 'div.hlFld-Abstract p']),
             ("oup.com", ['section.abstract p', 'div.abstract p']),
             ("emboj.", ['div#abstract p', '.abstract p']),
             ("biomedcentral.com", ['section[itemprop="description"] p', 'div.Abstract p']),
             ("frontiersin.org", ['div.Abstract *', 'div.abstractbody']),
             ("mdpi.com", ['div.art-abstract span', 'div.abstract-content']),
             ("elifesciences.org", ['section[data-testid="abstract"] p', 'div.abstract-content p']),
             ("biorxiv.org", ['div.section.abstract p', 'div.abstract.abstract-text p']),
             ("medrxiv.org", ['div.section.abstract p', 'div.abstract.abstract-text p']),
        ]
        matched_selectors = []
        for pattern, selectors in domain_selectors:
             if pattern in domain: matched_selectors = selectors; break
        if matched_selectors:
            for sel in matched_selectors:
                tags = soup.select(sel)
                if tags:
                    abstract_text = ' '.join(clean_text(tag.get_text(separator=' ', strip=True)) for tag in tags)
                    if abstract_text: break
        if not abstract_text:
            generic_selectors = [
                'div#abstract p', 'div.abstract p', 'section#abstract p', 'section.abstract p',
                'div#abstract', 'div.abstract', 'section#abstract', 'section.abstract'
            ]
            for sel in generic_selectors:
                tags = soup.select(sel)
                if tags:
                    abstract_text = ' '.join(clean_text(tag.get_text(separator=' ', strip=True)) for tag in tags)
                    if abstract_text: break
        if not abstract_text:
             meta_desc = soup.find('meta', attrs={'name': 'description'})
             if meta_desc and meta_desc.get('content'):
                  content = meta_desc['content']
                  if len(content) > 75 and ('method' in content.lower() or 'result' in content.lower() or 'conclusion' in content.lower()):
                       abstract_text = f"[META DESCRIPTION] {clean_text(content)}"
        if abstract_text:
            cleaned_abstract = clean_text(abstract_text)
            if cleaned_abstract and len(cleaned_abstract) > 50 and \
               not re.match(r'^(abstract|summary)\s*$', cleaned_abstract, re.IGNORECASE) and \
               "no abstract available" not in cleaned_abstract.lower() and \
               "abstract is not available" not in cleaned_abstract.lower() and \
               "abstract unavailable" not in cleaned_abstract.lower():
                 return cleaned_abstract
            else:
                 return "[ABSTRACT FOUND BUT EMPTY/PLACEHOLDER]"
        else:
            return "[ABSTRACT ELEMENT NOT FOUND]"
    except Exception as e:
        tqdm.write(f"Error parsing HTML from {domain}: {e}", file=sys.stderr)
        return "[HTML PARSE ERROR]"

async def process_single_entry(session, semaphore, entry, force_refetch, enable_scraping):
    """Processes one entry: checks missing fields and fetches them."""
    updated_entry = entry.copy()
    updated_entry['fields'] = entry['fields'].copy()
    status_report = {"id": entry['entry_id'], "doi": "Skipped", "pmid": "Skipped", "abstract": "Skipped"}

    # Fetch DOI
    current_doi = updated_entry['fields'].get('doi')
    needs_doi = force_refetch or not current_doi or not is_valid_doi(current_doi)
    if needs_doi:
        doi, doi_status = await get_doi_from_crossref(session, semaphore, updated_entry)
        if doi_status["doi_fetched"]:
            updated_entry['fields']['doi'] = doi; status_report["doi"] = "Fetched"
            if doi_status.get("fetched_title"):
                updated_entry['fields']['fetched_title'] = doi_status["fetched_title"]
                updated_entry['fields']['title_match_score'] = doi_status["doi_title_score"]
        else:
            status_report["doi"] = f"Failed ({doi_status.get('doi_error', 'Unknown')})"
            if not doi and current_doi and is_valid_doi(current_doi): updated_entry['fields']['doi'] = current_doi # Keep old valid DOI

    # Fetch PMID
    current_pmid = updated_entry['fields'].get('pmid')
    needs_pmid = force_refetch or not current_pmid or not is_valid_pmid(current_pmid)
    # Use the DOI field *after* the potential update from the DOI fetch step
    doi_present_and_valid = updated_entry['fields'].get('doi') and is_valid_doi(updated_entry['fields'].get('doi'))
    if needs_pmid or (needs_doi and doi_present_and_valid): # Try PMID if DOI was found/refreshed or if PMID needed anyway
        pmid, pmid_status = await get_pmid_from_pubmed(session, semaphore, updated_entry)
        if pmid_status["pmid_fetched"]:
            updated_entry['fields']['pmid'] = pmid; status_report["pmid"] = f"Fetched ({pmid_status.get('pmid_method', '?')})"
        else:
            status_report["pmid"] = f"Failed ({pmid_status.get('pmid_error', 'Unknown')})"
            if not pmid and current_pmid and is_valid_pmid(current_pmid): updated_entry['fields']['pmid'] = current_pmid # Keep old valid PMID

    # Fetch Abstract
    current_abstract = updated_entry['fields'].get('abstract', '')
    needs_abstract = force_refetch or not current_abstract or (isinstance(current_abstract, str) and current_abstract.startswith("["))
    # Use the PMID field *after* the potential update from the PMID fetch step
    pmid_present_and_valid = updated_entry['fields'].get('pmid') and is_valid_pmid(updated_entry['fields'].get('pmid'))
    if needs_abstract or (needs_pmid and pmid_present_and_valid): # Try Abstract if PMID was found/refreshed or if Abstract needed anyway
        abstract, abstract_status = await fetch_abstract_for_entry(session, semaphore, updated_entry, enable_scraping)
        if abstract_status["abstract_fetched"]:
            updated_entry['fields']['abstract'] = abstract; status_report["abstract"] = f"Fetched ({abstract_status.get('abstract_method', '?')})"
        else:
            status_report["abstract"] = f"Failed ({abstract_status.get('abstract_error', 'Unknown')})"
            # Only overwrite existing abstract if force_refetch is used or it was a placeholder
            if force_refetch or not current_abstract or (isinstance(current_abstract, str) and current_abstract.startswith("[")):
                 updated_entry['fields']['abstract'] = f"[{abstract_status.get('abstract_error', 'FETCH FAILED')}]"

    return updated_entry, status_report

# --- Output Generation ---
# CORRECTED reconstruct_ref_line to avoid duplication
def reconstruct_ref_line(fields):
    """Reconstructs a reference line string from fields, avoiding DOI/PMID duplication."""
    parts = []
    if fields.get('author_str'): parts.append(fields['author_str'])
    if fields.get('year'): parts.append(f"({fields['year']}).")
    title = fields.get('title')
    if title: parts.append(f"\"{title}.\"")
    if fields.get('source_str'): parts.append(fields['source_str'])

    line = " ".join(p for p in parts if p).replace("..", ".").strip()

    doi = fields.get('doi')
    pmid = fields.get('pmid')
    extra_info_to_append = []
    doi_already_present = False
    pmid_already_present = False

    line_lower = line.lower() # Check reconstructed line including source_str
    if doi and is_valid_doi(doi):
        # Check if "doi: <value>" or "doi <value>" is already present
        if re.search(r'doi\s*[:\s]\s*' + re.escape(doi), line_lower):
            doi_already_present = True
        else:
            extra_info_to_append.append(f"doi: {doi}")

    if pmid and is_valid_pmid(pmid):
         # Check if "PMID:<value>" or "PMID <value>" is already present
         if re.search(r'pmid\s*[:\s]\s*' + re.escape(pmid), line_lower):
             pmid_already_present = True
         else:
             extra_info_to_append.append(f"PMID:{pmid}") # Consistent format without trailing dot

    if extra_info_to_append:
        if line and not line.endswith('.'): line += '.' # Ensure period before adding extras
        line += " " + " ".join(extra_info_to_append)

    # Fallback logic
    if not line.strip() and fields.get('original_line'):
        line = fields.get('original_line')
    elif not line.strip():
         line = '[RECONSTRUCTION FAILED - NO DATA]'

    return line.strip()

# CORRECTED generate_endnote_text uses the above helper
def generate_endnote_text(entries):
    """Generates EndNote-like text output using reconstructed lines."""
    output_lines = []
    for entry in entries:
        ref_line = reconstruct_ref_line(entry['fields']) # Use the corrected helper
        output_lines.append(ref_line)
        abstract = entry['fields'].get('abstract', '')
        if abstract and not abstract.startswith("["):
            output_lines.append(f"\t{abstract}")
        output_lines.append("")
    return "\n".join(output_lines)

def generate_bibtex(entries):
    """Generates BibTeX output using bibtexparser."""
    bib_database = bibtexparser.bibdatabase.BibDatabase()
    bib_database.entries = []
    processed_ids = set()
    entry_counter = 1
    for entry in entries:
        bib_entry = {}
        entry_id = entry.get('entry_id', f"entry_{entry_counter}")
        original_entry_id = entry_id
        while entry_id in processed_ids:
             entry_id = f"{original_entry_id}_{entry_counter}"; entry_counter += 1
        processed_ids.add(entry_id)
        bib_entry['ENTRYTYPE'] = entry.get('entry_type', 'misc')
        bib_entry['ID'] = entry_id
        for key, value in entry.get('fields', {}).items():
             if key not in ['original_line', 'author_str', 'source_str', 'fetched_title', 'title_match_score', 'fetch_error'] and value is not None:
                 str_value = str(value)
                 if not str_value.startswith("["): # Don't write placeholders like "[FETCH FAILED]"
                     if key in ['author', 'title', 'journal', 'booktitle', 'abstract']:
                         bib_entry[key] = f"{{{str_value}}}" # Basic wrapping
                     else:
                         bib_entry[key] = str_value
                 # Otherwise, omit the field (e.g., for None or placeholder abstracts)
        bib_database.entries.append(bib_entry)
        entry_counter += 1
    writer = BibTexWriter(); writer.indent = '  '; writer.comma_first = False
    # Define a reasonable order for common fields
    writer.display_order = ('title', 'author', 'journal', 'booktitle', 'year', 'volume', 'number', 'pages', 'doi', 'pmid', 'abstract')
    writer.add_trailing_comma = True
    return bibtexparser.dumps(bib_database, writer=writer)


# --- Reporting ---
def generate_report(initial_entries, final_entries, fetch_statuses):
    """Generates a summary report."""
    num_total = len(initial_entries)
    if num_total == 0: return "--- Consolidation Report ---\nNo entries processed."

    report = ["--- Consolidation Report ---", f"Total Entries Processed: {num_total}"]
    initial_missing = {"doi": 0, "pmid": 0, "abstract": 0}
    final_missing = {"doi": 0, "pmid": 0, "abstract": 0}
    fetched_success = {"doi": 0, "pmid": 0, "abstract": 0}
    fetch_failed = {"doi": 0, "pmid": 0, "abstract": 0}

    for entry in initial_entries:
        if not entry['fields'].get('doi') or not is_valid_doi(entry['fields'].get('doi')): initial_missing["doi"] += 1
        if not entry['fields'].get('pmid') or not is_valid_pmid(entry['fields'].get('pmid')): initial_missing["pmid"] += 1
        abstract = entry['fields'].get('abstract', '')
        if not abstract or (isinstance(abstract, str) and abstract.startswith("[")): initial_missing["abstract"] += 1

    for entry in final_entries:
        if not entry['fields'].get('doi') or not is_valid_doi(entry['fields'].get('doi')): final_missing["doi"] += 1
        if not entry['fields'].get('pmid') or not is_valid_pmid(entry['fields'].get('pmid')): final_missing["pmid"] += 1
        abstract = entry['fields'].get('abstract', '')
        if not abstract or (isinstance(abstract, str) and abstract.startswith("[")): final_missing["abstract"] += 1

    for status in fetch_statuses:
        if status:
            if status.get("doi") == "Fetched": fetched_success["doi"] += 1
            elif status.get("doi", "").startswith("Failed"): fetch_failed["doi"] += 1
            if status.get("pmid", "").startswith("Fetched"): fetched_success["pmid"] += 1
            elif status.get("pmid", "").startswith("Failed"): fetch_failed["pmid"] += 1
            if status.get("abstract", "").startswith("Fetched"): fetched_success["abstract"] += 1
            elif status.get("abstract", "").startswith("Failed"): fetch_failed["abstract"] += 1

    report.extend(["\n--- Initial State ---",
                   f"Missing/Invalid DOI:      {initial_missing['doi']:>5} ({initial_missing['doi']/num_total:.1%})",
                   f"Missing/Invalid PMID:     {initial_missing['pmid']:>5} ({initial_missing['pmid']/num_total:.1%})",
                   f"Missing/Placeholder Abstract: {initial_missing['abstract']:>5} ({initial_missing['abstract']/num_total:.1%})",
                   "\n--- Fetching Summary (This Run) ---",
                   f"DOIs Fetched:     {fetched_success['doi']:>5}",
                   f"PMIDs Fetched:    {fetched_success['pmid']:>5}",
                   f"Abstracts Fetched:{fetched_success['abstract']:>5}",
                   f"DOI Fetch Fails:  {fetch_failed['doi']:>5}",
                   f"PMID Fetch Fails: {fetch_failed['pmid']:>5}",
                   f"Abstract Fetch Fails:{fetch_failed['abstract']:>5}",
                   "\n--- Final State ---",
                   f"Missing/Invalid DOI:      {final_missing['doi']:>5} ({final_missing['doi']/num_total:.1%})",
                   f"Missing/Invalid PMID:     {final_missing['pmid']:>5} ({final_missing['pmid']/num_total:.1%})",
                   f"Missing/Placeholder Abstract: {final_missing['abstract']:>5} ({final_missing['abstract']/num_total:.1%})"])
    return "\n".join(report)

# --- Main Orchestrator ---
async def run_consolidation(input_path, output_path, input_format, args):
    """Main async orchestrator for parsing, fetching, and writing."""
    if input_format == 'bibtex': entries = parse_bibtex(input_path)
    elif input_format == 'text': entries = parse_endnote_text(input_path)
    else: print(f"Error: Unsupported input format.", file=sys.stderr); return None
    if entries is None: return None
    initial_entries_snapshot = [entry.copy() for entry in entries]

    semaphore = asyncio.Semaphore(args.concurrency)
    connector = aiohttp.TCPConnector(limit=args.concurrency * 5, limit_per_host=max(1, args.concurrency // 2), ssl=False)

    final_entries = []
    fetch_statuses = []
    process_errors = 0

    pbar = tqdm(total=len(entries), desc="Processing Entries", unit="entry", file=sys.stdout, disable=not TQDM_AVAILABLE, bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]') if TQDM_AVAILABLE else None

    try:
        stderr_wrapper = TqdmStderrWrapper(pbar if pbar else tqdm)
        with contextlib.redirect_stderr(stderr_wrapper):
            async with aiohttp.ClientSession(connector=connector) as session:
                tasks = [process_single_entry(session, semaphore, entry, args.force_refetch, args.enable_scraping)
                         for entry in entries]
                results_temp = []
                for future in asyncio.as_completed(tasks):
                    try:
                        result = await future
                        results_temp.append(result)
                    except Exception as e:
                         tqdm.write(f"Error processing an entry task: {e}", file=sys.stderr)
                         process_errors += 1
                         results_temp.append((None, {"error": f"Task failed: {e}"}))
                    finally:
                        if pbar: pbar.update(1)

        for result in results_temp:
             if isinstance(result, tuple) and len(result) == 2:
                 final_entries.append(result[0] if result[0] is not None else {"error": "Task returned None entry", "fields":{}})
                 fetch_statuses.append(result[1])
             else:
                 tqdm.write(f"Error: Unexpected result structure from task: {result}", file=sys.stderr)
                 process_errors += 1
                 final_entries.append({"error": "Task processing failed unexpectedly", "fields": {}})
                 fetch_statuses.append(None)

    finally:
        if pbar: pbar.close()

    if process_errors:
        print(f"\nWarning: {process_errors} entries encountered exceptions during processing.", file=sys.stderr)

    print(f"\nGenerating output in {input_format} format...")
    if input_format == 'bibtex': output_content = generate_bibtex(final_entries)
    elif input_format == 'text': output_content = generate_endnote_text(final_entries)
    else: output_content = None

    if output_content:
        try:
            with open(output_path, 'w', encoding='utf-8') as outfile: outfile.write(output_content)
            print(f"Successfully saved consolidated library to: {output_path}")
        except Exception as e:
            print(f"Error writing output file '{output_path}': {e}", file=sys.stderr); return None
    else:
         print(f"Error: Failed to generate output content.", file=sys.stderr); return None

    report = generate_report(initial_entries_snapshot, final_entries, fetch_statuses)
    print("\n" + report)
    return final_entries

# --- Command Line Interface ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Consolidate and fetch missing metadata (DOI, PMID, Abstract) for reference libraries.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("input_file", help="Path to the input reference file (.txt for EndNote, .bib for BibTeX).")
    parser.add_argument("-o", "--output", help="Optional: Path for the output consolidated file. If omitted, defaults to <input_basename>_consolidated.<ext>.", default=None)
    parser.add_argument("--enable-scraping", action="store_true", help="Enable abstract fetching via web scraping (DOI required) as a fallback if API fails. Use with caution!")
    parser.add_argument("--force-refetch", action="store_true", help="Attempt to fetch DOI, PMID, and Abstract even if they already exist in the input file.")
    parser.add_argument("--concurrency", type=int, default=MAX_CONCURRENT_REQUESTS, help="Maximum number of concurrent network requests.")
    parser.add_argument("--email", default=EMAIL_ADDRESS, help="Your email address for API politeness.")
    parser.add_argument("--api-key", default=None, help="Optional: NCBI API key (Not fully implemented for query params yet, but adjusts delay).")
    args = parser.parse_args()

    if args.email == "your.email@example.com":
        print("CRITICAL ERROR: Please provide your email address using the --email argument or by editing EMAIL_ADDRESS.", file=sys.stderr); sys.exit(1)
    EMAIL_ADDRESS = args.email
    REQ_USER_AGENT = f"PythonRefConsolidator/1.1 (mailto:{EMAIL_ADDRESS})"

    if args.api_key:
        print("Info: NCBI API Key provided. Adjusting NCBI delay.")
        current_domain_delays["eutils.ncbi.nlm.nih.gov"] = 0.11

    start_time = time.time()
    input_path_arg = args.input_file
    if not os.path.exists(input_path_arg): print(f"Error: Input file not found: {input_path_arg}", file=sys.stderr); sys.exit(1)

    script_directory = os.path.dirname(os.path.abspath(input_path_arg))
    input_filename = os.path.basename(input_path_arg)
    base_name, input_ext = os.path.splitext(input_filename)

    input_format = None
    if input_ext.lower() == '.bib': input_format = 'bibtex'
    elif input_ext.lower() == '.txt': input_format = 'text'
    else: print(f"Error: Unsupported input file extension '{input_ext}'.", file=sys.stderr); sys.exit(1)

    output_filepath = args.output
    if not output_filepath:
        output_filename = f"{base_name}{OUTPUT_SUFFIX}{input_ext}"
        output_filepath = os.path.join(script_directory, output_filename)
    else:
        out_base, out_ext = os.path.splitext(output_filepath)
        if not out_ext: output_filepath += input_ext
        elif out_ext.lower() != input_ext.lower(): print(f"Warning: Output file extension {out_ext} differs from input {input_ext}.", file=sys.stderr)

    previous_output_filepath = os.path.join(script_directory, f"{base_name}{PREVIOUS_OUTPUT_SUFFIX}{input_ext}")
    actual_input_path = input_path_arg
    if os.path.exists(previous_output_filepath) and not args.force_refetch:
        print(f"Found previous output file: {os.path.basename(previous_output_filepath)}. Using it as input.")
        actual_input_path = previous_output_filepath
    else:
        print(f"Using original input file: {input_filename}")
        if args.force_refetch: print("(--force-refetch used, will attempt to fetch all data)")

    print(f"\nStarting consolidation process...")
    print(f"Input:  {os.path.basename(actual_input_path)}")
    print(f"Output: {os.path.basename(output_filepath)}")
    print(f"Format: {input_format}")
    print(f"Scraping Enabled: {args.enable_scraping}")
    print(f"Concurrency: {args.concurrency}")
    print(f"NCBI API Key: {'Provided (delay adjusted)' if args.api_key else 'Not provided (default delay)'}")

    MAX_CONCURRENT_REQUESTS = args.concurrency

    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    final_data = asyncio.run(run_consolidation(actual_input_path, output_filepath, input_format, args))

    end_time = time.time()
    if final_data is not None: print(f"\nConsolidation finished successfully.")
    else: print(f"\nConsolidation finished with errors.")
    print(f"Total execution time: {end_time - start_time:.2f} seconds")