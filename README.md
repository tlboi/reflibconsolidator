# Reference Library Consolidator

If you're an academic, you must have experienced the frustration of an incomplete reference library on EndNote, Mendeley, or BibTeX. This handy script can read .txt and .bib libraries and consolidates them by retrieving as much information as possible from the web. 

## Overview

This Python script automates the process of cleaning, consolidating, and enriching reference library files. It can handle libraries exported from EndNote (`.txt` format) or standard BibTeX (`.bib`) files.

The script aims to:

1.  **Parse** the input file into a structured format.
2.  **Fetch missing identifiers:**
    *   Retrieves **DOIs** using the Crossref API (based on title, author, year with fallbacks).
    *   Retrieves **PMIDs** using the NCBI E-Utilities ESearch API (prioritizing DOI search, falling back to title/author/year).
3.  **Fetch missing abstracts:**
    *   Primarily uses the NCBI E-Utilities EFetch API (requires PMID).
    *   Optionally uses **web scraping** (via DOI) as a fallback (less reliable).
4.  **Handle API Rate Limits:** Implements politeness delays for different domains and dynamically adjusts delays for NCBI if rate limits (HTTP 429) are encountered.
5.  **Support Iterative Runs:** If an output file from a previous run (`*_consolidated.<ext>`) exists, the script will use it as input, only attempting to fetch data that was previously missing or failed. This allows gradual enrichment over multiple runs.
6.  **Output** the consolidated and enriched library in the **same format** as the input file (`.txt` or `.bib`).
7.  **Report** a summary of the initial state, fetching success/failure rates, and the final state of the library.

## Features

*   **Input Formats:** EndNote `.txt` (simple format) and BibTeX `.bib`.
*   **Output Formats:** Matches input format (`.txt` or `.bib`).
*   **Metadata Fetching:** DOI (Crossref), PMID (NCBI ESearch), Abstract (NCBI EFetch API primary, optional scraping fallback).
*   **Asynchronous Fetching:** Uses `asyncio` and `aiohttp` for efficient concurrent requests.
*   **Rate Limiting:** Respects API politeness rules with configurable delays and dynamic backoff for NCBI.
*   **Iterative Processing:** Automatically uses previous output to fill gaps incrementally.
*   **Optional Scraping:** Fallback abstract retrieval via DOI (use with caution).
*   **Progress Bar:** Uses `tqdm` (if installed) for visual progress.
*   **Reporting:** Summarizes fetching results and library state.
*   **NCBI API Key Support:** Can utilize an NCBI API key (via argument) to potentially increase request limits (adjusts internal delays).

## Requirements

*   Python 3.8+
*   Required Libraries:
    *   `aiohttp`: For asynchronous HTTP requests.
    *   `lxml`: For efficient XML parsing (NCBI API responses).
    *   `beautifulsoup4`: For HTML parsing (used only if scraping is enabled).
    *   `bibtexparser`: For reading and writing BibTeX files.
*   Optional Library:
    *   `tqdm`: For displaying a progress bar during fetching.

Install dependencies using pip:

pip install aiohttp lxml beautifulsoup4 bibtexparser tqdm

(If you don't need the progress bar, you can omit tqdm)

## Usage

The script is run from the command line.

**Basic Syntax:**

python consolidate_references.py <input_file> --email <your_email> [options]

**Required Arguments:**

*   `input_file`: Path to your input reference library file (must end in `.txt` or `.bib`).
*   `--email <your_email>`: **Your email address.** This is crucial for using the Crossref and NCBI APIs politely. Replace `<your_email>` with your actual email. (Alternatively, you can edit the `EMAIL_ADDRESS` variable directly in the script).

**Common Optional Arguments:**

*   `-o <output_file>`, `--output <output_file>`: Specify a custom path for the output file. If omitted, it defaults to `<input_basename>_consolidated.<ext>` in the same directory as the input file.
*   `--enable-scraping`: Turns on the fallback mechanism to scrape abstracts from publisher websites using the DOI if the preferred NCBI EFetch (via PMID) fails. **Use this with caution!** Scraping is unreliable and may lead to IP blocks.
*   `--force-refetch`: Forces the script to try fetching DOI, PMID, and Abstract for *all* entries, even if valid data already exists in the input file. Useful for updating an entire library.
*   `--concurrency <number>`: Sets the maximum number of simultaneous network requests (default: 5). Lower this (e.g., to 2 or 3) if you still encounter rate limit errors, especially without an API key.
*   `--api-key <your_ncbi_key>`: Provide your registered NCBI API key. This *highly recommended* option allows the script to use faster request rates for NCBI services (adjusts internal delays).

**Examples:**

1.  **Basic run on a BibTeX file:**
    python consolidate_references.py my_references.bib --email user@example.com
    *(Output will be `my_references_consolidated.bib`)*

2.  **Basic run on an EndNote text file:**
    python consolidate_references.py my_library.txt --email user@example.com
    *(Output will be `my_library_consolidated.txt`)*

3.  **Run with scraping enabled and custom output name:**
    python consolidate_references.py my_references.bib --email user@example.com --enable-scraping -o my_references_final.bib

4.  **Iterative run (after a previous run failed for some entries):**
    # First run (might have failures)
    python consolidate_references.py my_library.txt --email user@example.com
    # Second run (reads my_library_consolidated.txt, tries only missing items)
    python consolidate_references.py my_library.txt --email user@example.com

5.  **Run using an NCBI API Key:**
    python consolidate_references.py my_library.bib --email user@example.com --api-key YOUR_ACTUAL_NCBI_API_KEY

6.  **Adjust Concurrency:**
    python consolidate_references.py my_library.bib --email user@example.com --concurrency 3

## How it Works Internally

1.  **Detect Format:** Determines if the input is `.txt` or `.bib`.
2.  **Check for Previous Output:** Looks for a file named `<input_basename>_consolidated.<ext>`. If found and `--force-refetch` is not used, it loads this file for iterative processing. Otherwise, it loads the original input file.
3.  **Parse:** Reads the input file into a standardized internal list of dictionaries using either `parse_endnote_text` or `parse_bibtex`.
4.  **Fetch Asynchronously:**
    *   Creates asynchronous tasks for each entry using `process_single_entry`.
    *   `process_single_entry` checks if DOI, PMID, or Abstract are missing or invalid (or if `--force-refetch` is used).
    *   It calls specific async fetching functions (`get_doi_from_crossref`, `get_pmid_from_pubmed`, `fetch_abstract_for_entry`) as needed.
    *   These functions use `async_fetch_resource`, which handles the actual HTTP requests, respecting `DOMAIN_DELAYS`, managing concurrency via a semaphore, handling retries, and dynamically adjusting delays on 429 errors.
    *   `get_doi_from_crossref` uses fallback search strategies (Title/Author/Year -> Title/Year -> Title Only).
    *   `fetch_abstract_for_entry` prioritizes NCBI EFetch via PMID and falls back to scraping via DOI only if `--enable-scraping` is active and the EFetch fails.
    *   `asyncio.gather` (or `asyncio.as_completed` with `tqdm`) runs these tasks concurrently.
5.  **Generate Output:** The final list of enriched dictionaries is converted back into the original input format (`.txt` or `.bib`) using `generate_endnote_text` or `generate_bibtex`.
6.  **Report:** A summary comparing the initial number of missing items to the final number is printed to the console.

## Important Notes

*   **Email Address:** Providing a valid email address via `--email` or by editing the script is essential for API politeness.
*   **Rate Limits:** Be mindful of API rate limits, especially for NCBI without an API key. If you see many 429 errors, reduce concurrency (`--concurrency`) and/or increase the `BASE_DOMAIN_DELAYS` values in the script. Getting an NCBI API key is the best solution.
*   **Scraping Fragility:** The `--enable-scraping` option is experimental and prone to breaking due to website changes or anti-scraping measures. Use it as a last resort.
*   **BibTeX Formatting:** The BibTeX output generation uses basic formatting. Complex LaTeX commands within fields might not be perfectly preserved.
*   **Text Reconstruction:** The EndNote `.txt` output is reconstructed based on the parsed fields. While it tries to include fetched DOI/PMID, the exact original formatting might be slightly different.
