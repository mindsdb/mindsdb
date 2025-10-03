import concurrent.futures
import io
import re
import traceback
from threading import Lock
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse, urlunparse

import html2text
import fitz  # PyMuPDF
import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from mindsdb.utilities import log

logger = log.getLogger(__name__)


def pdf_to_markdown(response: requests.Response, gap_threshold: int = 10) -> str:
    """
    Convert a PDF document to Markdown text.

    Args:
        response: The response object containing PDF data.
        gap_threshold: Vertical gap size that triggers a new line (default 10).

    Returns:
        Markdown text extracted from PDF.

    Raises:
        Exception if the PDF cannot be processed.
    """
    try:
        file_stream = io.BytesIO(response.content)
        document = fitz.open(stream=file_stream, filetype="pdf")
    except Exception as e:
        raise Exception("Failed to process PDF data: " + str(e))

    markdown_lines = []
    for page_num in range(len(document)):
        page = document.load_page(page_num)
        blocks = page.get_text("blocks")
        blocks.sort(key=lambda block: (block[1], block[0]))

        previous_block_bottom = 0
        for block in blocks:
            y0, y1, block_text = block[1], block[3], block[4]
            if y0 - previous_block_bottom > gap_threshold:
                markdown_lines.append("")
            markdown_lines.append(block_text.strip())
            previous_block_bottom = y1
        markdown_lines.append("")

    document.close()
    return "\n".join(markdown_lines)


def is_valid(url: str) -> bool:
    """
    Check if a URL is valid.

    Args:
        url: URL to check.

    Returns:
        True if URL has a scheme and netloc, else False.
    """
    parsed = urlparse(url)
    return bool(parsed.scheme and parsed.netloc)


def create_session_with_retries() -> requests.Session:
    """
    Create a requests.Session with retry logic.

    Returns:
        Configured requests.Session object.
    """
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def get_all_website_links(url: str, headers: Optional[Dict[str, str]] = None) -> dict:
    """
    Fetch all website links from a URL, extract HTML and text content.

    Args:
        url: URL to fetch links from.
        headers: Optional headers for the request.

    Returns:
        Dictionary with 'url', 'urls', 'html_content', 'text_content', 'error'.
    """
    logger.info(f"Crawling: {url} ...")
    urls = set()
    domain_name = urlparse(url).netloc

    try:
        session = create_session_with_retries()
        if headers is None:
            headers = {}
        headers.setdefault(
            "User-Agent",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.3"
        )

        response = session.get(url, headers=headers, timeout=10)
        content_type = response.headers.get("Content-Type", "").lower()

        if "application/pdf" in content_type:
            content_html = "PDF"
            content_text = pdf_to_markdown(response)
        else:
            content_html = response.text
            soup = BeautifulSoup(content_html, "html.parser")
            content_text = get_readable_text_from_soup(soup)

            for a_tag in soup.find_all("a"):
                href = a_tag.attrs.get("href")
                if not href:
                    continue
                href = urljoin(url, href)
                parsed_href = urlparse(href)
                href = urlunparse((parsed_href.scheme, parsed_href.netloc, parsed_href.path, "", "", ""))
                if not is_valid(href):
                    continue
                if href in urls:
                    continue
                if domain_name != parsed_href.netloc:
                    continue
                urls.add(href.rstrip("/"))

    except Exception as e:
        error_message = traceback.format_exc().splitlines()[-1]
        logger.error("An exception occurred: %s", str(e))
        return {"url": url, "urls": urls, "html_content": "", "text_content": "", "error": str(error_message)}

    return {"url": url, "urls": urls, "html_content": content_html, "text_content": content_text, "error": None}


def parallel_get_all_website_links(urls: List[str]) -> Dict[str, dict]:
    """
    Fetch all website links from multiple URLs in parallel.

    Args:
        urls: List of URLs.

    Returns:
        Dictionary mapping URL to its extracted data.
    """
    url_contents = {}
    if len(urls) <= 10:
        for url in urls:
            url_contents[url] = get_all_website_links(url)
        return url_contents

    with concurrent.futures.ProcessPoolExecutor() as executor:
        future_to_url = {executor.submit(get_all_website_links, url): url for url in urls}
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                url_contents[url] = future.result()
            except Exception as exc:
                logger.error(f"{url} generated an exception: {exc}")
    return url_contents


def get_readable_text_from_soup(soup: BeautifulSoup) -> str:
    """
    Convert BeautifulSoup HTML content to readable Markdown text.

    Args:
        soup: BeautifulSoup object.

    Returns:
        Markdown text.
    """
    html_converter = html2text.HTML2Text()
    html_converter.ignore_links = False
    return html_converter.handle(str(soup))


def get_all_website_links_recursively(
    url: str,
    reviewed_urls: dict,
    limit: Optional[int] = None,
    crawl_depth: int = 1,
    current_depth: int = 0,
    filters: Optional[List[str]] = None,
    headers: Optional[Dict[str, str]] = None,
):
    """
    Recursively gather all links from a website up to a specified depth/limit.

    Args:
        url: Starting URL.
        reviewed_urls: Dictionary to keep track of URLs already processed.
        limit: Maximum number of URLs to crawl.
        crawl_depth: Max depth to crawl from base URL.
        current_depth: Current depth in recursion.
        filters: Optional list of regex filters to include URLs.
        headers: Optional HTTP headers for requests.
    """
    if limit is not None and len(reviewed_urls) >= limit:
        return reviewed_urls

    matches_filter = True if not filters else any(re.match(f, url) for f in filters)
    if url not in reviewed_urls and matches_filter:
        try:
            reviewed_urls[url] = get_all_website_links(url, headers=headers)
        except Exception as e:
            error_message = traceback.format_exc().splitlines()[-1]
            logger.error("An exception occurred: %s", str(e))
            reviewed_urls[url] = {"url": url, "urls": [], "html_content": "", "text_content": "", "error": str(error_message)}

    if crawl_depth is not None and crawl_depth == current_depth:
        return reviewed_urls

    to_rev_url_list = []
    url_list_lock = Lock()
    for new_url in reviewed_urls[url]["urls"]:
        matches_filter = True if not filters else any(re.match(f, new_url) for f in filters)
        if not matches_filter or new_url in reviewed_urls or new_url in to_rev_url_list:
            continue

        with url_list_lock:
            if limit is None or len(reviewed_urls) < limit:
                reviewed_urls[new_url] = {}
                to_rev_url_list.append(new_url)
            else:
                break

    if to_rev_url_list:
        new_revised_urls = parallel_get_all_website_links(to_rev_url_list)
        reviewed_urls.update(new_revised_urls)
        for new_url in new_revised_urls:
            get_all_website_links_recursively(
                new_url,
                reviewed_urls,
                limit,
                crawl_depth=crawl_depth,
                current_depth=current_depth + 1,
                filters=filters,
            )


def get_all_websites(
    urls: List[str],
    limit: int = 1,
    html: bool = False,
    crawl_depth: int = 1,
    filters: Optional[List[str]] = None,
    headers: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    """
    Crawl a list of websites and return results as a pandas DataFrame.

    Args:
        urls: List of URLs to crawl.
        limit: Maximum number of pages to crawl.
        html: Include HTML content in output if True.
        crawl_depth: Maximum depth for recursion.
        filters: Optional regex filters for URLs.
        headers: Optional HTTP headers.

    Returns:
        DataFrame containing crawled data.
    """
    reviewed_urls = {}

    def fetch_url(url: str):
        url = url.strip("'").rstrip("/")
        if urlparse(url).scheme == "":
            url = "https://" + url
        get_all_website_links_recursively(url, reviewed_urls, limit, crawl_depth=crawl_depth, filters=filters, headers=headers)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_url = {executor.submit(fetch_url, url): url for url in urls}
        for future in concurrent.futures.as_completed(future_to_url):
            future.result()

    columns_to_ignore = ["urls"]
    if not html:
        columns_to_ignore.append("html_content")

    df = dict_to_dataframe(reviewed_urls, columns_to_ignore=columns_to_ignore, index_name="url")

    if not df.empty and df[df.error.isna()].empty:
        raise Exception(str(df.iloc[0].error))
    return df


def dict_to_dataframe(dict_of_dicts: dict, columns_to_ignore: Optional[List[str]] = None, index_name: Optional[str] = None) -> pd.DataFrame:
    """
    Convert a dictionary of dictionaries to a pandas DataFrame.

    Args:
        dict_of_dicts: Dictionary to convert.
        columns_to_ignore: Columns to drop.
        index_name: Name of the DataFrame index.

    Returns:
        pandas DataFrame.
    """
    df = pd.DataFrame.from_dict(dict_of_dicts, orient="index")
    if columns_to_ignore:
        for column in columns_to_ignore:
            if column in df.columns:
                df = df.drop(column, axis=1)
    if index_name:
        df.index.name = index_name
    return df
