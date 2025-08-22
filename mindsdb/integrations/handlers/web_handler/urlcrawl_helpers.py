import concurrent.futures
import io
import re
import traceback
from threading import Lock
from typing import List
from urllib.parse import urljoin, urlparse, urlunparse

import html2text
import fitz  # PyMuPDF
import pandas as pd
import requests
from bs4 import BeautifulSoup
from mindsdb.utilities import log

logger = log.getLogger(__name__)


def pdf_to_markdown(response, gap_threshold=10):
    """
    Convert a PDF document to Markdown text.

    Args:
        response: the response object containing the PDF data
        gap_threshold (int): the vertical gap size that triggers a new line in the output (default 10)

    Returns:
        A string containing the converted Markdown text.

    Raises:
        Exception -- if the PDF data cannot be processed.
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
            y0 = block[1]
            y1 = block[3]
            block_text = block[4]

            # Check if there's a large vertical gap between this block and the previous one
            if y0 - previous_block_bottom > gap_threshold:
                markdown_lines.append("")

            markdown_lines.append(block_text)
            previous_block_bottom = y1

        markdown_lines.append("")

    document.close()

    return "\n".join(markdown_lines)


def is_valid(url) -> bool:
    """
    Check if a URL is valid.

    Args:
        url: the URL to check

    Returns:
        bool: True if the URL is valid, False otherwise.
    """
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)


def parallel_get_all_website_links(urls) -> dict:
    """
    Fetch all website links from a list of URLs.

    Args:
        urls (list): a list of URLs to fetch links from

    Returns:
        A dictionary mapping each URL to a list of links found on that URL.

    Raises:
        Exception: if an error occurs while fetching links from a URL.
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
                # don't raise the exception, just log it, continue processing other urls

    return url_contents


def get_all_website_links(url, headers: dict = None) -> dict:
    """
    Fetch all website links from a URL.

    Args:
        url (str): the URL to fetch links from
        headers (dict): a dictionary of headers to use when fetching links

    Returns:
        A dictionary containing the URL, the extracted links, the HTML content, the text content, and any error that occurred.
    """
    logger.info("rawling: {url} ...".format(url=url))
    urls = set()

    domain_name = urlparse(url).netloc
    try:
        session = requests.Session()

        # Add headers to mimic a real browser request
        if headers is None:
            headers = {}
        if "User-Agent" not in headers:
            headers["User-Agent"] = (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.3"
            )

        response = session.get(url, headers=headers)
        if "cookie" in response.request.headers:
            session.cookies.update(response.cookies)

        content_type = response.headers.get("Content-Type", "").lower()

        if "application/pdf" in content_type:
            content_html = "PDF"
            content_text = pdf_to_markdown(response)
        else:
            content_html = response.text

            # Parse HTML content with BeautifulSoup
            soup = BeautifulSoup(content_html, "html.parser")
            content_text = get_readable_text_from_soup(soup)
            for a_tag in soup.find_all("a"):
                href = a_tag.attrs.get("href")
                if href == "" or href is None:
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

                href = href.rstrip("/")
                urls.add(href)

    except Exception as e:
        error_message = traceback.format_exc().splitlines()[-1]
        logger.error("An exception occurred: %s", str(e))
        return {
            "url": url,
            "urls": urls,
            "html_content": "",
            "text_content": "",
            "error": str(error_message),
        }

    return {
        "url": url,
        "urls": urls,
        "html_content": content_html,
        "text_content": content_text,
        "error": None,
    }


def get_readable_text_from_soup(soup) -> str:
    """
    Extract readable text from a BeautifulSoup object and convert it to Markdown.

    Args:
        soup (BeautifulSoup): a BeautifulSoup object

    Returns:
        The extracted text in Markdown format.
    """
    html_converter = html2text.HTML2Text()
    html_converter.ignore_links = False
    return html_converter.handle(str(soup))


def get_all_website_links_recursively(
    url,
    reviewed_urls,
    limit=None,
    crawl_depth: int = 1,
    current_depth: int = 0,
    filters: List[str] = None,
    headers=None,
):
    """
    Recursively gathers all links from a given website up to a specified limit.

    Args:
        url (str): The starting URL to fetch links from.
        reviewed_urls (dict): A dictionary to keep track of reviewed URLs and associated data.
        limit (int, optional): The maximum number of URLs to process.
        crawl_depth: How deep to crawl from each base URL. 0 = scrape given URLs only
        current_depth: How deep we are currently crawling from the base URL.
        filters (List[str]): Crawl URLs that only match these regex patterns.

    TODO: Refactor this function to use a iterative aproach instead of recursion
    """
    if limit is not None:
        if len(reviewed_urls) >= limit:
            return reviewed_urls

    if not filters:
        matches_filter = True
    else:
        matches_filter = any(re.match(f, url) is not None for f in filters)
    if url not in reviewed_urls and matches_filter:
        try:
            reviewed_urls[url] = get_all_website_links(url, headers=headers)
        except Exception as e:
            error_message = traceback.format_exc().splitlines()[-1]
            logger.error("An exception occurred: %s", str(e))
            reviewed_urls[url] = {
                "url": url,
                "urls": [],
                "html_content": "",
                "text_content": "",
                "error": str(error_message),
            }

    if crawl_depth is not None and crawl_depth == current_depth:
        return reviewed_urls

    to_rev_url_list = []

    # create a list of new urls to review that don't exist in the already reviewed ones
    for new_url in reviewed_urls[url]["urls"]:
        if not filters:
            matches_filter = True
        else:
            matches_filter = any(re.match(f, new_url) is not None for f in filters)
        if not matches_filter:
            continue
        # if this is already in the urls, then no need to go and crawl for it
        if new_url in reviewed_urls or new_url in to_rev_url_list:
            continue

        # insert immediately to count limit between threads. fill later
        url_list_lock = Lock()
        with url_list_lock:
            if limit is None or len(reviewed_urls) < limit:
                reviewed_urls[new_url] = {}
                to_rev_url_list.append(new_url)
            else:
                break

    if len(to_rev_url_list) > 0:
        new_revised_urls = parallel_get_all_website_links(to_rev_url_list)

        reviewed_urls.update(new_revised_urls)

        for new_url in new_revised_urls:
            get_all_website_links_recursively(
                new_url, reviewed_urls, limit, crawl_depth=crawl_depth, current_depth=current_depth + 1, filters=filters
            )


def get_all_websites(
    urls, limit=1, html=False, crawl_depth: int = 1, filters: List[str] = None, headers: dict = None
) -> pd.DataFrame:
    """
    Crawl a list of websites and return a DataFrame containing the results.

    Args:
        urls (list): a list of URLs to crawl
        limit (int): Absolute max number of web pages to crawl, regardless of crawl depth.
        crawl_depth (int): Crawl depth for URLs.
        html (bool): a boolean indicating whether to include the HTML content in the results
        filters (List[str]): Crawl URLs that only match these regex patterns.
        headers (dict): headers of request

    Returns:
        A DataFrame containing the results.
    """
    reviewed_urls = {}

    def fetch_url(url, crawl_depth: int = 1, filters: List[str] = None):
        # Allow URLs to be passed wrapped in quotation marks so they can be used
        # directly from the SQL editor.
        if url.startswith("'") and url.endswith("'"):
            url = url[1:-1]
        url = url.rstrip("/")
        if urlparse(url).scheme == "":
            # Try HTTPS first
            url = "https://" + url
        get_all_website_links_recursively(
            url, reviewed_urls, limit, crawl_depth=crawl_depth, filters=filters, headers=headers
        )

    # Use a ThreadPoolExecutor to run the helper function in parallel.
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_url = {executor.submit(fetch_url, url, crawl_depth=crawl_depth, filters=filters): url for url in urls}

        for future in concurrent.futures.as_completed(future_to_url):
            future.result()

    columns_to_ignore = ["urls"]
    if html is False:
        columns_to_ignore += ["html_content"]
    df = dict_to_dataframe(reviewed_urls, columns_to_ignore=columns_to_ignore, index_name="url")

    if not df.empty and df[df.error.isna()].empty:
        raise Exception(str(df.iloc[0].error))
    return df


def dict_to_dataframe(dict_of_dicts, columns_to_ignore=None, index_name=None) -> pd.DataFrame:
    """
    Convert a dictionary of dictionaries to a DataFrame.

    Args:
        dict_of_dicts (dict): a dictionary of dictionaries
        columns_to_ignore (list): a list of columns to ignore
        index_name (str): the name of the index column
    Returns:
        A DataFrame containing the data.
    """
    df = pd.DataFrame.from_dict(dict_of_dicts, orient="index")

    if columns_to_ignore:
        for column in columns_to_ignore:
            if column in df.columns:
                df = df.drop(column, axis=1)

    if index_name:
        df.index.name = index_name

    return df
