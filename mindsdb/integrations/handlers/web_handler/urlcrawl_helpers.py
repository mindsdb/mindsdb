import concurrent.futures
import io
import traceback
from threading import Lock
from urllib.parse import urljoin, urlparse

import fitz  # PyMuPDF
import pandas as pd
import requests
from bs4 import BeautifulSoup
from mindsdb.utilities import log

logger = log.getLogger(__name__)


def pdf_to_markdown(response):

    file_stream = io.BytesIO(response.content)

    document = fitz.open(stream=file_stream, filetype="pdf")

    markdown_text = ""
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
            if y0 - previous_block_bottom > 10:
                markdown_text += "\n"

            markdown_text += block_text + "\n"
            previous_block_bottom = y1

        markdown_text += "\n"

    document.close()

    return markdown_text

def is_valid(url):
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)


def parallel_get_all_website_links(urls):
    url_contents = {}

    if len(urls) <= 10:
        for url in urls:
            url_contents[url] = get_all_website_links(url)
        return url_contents

    with concurrent.futures.ProcessPoolExecutor() as executor:
        future_to_url = {
            executor.submit(get_all_website_links, url): url for url in urls
        }
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                url_contents[url] = future.result()
            except Exception as exc:
                logger.error(f'{url} generated an exception: {exc}')

    return url_contents


def get_all_website_links(url):
    logger.info("crawling: {url} ...".format(url=url))
    urls = set()

    domain_name = urlparse(url).netloc
    try:
        session = requests.Session()

        # Add headers to mimic a real browser request
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36"
        }

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
            for a_tag in soup.findAll("a"):
                href = a_tag.attrs.get("href")
                if href == "" or href is None:
                    continue
                href = urljoin(url, href)
                parsed_href = urlparse(href)

                href = (
                    parsed_href.scheme + "://" + parsed_href.netloc + parsed_href.path
                )
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


def get_readable_text_from_soup(soup):
    markdown_output = ""

    for tag in soup.find_all(
        ["h1", "h2", "h3", "h4", "h5", "h6", "p", "a", "ul", "ol", "li"]
    ):
        if tag.name in ["h1", "h2", "h3", "h4", "h5", "h6"]:
            markdown_output += (
                "#" * int(tag.name[1]) + " " + tag.get_text().strip() + "\n\n"
            )
        elif tag.name == "p":
            markdown_output += tag.get_text().strip() + "\n\n"
        elif tag.name == "a":
            markdown_output += f"[{tag.get_text().strip()}]({tag.get('href')})\n\n"
        elif tag.name == "ul":
            for li in tag.find_all("li"):
                markdown_output += f"* {li.get_text().strip()}\n"
            markdown_output += "\n"
        elif tag.name == "ol":
            for index, li in enumerate(tag.find_all("li")):
                markdown_output += f"{index + 1}. {li.get_text().strip()}\n"
            markdown_output += "\n"

    return markdown_output


def get_all_website_links_rec(url, reviewd_urls, limit=None):
    if limit is not None:
        if len(reviewd_urls) >= limit:
            return reviewd_urls

    if url not in reviewd_urls:
        try:
            reviewd_urls[url] = get_all_website_links(url)
        except Exception as e:
            error_message = traceback.format_exc().splitlines()[-1]
            logger.error("An exception occurred: %s", str(e))
            reviewd_urls[url] = {
                "url": url,
                "urls": [],
                "html_content": "",
                "text_content": "",
                "error": str(error_message),
            }

    to_rev_url_list = []

    # create a list of new urls to review that don't exist in the already reviewed ones
    for new_url in reviewd_urls[url]["urls"]:
        # if this is already in the urls, then no need to go and crawl for it
        if new_url in reviewd_urls or new_url in to_rev_url_list:
            continue

        # insert immediately to count limit between threads. fill later
        url_list_lock = Lock()
        with url_list_lock:
            if limit is None or len(reviewd_urls) < limit:
                reviewd_urls[new_url] = {}
                to_rev_url_list.append(new_url)
            else:
                break

    if len(to_rev_url_list) > 0:
        new_revised_urls = parallel_get_all_website_links(to_rev_url_list)

        reviewd_urls.update(new_revised_urls)

        for new_url in new_revised_urls:
            get_all_website_links_rec(new_url, reviewd_urls, limit)


def get_all_websites(urls, limit=1, html=False):
    reviewd_urls = {}

    def fetch_url(url):
        # Allow URLs to be passed wrapped in quotation marks so they can be used
        # directly from the SQL editor.
        if url.startswith("'") and url.endswith("'"):
            url = url[1:-1]
        url = url.rstrip("/")
        if urlparse(url).scheme == "":
            # Try HTTPS first
            url = "https://" + url
        get_all_website_links_rec(url, reviewd_urls, limit)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_url = {executor.submit(fetch_url, url): url for url in urls}

        for future in concurrent.futures.as_completed(future_to_url):
            future.result()

    columns_to_ignore = ["urls"]
    if html is False:
        columns_to_ignore += ["html_content"]
    df = dict_to_dataframe(
        reviewd_urls, columns_to_ignore=columns_to_ignore, index_name="url"
    )
    print('get_all_websites', df)
    if not df.empty and df[df.error.isna()].empty:
        raise Exception(str(df.iloc[0].error))
    return df


def dict_to_dataframe(dict_of_dicts, columns_to_ignore=None, index_name=None):
    df = pd.DataFrame.from_dict(dict_of_dicts, orient="index")

    if columns_to_ignore:
        df = df.drop(columns_to_ignore, axis=1, errors="ignore")

    if index_name:
        df.index.name = index_name

    return df
