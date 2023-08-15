from urllib.parse import urlparse, urljoin
import logging
import re
import traceback
from threading import Lock
import concurrent.futures
import io

import requests
from bs4 import BeautifulSoup
import pandas as pd
import fitz  # PyMuPDF


def pdf_to_markdown(response):
    # Download the PDF from the given URL
    
    file_stream = io.BytesIO(response.content)

    # Open the PDF from the in-memory file
    document = fitz.open(stream=file_stream, filetype='pdf')

    markdown_text = ''
    for page_num in range(len(document)):
        page = document.load_page(page_num)

        # Get the blocks of text
        blocks = page.get_text("blocks")

        # Sort the blocks by their vertical position on the page
        blocks.sort(key=lambda block: (block[1], block[0]))

        previous_block_bottom = 0
        for block in blocks:
            y0 = block[1]
            y1 = block[3]
            block_text = block[4]

            # Check if there's a large vertical gap between this block and the previous one
            if y0 - previous_block_bottom > 10:
                markdown_text += '\n'

            markdown_text += block_text + '\n'
            previous_block_bottom = y1

        markdown_text += '\n'

    # Close the document
    document.close()

    return markdown_text


url_list_lock = Lock()


def is_valid(url):
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)


# this bad boy gets all the crawling done in parallel
def parallel_get_all_website_links(urls):
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
                logging.error(f'{url} generated an exception: {exc}')
   
    return url_contents


# this crawls one individual website
def get_all_website_links(url):
    logging.info('crawling: {url} ...'.format(url=url))
    urls = set()
            
    domain_name = urlparse(url).netloc
    try:
        # Create a session to handle cookies
        session = requests.Session()

        # Add headers to mimic a real browser request
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36"
        }

        # Send GET request
        response = session.get(url, headers=headers)
        # Accept cookies if necessary
        if 'cookie' in response.request.headers:
            session.cookies.update(response.cookies)

        content_type = response.headers.get('Content-Type', '').lower()

        if 'application/pdf' in content_type:
            
            content_html = 'PDF'
            content_text = pdf_to_markdown(response)
        else:
            content_html = response.text

            # Parse HTML content with BeautifulSoup
            soup = BeautifulSoup(content_html, 'html.parser')
            content_text = get_readable_text_from_soup(soup)
            for a_tag in soup.findAll("a"):
                href = a_tag.attrs.get("href")
                if href == "" or href is None:
                    continue
                href = urljoin(url, href)
                parsed_href = urlparse(href)
                
                href = parsed_href.scheme + "://" + parsed_href.netloc + parsed_href.path
                if not is_valid(href):
                    continue
                if href in urls:
                    continue
                if domain_name != parsed_href.netloc:
                    continue
                
                href = href.rstrip('/')    
                urls.add(href)

    except Exception as e:
        error_message = traceback.format_exc().splitlines()[-1]
        logging.error("An exception occurred: %s", str(e))
        return {'url':url,'urls':urls, 'html_content':'', 'text_content': '', 'error':str(error_message)}

    return {'url': url,'urls': urls, 'html_content': content_html, 'text_content': content_text, 'error': None}


def get_readable_text_from_soup(soup):

    # Start formatting as Markdown
    markdown_output = ""

    # Iterate through headings and paragraphs
    for tag in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'a', 'ul', 'ol', 'li']):
        if tag.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
            markdown_output += "#" * int(tag.name[1]) + " " + tag.get_text().strip() + "\n\n"
        elif tag.name == 'p':
            markdown_output += tag.get_text().strip() + "\n\n"
        elif tag.name == 'a':
            markdown_output += f"[{tag.get_text().strip()}]({tag['href']})\n\n"
        elif tag.name == 'ul':
            for li in tag.find_all('li'):
                markdown_output += f"* {li.get_text().strip()}\n"
            markdown_output += "\n"
        elif tag.name == 'ol':
            for index, li in enumerate(tag.find_all('li')):
                markdown_output += f"{index + 1}. {li.get_text().strip()}\n"
            markdown_output += "\n"

    return markdown_output


# this bad girl does the recursive crawling of the websites
def get_all_website_links_rec(url, reviewd_urls, limit=None):

    if limit is not None:
        if len(reviewd_urls) >= limit:
            return reviewd_urls

    if url not in reviewd_urls:
        # if something happens getting the website links for this url then log the error
        try:
            reviewd_urls[url] = get_all_website_links(url)
        except Exception as e:
            error_message = traceback.format_exc().splitlines()[-1]
            logging.error("An exception occurred: %s", str(e))
            reviewd_urls[url] = {'url': url, 'urls': [], 'html_content': '', 'text_content': '', 'error': str(error_message)}

    to_rev_url_list = []

    # create a list of new urls to review that don't exist in the already reviewed ones
    for new_url in reviewd_urls[url]['urls']:
        
        # if this is already in the urls, then no need to go and crawl for it
        if new_url in reviewd_urls or new_url in to_rev_url_list:
            continue
        
        # insert immediately to count limit between threads. fill later
        with url_list_lock:
            if limit is None or len(reviewd_urls) < limit:
                reviewd_urls[new_url] = {}
                to_rev_url_list.append(new_url)
            else:
                break

    # if there is something to fetch, go fetch
    if len(to_rev_url_list) > 0:
        new_revised_urls = parallel_get_all_website_links(to_rev_url_list)
    
        reviewd_urls.update(new_revised_urls)

        for new_url in new_revised_urls:
            get_all_website_links_rec(new_url, reviewd_urls, limit)


# this crawls the websites and retuns it all as a dataframe, ready to be served
def get_all_websites(urls, limit=1, html=False):
    reviewd_urls = {}

    # def fetch_url(url):
    #     url = url.rstrip('/')
    #     if urlparse(url).scheme == "":
    #         # Try HTTPS first
    #         url = "https://" + url
    #     reviewd_urls_iter = {}
    #     get_all_website_links_rec(url, reviewd_urls_iter, limit)
    #     return reviewd_urls_iter
    
    # reviewd_urls = fetch_url(urls[0])
    # Define a helper function that will be run in parallel.
    def fetch_url(url):
        url = url.rstrip('/')
        if urlparse(url).scheme == "":
            # Try HTTPS first
            url = "https://" + url
        get_all_website_links_rec(url, reviewd_urls, limit)

    # Use a ThreadPoolExecutor to run the helper function in parallel.
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_url = {executor.submit(fetch_url, url): url for url in urls}

        for future in concurrent.futures.as_completed(future_to_url):
            future.result()

    columns_to_ignore = ['urls']
    if html is False:
        columns_to_ignore += ['html_content']
    df = dict_to_dataframe(reviewd_urls, columns_to_ignore=columns_to_ignore, index_name='url')

    if not df.empty and df[df.error.isna()].empty:
        # no real data - rise exception from first row
        raise Exception(str(df.iloc[0].error))
    return df


# this can parse the native query    
def parse_urls_limit(input_str):
    # Split the input string into 'url', 'limit' or 'html' parts
    items = re.split(r',', input_str)

    # Initialize list for urls, limit and html
    urls = []
    limit = None
    html = False

    for item in items:
        item = item.strip()  # Remove leading/trailing whitespace

        # Check if item is a 'limit' or 'html' setting
        if item.lower().startswith('limit'):
            limit_match = re.search(r'\d+', item)
            if limit_match:
                limit = int(limit_match.group())  # Extract the number
        elif item.lower().startswith('html'):
            html_match = re.search(r'(true|false)', item, re.I)
            if html_match:
                html = html_match.group().lower() == 'true'  # Check if the value is 'true'
        else:
            urls.append(item)  # Add the item to the url list

    return {"urls": urls, "limit": limit, "html": html}


# run a query that goes and crawls urls
# format url, url, ..., limit=n
# you can pass one of many urls, limit is optional
def get_df_from_query_str(query_str):
    args = parse_urls_limit(query_str)
    df = get_all_websites(args['urls'], args['limit'], args['html'])
    return df


# this flips a dictionary of dictionaries into a dataframe so we can use it in mindsdb
def dict_to_dataframe(dict_of_dicts, columns_to_ignore=None, index_name=None):
    # Convert dictionary of dictionaries into DataFrame
    df = pd.DataFrame.from_dict(dict_of_dicts, orient='index')

    # If columns_to_ignore is provided, drop these columns
    if columns_to_ignore:
        df = df.drop(columns_to_ignore, axis=1, errors='ignore')

    # If index_name is provided, rename the index
    if index_name:
        df.index.name = index_name

    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    response = requests.get('https://www.goldmansachs.com/media-relations/press-releases/current/pdfs/2023-q2-results.pdf')
    print(pdf_to_markdown(response))

