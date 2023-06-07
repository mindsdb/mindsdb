import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import logging
import re
import traceback
import pandas as pd


import requests
import concurrent.futures
from urllib.parse import urlparse

def is_valid(url):
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)





# this bad boy gets all the crawling done in parallel
def parallel_get_all_website_links(urls):
    url_contents = {}
    # if len(urls) <= 2:
    #     for url in urls:
    #         url_contents[url] = get_all_website_links(url)
    #     return url_contents
    
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
    content_html = requests.get(url).content
    try:
        soup = BeautifulSoup(content_html, "html.parser")
    except Exception as e:
        logging.error("An exception occurred: %s", str(e))
        return {'urls':urls, 'html_content':'', 'text_content': '', 'error':str(e)}
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
        if domain_name !=parsed_href.netloc:
            continue
        
        href = href.rstrip('/')    
        urls.add(href)
    

    return {'urls':urls, 'html_content':content_html, 'text_content': content_text, 'error': None}


# this returns the soup object
def get_readable_text_from_soup(soup):


    # Remove script and style elements
    for script in soup(["script", "style"]):
        script.decompose()

    # Get text
    text = soup.get_text()

    # Remove leading and trailing spaces on each line
    lines = (line.strip() for line in text.splitlines())
    # Break multi-headlines into a line each
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    # Remove blank lines
    text = '\n'.join(chunk for chunk in chunks if chunk)

    return text



# this bad girl does the recursive crawling of the websites
def get_all_website_links_rec(url, reviewd_urls, limit = None):

    
    
    
    
    # if something happens getting the website links for this url then log the error
    try:
        reviewd_urls[url]=get_all_website_links(url)
    except Exception as e:
        error_message = traceback.format_exc()
        logging.error("An exception occurred: %s", str(e))
        reviewd_urls[url]= {'urls':[], 'html_content':'', 'text_content': '', 'error':str(error_message)}
    if limit is not None:
        if len(reviewd_urls) >= limit:
            return reviewd_urls
        
    
    so_far_rev = list(reviewd_urls.keys())
    to_rev_url_list = []

    # create a list of new urls to review that dont exist in the already reviewed ones
    for new_url in reviewd_urls[url]['urls']:
        
        # if this is already in the urls, then no need to go and crawl for it
        if new_url in reviewd_urls: 
            continue
        
        # if exceeds the limit continue
        if limit is not None:
            if len(reviewd_urls) + len(to_rev_url_list)  >= limit:
                continue

        to_rev_url_list += [new_url]
    
    new_revised_urls = {}
    

    # if there is somehting to fetch, go fetch
    if len(to_rev_url_list) > 0 :
        new_revised_urls = parallel_get_all_website_links(to_rev_url_list)
    
    reviewd_urls.update(new_revised_urls)
    
    #print("----\nprev:{p}".format(p=len(reviewd_urls)))
    
    #print("post:{p}".format(p=len(reviewd_urls)))

    for new_url in new_revised_urls:
        

        
        get_all_website_links_rec(new_url, reviewd_urls, limit)

    

    return reviewd_urls


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
        reviewd_urls_iter = {}
        get_all_website_links_rec(url, reviewd_urls_iter, limit)
        return reviewd_urls_iter

    # Use a ThreadPoolExecutor to run the helper function in parallel.
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_url = {executor.submit(fetch_url, url): url for url in urls}

        for future in concurrent.futures.as_completed(future_to_url):
            reviewd_urls_iter = future.result()
            reviewd_urls.update(reviewd_urls_iter)

    columns_to_ignore=['urls']
    if html == False:
        columns_to_ignore += ['html_content']
    df = dict_to_dataframe(reviewd_urls, columns_to_ignore=columns_to_ignore, index_name='url')

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
    df = get_df_from_query_str('docs.mindsdb.com, docs.airbyte.com, limit=4')
    print(df)






   




