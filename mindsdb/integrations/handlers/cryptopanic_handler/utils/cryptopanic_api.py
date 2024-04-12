import requests
import pandas as pd
import time

# Global variable to handle API rate limiting
API_RATE_LIMIT_DELAY = .2  # All API methods are rate limited per IP at 5req/sec.

# Function to retrieve JSON data for a single page from the API
def get_single_page_json(url):
    """
    Retrieves JSON data for a single page from the CryptoPanic API.

    Args:
        url (str): API URL for the request.

    Returns:
        dict: JSON data returned by the API.
    """
    time.sleep(API_RATE_LIMIT_DELAY)  # Rate limiting to avoid exceeding 5 requests per second
    response = requests.get(url)
    data = response.json()
    return data.get('results', []),  data.get('next')

# Function to retrieve JSON data for multiple pages from the API
def get_multiple_pages_json(url, num_pages=None):
    """
    Retrieves JSON data for multiple pages from the CryptoPanic API.

    Args:
        url (str): API URL for the request.
        num_pages (int): Number of pages to retrieve. If None, retrieves all pages.

    Returns:
        list: List of JSON data for each page.
    """
    pages_list_json = []
    while url and (num_pages is None or len(pages_list_json) < num_pages*20):
        page_data, url = get_single_page_json(url)
        if not page_data:
            break
        pages_list_json.extend(page_data)
    return pages_list_json

def format_data(data):
    for entry in data:
        entry['region']=entry['source']['region']
        entry['source']=entry['source']['title']
        if 'currencies' in entry:
            entry['currencies']=[currency['title'] for currency in entry['currencies'] ]

        entry['votes']=max(entry['votes'], key=entry['votes'].get)
    
        if 'metadata' in entry:
            del entry['metadata']


# Function to call the CryptoPanic API and return DataFrame
def call_cryptopanic_api(api_token=None, filter=None, currencies=None, kind=None, regions=None, page=None, num_pages=None):
    """
    Calls the CryptoPanic API and returns data in a DataFrame.

    Args:
        api_token (str): API token for authentication.
        filters (str): Type of filter to apply (e.g., 'hot', 'bullish', 'important').
        currencies (str): Comma-separated list of currencies to filter by.
        kind (str): Type of content (e.g., 'news', 'media').
        regions (str): Language regions (e.g., 'en', 'de', 'es').
        page (int): Page number to retrieve.
        num_pages (int): Number of pages to retrieve.

    Returns:
        pandas.DataFrame: DataFrame containing the retrieved data.
    """
    api_url = 'https://cryptopanic.com/api/v1/posts/?auth_token={}'.format(api_token)
    if currencies is not None:
        api_url += "&currencies={}".format(currencies)
    if kind is not None:
        api_url += "&kind={}".format(kind)
    if regions is not None:
        api_url += "&regions={}".format(regions)
    if filter is not None:
        api_url += "&filter={}".format(filter)
    if page is not None:
        api_url += "&page={}".format(page)

    data = get_multiple_pages_json(api_url, num_pages)
    format_data(data)
    return pd.DataFrame(data)