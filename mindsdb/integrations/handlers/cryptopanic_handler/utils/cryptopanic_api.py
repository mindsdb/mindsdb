import requests
import pandas as pd
import time

# Global variable to handle API rate limiting
API_RATE_LIMIT_DELAY = .2  # All API methods are rate limited per IP at 5req/sec.

# Function to construct API URL with various parameters
def construct_api_url(api_token=None, filters=None, currencies=None, kind=None, regions=None, page=None, following=None):
    """
    Constructs the URL for the CryptoPanic API based on specified parameters.

    Args:
        filters (str): Type of filter to apply (e.g., 'hot', 'bullish', 'important').
        currencies (str): Comma-separated list of currencies to filter by.
        kind (str): Type of content (e.g., 'news', 'media').
        regions (str): Language regions (e.g., 'en', 'de', 'es').
        page (int): Page number to retrieve.

    Returns:
        str: Constructed URL for API request.
    """
    if api_token:
        url = 'https://cryptopanic.com/api/v1/posts/?auth_token={}'.format(api_token)

    if currencies is not None:
        if len(currencies.split(',')) <= 50:
            url += "&currencies={}".format(currencies)
        else:
            print("Warning: Max Currencies is 50")
            return

    if kind is not None and kind in ['news', 'media']:
        url += "&kind={}".format(kind)
    
    if following is not None:
        url += "&following={}".format(following)

    if filter is not None:
        url += "&filter={}".format(filter)

    if regions is not None:
        url += "&regions={}".format(regions)

    if page is not None:
        url += "&page={}".format(page)

    return url

# Function to retrieve JSON data for a single page from the API
def get_single_page_json(url=None, api_token=None):
    """
    Retrieves JSON data for a single page from the CryptoPanic API.

    Args:
        url (str): API URL for the request.

    Returns:
        dict: JSON data returned by the API.
    """
    time.sleep(API_RATE_LIMIT_DELAY)  # Rate limiting to avoid exceeding 5 requests per second
    if not url:
        url = "https://cryptopanic.com/api/v1/posts/?auth_token={}".format(api_token)
    response = requests.get(url)
    data = response.json()
    data = flatten_json(data)
    return data

# Function to retrieve a list of JSON data for multiple pages from the API
def get_multiple_pages_json(lookback, url):
    """
    Retrieves a history of pages starting from page 1 up to the specified lookback.

    Args:
        lookback (int): Number of pages to retrieve.
        url (str): API URL for the request.

    Returns:
        list: List of JSON data for each page.
    """
    pages_list_json = [get_single_page_json(url)]

    for _ in range(lookback):
        pages_list_json.append(get_single_page_json(pages_list_json[-1]["next"]))

    return pages_list_json

# Function to convert JSON data to pandas DataFrame
def json_to_dataframe(data):
    """
    Converts JSON data to a pandas DataFrame.

    Args:
        data (dict): JSON data to convert.

    Returns:
        pandas.DataFrame: DataFrame containing the JSON data.
    """
    df = pd.DataFrame(data)
    try:
        df['created_at'] = pd.to_datetime(df.created_at)
    except Exception as e:
        pass

    return df

# Function to concatenate JSON data from multiple pages into a single DataFrame
def concatenate_pages_to_dataframe(pages_list):
    """
    Concatenates JSON data from multiple pages into a single DataFrame.

    Args:
        pages_list (list): List of JSON data for each page.

    Returns:
        pandas.DataFrame: DataFrame containing concatenated JSON data.
    """
    frames = [json_to_dataframe(page) for page in pages_list]
    return pd.concat(frames, ignore_index=True)

def flatten_json(data, prefix=""):
  """
  Flattens a nested JSON object into a dictionary with key paths as keys.

  Args:
      data: The nested JSON object to flatten.
      prefix: An optional prefix to prepend to key names (default: "").

  Returns:
      A dictionary with flattened key-value pairs.
  """
  flattened_data = {}
  if isinstance(data, dict):
    for key, value in data.items():
      new_prefix = prefix + key + "_" if prefix else key + "_"
      flattened_data.update(flatten_json(value, new_prefix))
  elif isinstance(data, list):
    for i, item in enumerate(data):
      new_prefix = prefix + str(i) + "_" if prefix else str(i) + "_"
      flattened_data.update(flatten_json(item, new_prefix))
  else:
    flattened_data[prefix[:-1]] = data  # Remove the trailing underscore from prefix
  return flattened_data


# Function to call the cryptographic API and return DataFrame
def call_cryptographic_api(api_token=None, filter=None, currencies=None, kind=None, regions=None, page=None, lookback=None):
    """
    Calls the cryptographic API and returns data in a DataFrame.

    Args:
        filters (str): Type of filter to apply (e.g., 'hot', 'bullish', 'important').
        currencies (str): Comma-separated list of currencies to filter by.
        kind (str): Type of content (e.g., 'news', 'media').
        regions (str): Language regions (e.g., 'en', 'de', 'es').
        page (int): Page number to retrieve.
        lookback (int): Number of pages to retrieve.

    Returns:
        pandas.DataFrame: DataFrame containing the retrieved data.
    """
    api_url = construct_api_url(api_token=api_token, filter=filter, currencies=currencies, kind=kind,
                                regions=regions, page=page)
    
    if lookback:
        pages_list = get_multiple_pages_json(lookback, api_url)
        return concatenate_pages_to_dataframe(pages_list)
    else:
        json_data = get_single_page_json(api_token=api_token, url=api_url)
        return json_to_dataframe(json_data)
