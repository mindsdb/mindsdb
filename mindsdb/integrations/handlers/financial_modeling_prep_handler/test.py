#from financial_modeling_handler import FinancialModelingHandler
import json
import pandas as pd

#!/usr/bin/env python
# try:
#     # For Python 3.0 and later
from urllib.request import urlopen
# except ImportError:
#     # Fall back to Python 2's urllib2
#from urllib2 import urlopen
import certifi
# import json
from urllib.parse import urlencode
# import requests
# def get_jsonparsed_data(url):
#     response = urlopen(url, cafile=certifi.where())
#     data = response.read().decode("utf-8")
#     return json.loads(data)

# url = ("https://financialmodelingprep.com/api/v3/historical-price-full/AAPL?apikey=GJvlw9YgVm5J4KIxdP1VPkvWzt747Q6j")

# response = get_jsonparsed_data(url)
# #take out symbol in dict
# data = json.loads(response) #parses into python dict

# historical_data = data["historical"][:3] #first 3 elements

# print(historical_data)

# base_url = "https://financialmodelingprep.com/api/v3/historical-price-full/"
# symbol = "AAPL"
# api_key = "GJvlw9YgVm5J4KIxdP1VPkvWzt747Q6j"
# params = {
#     "apikey": api_key,
#     "from": "2023-10-10",
#     "to": "2023-12-10",
#     "serietype": "line"
# }

# url = f"{base_url}{symbol}?{urlencode(params)}"
# r = requests.get('https://financialmodelingprep.com/api/v3/historical-price-full/AAPL?apikey=GJvlw9YgVm5J4KIxdP1VPkvWzt747Q6j&from=2023-10-10&to=2023-12-10&serietype=line')

import requests

# print(r.content)
def get_jsonparsed_data(url):
    response = urlopen(url, cafile=certifi.where())
    data = response.read().decode("utf-8")
    return json.loads(data)

def get_daily_chart(params: dict = None) -> pd.DataFrame:  
    url = ("https://financialmodelingprep.com/api/v3/historical-price-full/AAPL?apikey=GJvlw9YgVm5J4KIxdP1VPkvWzt747Q6j")
    base_url = "https://financialmodelingprep.com/api/v3/historical-price-full/"
    # symbol = "AAPL"
    # api_key = "GJvlw9YgVm5J4KIxdP1VPkvWzt747Q6j"
    # params = {
    #     "apikey": api_key,
    #     "from": "2023-10-10",
    #     "to": "2023-12-10",
    #     "serietype": "line"
    # }
    print(params)
    #symbol = params['symbol']
    from_date = params['from']
    to_date = params['to']

    # url = f"{base_url}{symbol}?{urlencode(params)}"
    # print(url)
    # if 'symbol' not in params:
    #     raise ValueError('Missing "symbol" param')
    r = requests.get(url, params)
    #print(r)
    #response = get_jsonparsed_data(url)
    #take out symbol in dict
    # data = json.loads(r) #parses into python dict

    # historical_data = data["historical"][:3] #first 3 elements

    return r
    
def main():
    # Define params
    params = {
        "from": "2023-10-10",
        "to": "2023-12-10",
        "serietype": "line"
    }
    print(params)
    # Call get_daily_chart method with params
    df = get_daily_chart(params)
    print(df)
    

if __name__ == "__main__":
    main()