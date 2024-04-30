#!/usr/bin/env python
try:
    # For Python 3.0 and later
    from urllib.request import urlopen
except ImportError:
    # Fall back to Python 2's urllib2
    from urllib2 import urlopen

import certifi
import json
from urllib.parse import urlencode
import requests
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

base_url = "https://financialmodelingprep.com/api/v3/historical-price-full/"
symbol = "AAPL"
api_key = "GJvlw9YgVm5J4KIxdP1VPkvWzt747Q6j"
params = {
    "apikey": api_key,
    "from": "2023-10-10",
    "to": "2023-12-10",
    "serietype": "line"
}

url = f"{base_url}{symbol}?{urlencode(params)}"
r = requests.get('https://financialmodelingprep.com/api/v3/historical-price-full/AAPL?apikey=GJvlw9YgVm5J4KIxdP1VPkvWzt747Q6j&from=2023-10-10&to=2023-12-10&serietype=line')

print(r.content)