#from financial_modeling_handler import FinancialModelingHandler
import json
import pandas as pd
import certifi
# import json
import requests

# print(r.content)
def get_jsonparsed_data(url):
    response = urlopen(url, cafile=certifi.where())
    data = response.read().decode("utf-8")
    return json.loads(data)

def get_daily_chart(params: dict = None) -> pd.DataFrame:  
    url = ("https://financialmodelingprep.com/api/v3/historical-price-full/AAPL?apikey=GJvlw9YgVm5J4KIxdP1VPkvWzt747Q6j")
    base_url = "https://financialmodelingprep.com/api/v3/historical-price-full/"
    # api_key = "GJvlw9YgVm5J4KIxdP1VPkvWzt747Q6j"

    if 'symbol' not in params:
        raise ValueError('Missing "symbol" param')
    symbol = params['symbol']
    params.pop('symbol')
    temp_url = f"{base_url}{symbol}"
    print(url)
    # print(url)
    r = requests.get(url, params)
    data = r.json()
    #historical_data = data[:3] #first 3 elements
    print(data)
    return data
    
def main():
    params = {
        "symbol": "AAPL",
        "from": "2023-10-10",
        "to": "2023-12-10",
        "serietype": "line"
    }
    print(params)
    df = get_daily_chart(params)
    print(df)
    

if __name__ == "__main__":
    main()