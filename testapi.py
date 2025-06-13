import requests
import json

def test_coinmarketcap_api():
    """
    Test call to CoinMarketCap API to fetch active cryptocurrencies
    """
    
    # Your API key
    API_KEY = "9d88d55d-42a9-4cef-85a8-5b9eab8335b6"
    
    # API endpoint for latest cryptocurrency listings
    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
    
    # Headers with your API key
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': API_KEY,
    }
    
    # Parameters for the request
    parameters = {
        'start': '1',           # Start from rank 1
        'limit': '10',          # Get top 10 cryptocurrencies
        'convert': 'USD'        # Convert prices to USD
    }
    
    try:
        # Make the API request
        response = requests.get(url, headers=headers, params=parameters)
        
        # Check if request was successful
        if response.status_code == 200:
            data = response.json()
            
            print("✅ API Test Successful!")
            print(f"Status: {data['status']['error_code']} - {data['status']['error_message']}")
            print(f"Total cryptocurrencies available: {data['status']['total_count']}")
            print("\n🚀 Top 10 Cryptocurrencies:")
            print("-" * 80)
            
            # Display the cryptocurrency data
            for crypto in data['data']:
                name = crypto['name']
                symbol = crypto['symbol']
                price = crypto['quote']['USD']['price']
                market_cap = crypto['quote']['USD']['market_cap']
                change_24h = crypto['quote']['USD']['percent_change_24h']
                
                print(f"{crypto['cmc_rank']:2d}. {name} ({symbol})")
                print(f"    💰 Price: ${price:,.2f}")
                print(f"    📊 Market Cap: ${market_cap:,.0f}")
                print(f"    📈 24h Change: {change_24h:+.2f}%")
                print()
                
        else:
            print(f"❌ API request failed with status code: {response.status_code}")
            print(f"Response: {response.text}")
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Request error: {e}")
    except json.JSONDecodeError as e:
        print(f"❌ JSON decode error: {e}")
    except KeyError as e:
        print(f"❌ Missing expected data in response: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

# Alternative: More detailed API exploration
def explore_coinmarketcap_endpoints():
    """
    Explore different CoinMarketCap API endpoints
    """
    API_KEY = "9d88d55d-42a9-4cef-85a8-5b9eab8335b6"
    
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': API_KEY,
    }
    
    # Test different endpoints
    endpoints = {
        "Global Metrics": "https://pro-api.coinmarketcap.com/v1/global-metrics/quotes/latest",
        "Bitcoin Info": "https://pro-api.coinmarketcap.com/v1/cryptocurrency/info?symbol=BTC",
        "Top Gainers": "https://pro-api.coinmarketcap.com/v1/cryptocurrency/trending/gainers-losers"
    }
    
    for name, url in endpoints.items():
        print(f"\n🔍 Testing {name}:")
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                print(f"✅ {name} - Success")
                # Print first few keys of response
                data = response.json()
                if 'data' in data:
                    print(f"   Available data keys: {list(data['data'].keys()) if isinstance(data['data'], dict) else 'List of items'}")
            else:
                print(f"❌ {name} - Failed ({response.status_code})")
        except Exception as e:
            print(f"❌ {name} - Error: {e}")

if __name__ == "__main__":
    print("🪙 CoinMarketCap API Test")
    print("=" * 50)
    
    # Run the basic test
    test_coinmarketcap_api()
    
    # Uncomment to explore more endpoints
    # explore_coinmarketcap_endpoints()