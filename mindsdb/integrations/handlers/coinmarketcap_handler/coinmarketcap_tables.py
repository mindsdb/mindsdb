# CoinMarketCap Table Classes - Required Structure
# Each table class MUST implement these methods

import pandas as pd
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.libs.response import Response, RESPONSE_TYPE
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions

class ListingTable(APITable):  # Note: Should be "ListingTable" not "ListingsTable"
    """Table for cryptocurrency listings (/v1/cryptocurrency/listings/latest)"""
    
    def select(self, query) -> Response:
        """Handle SELECT queries - REQUIRED METHOD"""
        try:
            # Extract WHERE conditions
            conditions = extract_comparison_conditions(query.where)
            
            # Build API parameters
            params = {
                'start': 1,
                'limit': 100,
                'convert': 'USD'
            }
            
            # Process conditions (WHERE clauses)
            for op, arg1, arg2 in conditions:
                if arg1 == 'limit' and op == '=':
                    params['limit'] = min(int(arg2), 5000)  # CoinMarketCap max
                elif arg1 == 'start' and op == '=':
                    params['start'] = int(arg2)
                elif arg1 == 'convert' and op == '=':
                    params['convert'] = str(arg2)
            
            # Handle LIMIT clause
            if query.limit is not None:
                params['limit'] = min(query.limit.value, 5000)
            
            # Make API call through handler
            data = self.handler.call_coinmarketcap_api(
                '/v1/cryptocurrency/listings/latest',
                params
            )
            
            # Parse response into DataFrame
            df = self._parse_listings_data(data)
            
            return Response(RESPONSE_TYPE.TABLE, df)
            
        except Exception as e:
            return Response(RESPONSE_TYPE.ERROR, error_message=str(e))
    
    def get_columns(self) -> Response:
        """Return column definitions - REQUIRED METHOD"""
        columns = [
            {'name': 'id', 'type': 'integer', 'description': 'CoinMarketCap cryptocurrency ID'},
            {'name': 'name', 'type': 'string', 'description': 'Cryptocurrency name'},
            {'name': 'symbol', 'type': 'string', 'description': 'Cryptocurrency symbol'},
            {'name': 'slug', 'type': 'string', 'description': 'URL-friendly name'},
            {'name': 'cmc_rank', 'type': 'integer', 'description': 'CoinMarketCap ranking'},
            {'name': 'price', 'type': 'float', 'description': 'Current price in USD'},
            {'name': 'volume_24h', 'type': 'float', 'description': '24-hour trading volume'},
            {'name': 'market_cap', 'type': 'float', 'description': 'Market capitalization'},
            {'name': 'percent_change_1h', 'type': 'float', 'description': '1-hour price change %'},
            {'name': 'percent_change_24h', 'type': 'float', 'description': '24-hour price change %'},
            {'name': 'percent_change_7d', 'type': 'float', 'description': '7-day price change %'},
            {'name': 'circulating_supply', 'type': 'float', 'description': 'Circulating supply'},
            {'name': 'total_supply', 'type': 'float', 'description': 'Total supply'},
            {'name': 'max_supply', 'type': 'float', 'description': 'Maximum supply'},
            {'name': 'last_updated', 'type': 'datetime', 'description': 'Last update timestamp'}
        ]
        
        return Response(RESPONSE_TYPE.TABLE, pd.DataFrame(columns))
    
    def _parse_listings_data(self, data: dict) -> pd.DataFrame:
        """Convert CoinMarketCap API response to DataFrame"""
        rows = []
        
        for crypto in data.get('data', []):
            usd_quote = crypto.get('quote', {}).get('USD', {})
            
            row = {
                'id': crypto.get('id'),
                'name': crypto.get('name'),
                'symbol': crypto.get('symbol'),
                'slug': crypto.get('slug'),
                'cmc_rank': crypto.get('cmc_rank'),
                'price': usd_quote.get('price'),
                'volume_24h': usd_quote.get('volume_24h'),
                'market_cap': usd_quote.get('market_cap'),
                'percent_change_1h': usd_quote.get('percent_change_1h'),
                'percent_change_24h': usd_quote.get('percent_change_24h'),
                'percent_change_7d': usd_quote.get('percent_change_7d'),
                'circulating_supply': crypto.get('circulating_supply'),
                'total_supply': crypto.get('total_supply'),
                'max_supply': crypto.get('max_supply'),
                'last_updated': usd_quote.get('last_updated')
            }
            rows.append(row)
        
        return pd.DataFrame(rows)

class QuotesTable(APITable):
    """Table for specific cryptocurrency quotes (/v1/cryptocurrency/quotes/latest)"""
    
    def select(self, query) -> Response:
        """Handle SELECT queries for specific crypto quotes"""
        try:
            conditions = extract_comparison_conditions(query.where)
            
            params = {'convert': 'USD'}
            
            # Look for symbol or id conditions
            symbols = []
            ids = []
            
            for op, arg1, arg2 in conditions:
                if arg1 == 'symbol' and op == '=':
                    symbols.append(str(arg2))
                elif arg1 == 'id' and op == '=':
                    ids.append(int(arg2))
                elif arg1 == 'convert' and op == '=':
                    params['convert'] = str(arg2)
            
            # Set symbol or id parameter
            if symbols:
                params['symbol'] = ','.join(symbols)
            elif ids:
                params['id'] = ','.join(map(str, ids))
            else:
                # Default to top 10 if no specific symbols/ids
                params['symbol'] = 'BTC,ETH,BNB,ADA,XRP,SOL,DOGE,DOT,AVAX,MATIC'
            
            # Make API call
            data = self.handler.call_coinmarketcap_api(
                '/v1/cryptocurrency/quotes/latest',
                params
            )
            
            # Parse response
            df = self._parse_quotes_data(data)
            
            return Response(RESPONSE_TYPE.TABLE, df)
            
        except Exception as e:
            return Response(RESPONSE_TYPE.ERROR, error_message=str(e))
    
    def get_columns(self) -> Response:
        """Return column definitions for quotes table"""
        columns = [
            {'name': 'id', 'type': 'integer', 'description': 'Cryptocurrency ID'},
            {'name': 'name', 'type': 'string', 'description': 'Cryptocurrency name'},
            {'name': 'symbol', 'type': 'string', 'description': 'Cryptocurrency symbol'},
            {'name': 'price', 'type': 'float', 'description': 'Current price'},
            {'name': 'volume_24h', 'type': 'float', 'description': '24-hour volume'},
            {'name': 'market_cap', 'type': 'float', 'description': 'Market cap'},
            {'name': 'percent_change_1h', 'type': 'float', 'description': '1h change %'},
            {'name': 'percent_change_24h', 'type': 'float', 'description': '24h change %'},
            {'name': 'percent_change_7d', 'type': 'float', 'description': '7d change %'},
            {'name': 'last_updated', 'type': 'datetime', 'description': 'Last updated'}
        ]
        
        return Response(RESPONSE_TYPE.TABLE, pd.DataFrame(columns))
    
    def _parse_quotes_data(self, data: dict) -> pd.DataFrame:
        """Convert quotes API response to DataFrame"""
        rows = []
        
        # Handle both single crypto and multiple cryptos response format
        crypto_data = data.get('data', {})
        
        # If data is a dict (single crypto), convert to list
        if isinstance(crypto_data, dict):
            crypto_data = crypto_data.values()
        
        for crypto in crypto_data:
            usd_quote = crypto.get('quote', {}).get('USD', {})
            
            row = {
                'id': crypto.get('id'),
                'name': crypto.get('name'),
                'symbol': crypto.get('symbol'),
                'price': usd_quote.get('price'),
                'volume_24h': usd_quote.get('volume_24h'),
                'market_cap': usd_quote.get('market_cap'),
                'percent_change_1h': usd_quote.get('percent_change_1h'),
                'percent_change_24h': usd_quote.get('percent_change_24h'),
                'percent_change_7d': usd_quote.get('percent_change_7d'),
                'last_updated': usd_quote.get('last_updated')
            }
            rows.append(row)
        
        return pd.DataFrame(rows)

class InfoTable(APITable):
    """Table for cryptocurrency metadata (/v1/cryptocurrency/info)"""
    
    def select(self, query) -> Response:
        """Handle SELECT queries for crypto info"""
        try:
            conditions = extract_comparison_conditions(query.where)
            
            params = {}
            symbols = []
            ids = []
            
            for op, arg1, arg2 in conditions:
                if arg1 == 'symbol' and op == '=':
                    symbols.append(str(arg2))
                elif arg1 == 'id' and op == '=':
                    ids.append(int(arg2))
            
            if symbols:
                params['symbol'] = ','.join(symbols)
            elif ids:
                params['id'] = ','.join(map(str, ids))
            else:
                params['symbol'] = 'BTC,ETH'  # Default
            
            data = self.handler.call_coinmarketcap_api(
                '/v1/cryptocurrency/info',
                params
            )
            
            df = self._parse_info_data(data)
            
            return Response(RESPONSE_TYPE.TABLE, df)
            
        except Exception as e:
            return Response(RESPONSE_TYPE.ERROR, error_message=str(e))
    
    def get_columns(self) -> Response:
        """Return column definitions for info table"""
        columns = [
            {'name': 'id', 'type': 'integer', 'description': 'Cryptocurrency ID'},
            {'name': 'name', 'type': 'string', 'description': 'Cryptocurrency name'},
            {'name': 'symbol', 'type': 'string', 'description': 'Cryptocurrency symbol'},
            {'name': 'category', 'type': 'string', 'description': 'Category'},
            {'name': 'description', 'type': 'string', 'description': 'Description'},
            {'name': 'website', 'type': 'string', 'description': 'Official website'},
            {'name': 'technical_doc', 'type': 'string', 'description': 'Technical documentation'},
            {'name': 'twitter', 'type': 'string', 'description': 'Twitter username'},
            {'name': 'reddit', 'type': 'string', 'description': 'Reddit community'},
            {'name': 'date_added', 'type': 'datetime', 'description': 'Date added to CMC'}
        ]
        
        return Response(RESPONSE_TYPE.TABLE, pd.DataFrame(columns))
    
    def _parse_info_data(self, data: dict) -> pd.DataFrame:
        """Convert info API response to DataFrame"""
        rows = []
        
        crypto_data = data.get('data', {})
        
        for crypto in crypto_data.values():
            urls = crypto.get('urls', {})
            
            row = {
                'id': crypto.get('id'),
                'name': crypto.get('name'),
                'symbol': crypto.get('symbol'),
                'category': crypto.get('category'),
                'description': crypto.get('description'),
                'website': urls.get('website', [None])[0],
                'technical_doc': urls.get('technical_doc', [None])[0],
                'twitter': urls.get('twitter', [None])[0],
                'reddit': urls.get('reddit', [None])[0],
                'date_added': crypto.get('date_added')
            }
            rows.append(row)
        
        return pd.DataFrame(rows)

class GlobalMetricsTable(APITable):
    """Table for global market metrics (/v1/global-metrics/quotes/latest)"""
    
    def select(self, query) -> Response:
        """Handle SELECT queries for global metrics"""
        try:
            # Global metrics doesn't usually need parameters
            data = self.handler.call_coinmarketcap_api(
                '/v1/global-metrics/quotes/latest'
            )
            
            df = self._parse_global_data(data)
            
            return Response(RESPONSE_TYPE.TABLE, df)
            
        except Exception as e:
            return Response(RESPONSE_TYPE.ERROR, error_message=str(e))
    
    def get_columns(self) -> Response:
        """Return column definitions for global metrics table"""
        columns = [
            {'name': 'active_cryptocurrencies', 'type': 'integer', 'description': 'Number of active cryptocurrencies'},
            {'name': 'total_cryptocurrencies', 'type': 'integer', 'description': 'Total cryptocurrencies'},
            {'name': 'active_market_pairs', 'type': 'integer', 'description': 'Active market pairs'},
            {'name': 'active_exchanges', 'type': 'integer', 'description': 'Number of active exchanges'},
            {'name': 'total_market_cap_usd', 'type': 'float', 'description': 'Total market cap in USD'},
            {'name': 'total_volume_24h_usd', 'type': 'float', 'description': 'Total 24h volume in USD'},
            {'name': 'bitcoin_percentage_of_market_cap', 'type': 'float', 'description': 'Bitcoin dominance %'},
            {'name': 'altcoin_percentage_of_market_cap', 'type': 'float', 'description': 'Altcoin dominance %'},
            {'name': 'altcoin_market_cap_usd', 'type': 'float', 'description': 'Altcoin market cap USD'},
            {'name': 'last_updated', 'type': 'datetime', 'description': 'Last updated'}
        ]
        
        return Response(RESPONSE_TYPE.TABLE, pd.DataFrame(columns))
    
    def _parse_global_data(self, data: dict) -> pd.DataFrame:
        """Convert global metrics API response to DataFrame"""
        global_data = data.get('data', {})
        usd_quote = global_data.get('quote', {}).get('USD', {})
        
        row = {
            'active_cryptocurrencies': global_data.get('active_cryptocurrencies'),
            'total_cryptocurrencies': global_data.get('total_cryptocurrencies'),
            'active_market_pairs': global_data.get('active_market_pairs'),
            'active_exchanges': global_data.get('active_exchanges'),
            'total_market_cap_usd': usd_quote.get('total_market_cap'),
            'total_volume_24h_usd': usd_quote.get('total_volume_24h'),
            'bitcoin_percentage_of_market_cap': usd_quote.get('bitcoin_percentage_of_market_cap'),
            'altcoin_percentage_of_market_cap': usd_quote.get('altcoin_percentage_of_market_cap'),
            'altcoin_market_cap_usd': usd_quote.get('altcoin_market_cap'),
            'last_updated': usd_quote.get('last_updated')
        }
        
        return pd.DataFrame([row])  # Single row for global metrics