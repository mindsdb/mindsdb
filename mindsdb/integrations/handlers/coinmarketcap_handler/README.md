# CoinMarketCap Handler

The CoinMarketCap handler is a data integration that allows MindsDB to connect to the [CoinMarketCap API](https://coinmarketcap.com/api/documentation/v1/), enabling users to access comprehensive cryptocurrency market data for machine learning and predictive analytics.

## About CoinMarketCap

CoinMarketCap is the world's most-referenced price-tracking website for cryptoassets in the rapidly growing cryptocurrency space. Their API provides access to:
- Real-time and historical cryptocurrency prices
- Market capitalization data
- Trading volume information
- Market metrics across thousands of cryptocurrencies
- Global market statistics
- Exchange information

## Implementation

This handler was implemented using the CoinMarketCap Professional API, which provides robust and reliable access to cryptocurrency market data.

The CoinMarketCap handler is initialized with the `coinmarketcap` engine.

## Connection Arguments

The required arguments to establish a connection are:

- `api_key`: Your CoinMarketCap API key (required)
- `sandbox`: Boolean flag to use sandbox environment for testing (optional, default: False)

## Usage

To connect MindsDB to CoinMarketCap, use the following SQL command:

```sql
CREATE DATABASE coinmarketcap_datasource
WITH ENGINE = 'coinmarketcap',
PARAMETERS = {
  "api_key": "your_coinmarketcap_api_key_here"
};
```

For sandbox/testing environment:

```sql
CREATE DATABASE coinmarketcap_test
WITH ENGINE = 'coinmarketcap',
PARAMETERS = {
  "api_key": "your_sandbox_api_key_here",
  "sandbox": true
};
```

## Supported Tables

The CoinMarketCap handler provides access to several virtual tables that correspond to different API endpoints:

### 1. `cryptocurrency_listings` - Latest Cryptocurrency Listings

Get the latest market data for cryptocurrencies.

**Available columns:**
- `id`: CoinMarketCap cryptocurrency ID
- `name`: Cryptocurrency name
- `symbol`: Cryptocurrency symbol
- `slug`: URL-friendly cryptocurrency name
- `cmc_rank`: CoinMarketCap ranking
- `num_market_pairs`: Number of market pairs
- `circulating_supply`: Circulating supply
- `total_supply`: Total supply
- `max_supply`: Maximum supply
- `infinite_supply`: Whether supply is infinite
- `last_updated`: Last update timestamp
- `date_added`: Date added to CoinMarketCap
- `tags`: Associated tags
- `platform`: Platform information
- `self_reported_circulating_supply`: Self-reported circulating supply
- `self_reported_market_cap`: Self-reported market cap
- `tvl_ratio`: TVL ratio
- `price`: Current price (in specified currency)
- `volume_24h`: 24-hour trading volume
- `volume_change_24h`: 24-hour volume change percentage
- `percent_change_1h`: 1-hour price change percentage
- `percent_change_24h`: 24-hour price change percentage
- `percent_change_7d`: 7-day price change percentage
- `percent_change_30d`: 30-day price change percentage
- `percent_change_60d`: 60-day price change percentage
- `percent_change_90d`: 90-day price change percentage
- `market_cap`: Market capitalization
- `market_cap_dominance`: Market cap dominance
- `fully_diluted_market_cap`: Fully diluted market cap

### 2. `cryptocurrency_quotes` - Cryptocurrency Quotes

Get the latest market quote for specific cryptocurrencies.

**Available columns:**
- Same as `cryptocurrency_listings` but for specific cryptocurrency IDs or symbols

### 3. `cryptocurrency_historical` - Historical OHLCV Data

Get historical Open, High, Low, Close, Volume data for cryptocurrencies.

**Available columns:**
- `time_open`: Opening time
- `time_close`: Closing time
- `time_high`: Time of highest price
- `time_low`: Time of lowest price
- `open`: Opening price
- `high`: Highest price
- `low`: Lowest price
- `close`: Closing price
- `volume`: Trading volume
- `market_cap`: Market capitalization
- `timestamp`: Data timestamp

### 4. `cryptocurrency_info` - Cryptocurrency Metadata

Get static metadata information for cryptocurrencies.

**Available columns:**
- `id`: CoinMarketCap ID
- `name`: Cryptocurrency name
- `symbol`: Cryptocurrency symbol
- `category`: Category
- `description`: Description
- `slug`: URL slug
- `logo`: Logo URL
- `subreddit`: Associated subreddit
- `notice`: Important notices
- `tags`: Associated tags
- `tag_names`: Tag names
- `tag_groups`: Tag groups
- `urls`: Associated URLs
- `platform`: Platform information
- `date_added`: Date added
- `twitter_username`: Twitter username
- `is_hidden`: Hidden status
- `date_launched`: Launch date
- `contract_address`: Contract addresses
- `self_reported_circulating_supply`: Self-reported supply
- `self_reported_tags`: Self-reported tags

### 5. `global_metrics` - Global Market Metrics

Get global cryptocurrency market metrics.

**Available columns:**
- `active_cryptocurrencies`: Number of active cryptocurrencies
- `total_cryptocurrencies`: Total number of cryptocurrencies
- `active_market_pairs`: Number of active market pairs
- `active_exchanges`: Number of active exchanges
- `total_exchanges`: Total number of exchanges
- `eth_dominance`: Ethereum dominance percentage
- `btc_dominance`: Bitcoin dominance percentage
- `eth_dominance_yesterday`: Yesterday's Ethereum dominance
- `btc_dominance_yesterday`: Yesterday's Bitcoin dominance
- `eth_dominance_24h_percentage_change`: 24h Ethereum dominance change
- `btc_dominance_24h_percentage_change`: 24h Bitcoin dominance change
- `defi_volume_24h`: 24h DeFi volume
- `defi_volume_24h_reported`: 24h reported DeFi volume
- `defi_market_cap`: DeFi market cap
- `defi_24h_percentage_change`: 24h DeFi percentage change
- `stablecoin_volume_24h`: 24h stablecoin volume
- `stablecoin_volume_24h_reported`: 24h reported stablecoin volume
- `stablecoin_market_cap`: Stablecoin market cap
- `stablecoin_24h_percentage_change`: 24h stablecoin change
- `derivatives_volume_24h`: 24h derivatives volume
- `derivatives_volume_24h_reported`: 24h reported derivatives volume
- `derivatives_24h_percentage_change`: 24h derivatives change
- `total_market_cap`: Total market capitalization
- `total_volume_24h`: Total 24h volume
- `total_volume_24h_reported`: Total 24h reported volume
- `altcoin_volume_24h`: 24h altcoin volume
- `altcoin_volume_24h_reported`: 24h reported altcoin volume
- `altcoin_market_cap`: Altcoin market cap
- `last_updated`: Last update timestamp

### 6. `exchange_listings` - Exchange Listings

Get information about cryptocurrency exchanges.

**Available columns:**
- `id`: Exchange ID
- `name`: Exchange name
- `slug`: URL slug
- `is_active`: Active status
- `is_defunct`: Defunct status
- `status`: Current status
- `first_historical_data`: First historical data date
- `last_historical_data`: Last historical data date
- `spot_volume_usd`: Spot volume in USD
- `spot_volume_24h_percentage_change`: 24h spot volume change
- `derivatives_volume_usd`: Derivatives volume in USD
- `derivatives_volume_24h_percentage_change`: 24h derivatives volume change
- `total_volume_24h`: Total 24h volume
- `total_volume_24h_percentage_change`: 24h total volume change
- `num_market_pairs`: Number of market pairs
- `traffic_score`: Traffic score
- `rank`: Exchange rank
- `exchange_score`: Exchange score
- `liquidity_score`: Liquidity score
- `last_updated`: Last update timestamp

## Query Parameters

Many tables support additional parameters to filter and customize the data:

### Common Parameters:
- `convert`: Convert prices to specific currency (e.g., 'USD', 'EUR', 'BTC')
- `limit`: Limit number of results returned
- `start`: Starting offset for pagination
- `sort`: Sort field for listings
- `sort_dir`: Sort direction ('asc' or 'desc')

### Historical Data Parameters:
- `time_start`: Start date for historical data (ISO 8601 format)
- `time_end`: End date for historical data (ISO 8601 format)
- `time_period`: Time period ('daily', 'weekly', 'monthly')
- `interval`: Data interval ('1h', '2h', '3h', '4h', '6h', '8h', '12h', '1d', '2d', '3d', '7d', '14d', '15d', '30d', '60d', '90d', '365d')

### Specific Filters:
- `symbol`: Filter by cryptocurrency symbols (comma-separated)
- `id`: Filter by CoinMarketCap IDs (comma-separated)
- `cryptocurrency_type`: Filter by type ('all', 'coins', 'tokens')
- `market_type`: Filter by market type ('all', 'spot', 'derivatives', 'otc')

## Example Queries

### 1. Get Top 10 Cryptocurrencies by Market Cap

```sql
SELECT name, symbol, price, market_cap, percent_change_24h
FROM coinmarketcap_datasource.cryptocurrency_listings
WHERE convert = 'USD'
ORDER BY market_cap DESC
LIMIT 10;
```

### 2. Get Bitcoin Price and Volume Data

```sql
SELECT name, symbol, price, volume_24h, percent_change_24h, market_cap
FROM coinmarketcap_datasource.cryptocurrency_quotes
WHERE symbol = 'BTC' AND convert = 'USD';
```

### 3. Get Historical Bitcoin Data for the Last Month

```sql
SELECT timestamp, open, high, low, close, volume
FROM coinmarketcap_datasource.cryptocurrency_historical
WHERE symbol = 'BTC'
  AND convert = 'USD'
  AND time_period = 'daily'
  AND time_start = '2024-01-01'
  AND time_end = '2024-01-31';
```

### 4. Get Global Market Metrics

```sql
SELECT total_market_cap, total_volume_24h, btc_dominance, eth_dominance, active_cryptocurrencies
FROM coinmarketcap_datasource.global_metrics
WHERE convert = 'USD';
```

### 5. Get Information About Top Exchanges

```sql
SELECT name, spot_volume_usd, rank, exchange_score, num_market_pairs
FROM coinmarketcap_datasource.exchange_listings
ORDER BY spot_volume_usd DESC
LIMIT 10;
```

### 6. Get Detailed Cryptocurrency Information

```sql
SELECT name, symbol, description, category, date_added, twitter_username
FROM coinmarketcap_datasource.cryptocurrency_info
WHERE symbol IN ('BTC', 'ETH', 'ADA', 'DOT');
```

## Machine Learning Examples

### 1. Create a Price Prediction Model

```sql
-- Create a model to predict Bitcoin price
CREATE MODEL bitcoin_price_predictor
PREDICT price
USING
  engine = 'lightgbm',
  target_column = 'price'
FROM coinmarketcap_datasource.cryptocurrency_historical
WHERE symbol = 'BTC' AND convert = 'USD'
  AND time_start = '2023-01-01'
  AND time_end = '2024-01-01';
```

### 2. Predict Market Trends

```sql
-- Create a model to predict 24h price changes
CREATE MODEL crypto_trend_predictor
PREDICT percent_change_24h
USING
  engine = 'neural',
  target_column = 'percent_change_24h'
FROM coinmarketcap_datasource.cryptocurrency_listings
WHERE convert = 'USD' AND market_cap > 1000000000;
```

### 3. Make Predictions

```sql
-- Get Bitcoin price predictions
SELECT m.price as predicted_price, l.price as current_price
FROM bitcoin_price_predictor as m
JOIN coinmarketcap_datasource.cryptocurrency_quotes as l
WHERE l.symbol = 'BTC' AND l.convert = 'USD';
```

## API Rate Limits

Please be aware of CoinMarketCap API rate limits:
- **Basic Plan (Free)**: 333 calls/day, 10,000 calls/month
- **Hobbyist Plan**: 3,333 calls/day, 100,000 calls/month
- **Startup Plan**: 10,000 calls/day, 300,000 calls/month
- **Standard Plan**: 33,333 calls/day, 1,000,000 calls/month
- **Professional Plan**: 100,000 calls/day, 3,000,000 calls/month
- **Enterprise Plan**: Custom limits

## Error Handling

The handler includes comprehensive error handling for:
- Invalid API keys
- Rate limit exceeded
- Network connectivity issues
- Invalid parameters
- API endpoint errors

Common error messages:
- `Authentication failed`: Invalid API key
- `Rate limit exceeded`: Too many requests
- `Invalid symbol`: Cryptocurrency symbol not found
- `Invalid date format`: Incorrect date format for historical data

## Getting Your API Key

1. Visit [CoinMarketCap API](https://pro.coinmarketcap.com/api/)
2. Sign up for a free account
3. Navigate to the API section in your dashboard
4. Copy your API key
5. Use the API key in your MindsDB connection

## Troubleshooting

### Connection Issues
- Verify your API key is correct and active
- Check your internet connection
- Ensure you haven't exceeded rate limits

### Data Issues
- Verify cryptocurrency symbols are correct (use uppercase)
- Check date formats (ISO 8601: YYYY-MM-DD)
- Ensure requested date ranges are valid

### Performance Optimization
- Use specific symbol filters to reduce data transfer
- Implement pagination for large datasets
- Cache frequently accessed data
- Monitor your API usage to avoid rate limits

## Support

For issues specific to the CoinMarketCap handler:
1. Check the troubleshooting section above
2. Verify your API key and connection parameters
3. Review CoinMarketCap API documentation
4. Check MindsDB community forums

For general MindsDB support:
- [MindsDB Documentation](https://docs.mindsdb.com/)
- [Community Slack](https://mindsdb.com/joincommunity)
- [GitHub Issues](https://github.com/mindsdb/mindsdb/issues)

## Contributing

We welcome contributions to improve the CoinMarketCap handler! Please see the [MindsDB contribution guidelines](https://docs.mindsdb.com/contribute/) for more information.

## License

This handler is distributed under the same license as MindsDB.