# OpenBB Handler

> The OpenBB platform (where this is built on) is not yet official released, so this documentation will need to be updated over the next couple of weeks.

[OpenBB](https://openbb.co) is a leading open source investment research software platform that provides access to high-quality financial market data. Its mission is to make investment research effective, powerful, and accessible to everyone.

This handler integrates with the [OpenBB Platform](https://my.openbb.co/app/sdk), and provides access to 500+ financial data endpoints from over 90+ different data providers. The open source platform can be found [on GitHub](https://github.com/OpenBB-finance/OpenBBTerminal).

The data available can be found [here](https://docs.openbb.co/sdk/reference).

## Pre-requisites

In order to be able to access OpenBB, you will need to sign-up to [OpenBB Hub](https://my.openbb.co).

Once you are in, you'll want to go to the [Platform API Keys](https://my.openbb.co/app/sdk/api-keys) which allows you to set the API keys from the data providers you are interested in getting data from.

After that you'll need to get your Personal Access Token (PAT) in [here](https://my.openbb.co/app/sdk/pat). The PAT is what permits OpenBB to know what API keys to use once you hit endpoint from the data providers of interest.

## Connect to the OpenBB Platform

We start by creating a database to connect to the OpenBB Platform.

```sql
CREATE DATABASE obb_db
WITH ENGINE = "openbb",
PARAMETERS = {
    "PAT": "YOUR PERSONAL ACCESS TOKEN FROM OPENBB HUB"
    };
```

## Select Data

In order to support all data available through OpenBB's ecosystem ([reference](https://docs.openbb.co/sdk/reference)) we have a single DB called `obb_db.openbb_fetcher` and then **ALWAYS** need to provide the `cmd` argument which selects the data endpoint the user is interested in.

Then, based on the request sent by the user, you may need to specify other parameters, which is done through `AND` operator. 

These additional parameters need to have the exact same naming as in [OpenBB documentation](https://docs.openbb.co/sdk/reference). In addition, they need to be surrounded by double quotes AND internally they need to be represented by the same convention that the OpenBB API would be expecting.

## Examples

### **Example 1**: obb.crypto.price.historical

Reference: [https://docs.openbb.co/platform/reference/crypto/price/historical](https://docs.openbb.co/platform/reference/crypto/price/historical)

>>Note: Make Sure to add API Key corresponding to provider to [OpenBB Dashboard](https://my.openbb.co/app/platform/credentials) 

MindsDB will provide an abstraction in full SQL for commands so that they look like virtual tables: 

```sql
SELECT *
FROM obb_db.crypto_price_historical
WHERE  symbol = 'BCTUSD'
    AND start_date = '2023-09-01'
    AND  provider='fmp';
```

You can also call the command like this:

```sql
SELECT *
FROM obb_db.openbb_fetcher 
WHERE cmd = "obb.crypto.price.historical"
    AND symbol = "'BCTUSD'"
    AND start_date = "'2023-09-01'"
    AND provider = "'fmp'";
```

is converted into:

```python
obb.crypto.price.historical(symbol = 'BCTUSD', start_date = '2023-09-01', provider = 'fmp')
```
<img width="1115" alt="OpenBB Results" src="https://github.com/user-attachments/assets/f1bdc7e5-f511-46aa-896b-3465f494dbf8">


### **Example 2**: obb.economy.cpi

Reference: [https://docs.openbb.co/sdk/reference/economy/cpi](https://docs.openbb.co/sdk/reference/economy/cpi)

```sql
SELECT *
FROM obb_db.openbb_fetcher
WHERE cmd = "obb.economy.cpi"
    AND country = "'india,israel'";
```

is converted into:

```python
obb.economy.cpi(country = 'india,israel')
```

<img width="881" alt="OpenBB Results" src="https://github.com/user-attachments/assets/56ec368f-6ae0-4940-b328-588afaa37977">

## Enhance data access through OpenBB extensions

When install the OpenBB platform, you'll get by default access to core data that OpenBB official supports. However, there's a broad range of additional data that can be utilized - for that all you need to do is to install OpenBB extensions and the python library will automatically recognize where they belong.

Thus, you can go to the [requirements.txt](requirements.txt) file and add the extension of your choice.

E.g. if you want to access data from yfinance, all you need to do is add `openbb-yfinance` to that file, and now you can re-run the _Example 1_ with:

```sql

SELECT * 
FROM obb_db.equity_price_quote
where  
    symbol = 'RELIANCE.NS' 
    AND provider = "yfinance";

```

which will allow you to access financial data from stocks from India (which wasn't possible before).
