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

### **Example 1**: obb.stocks.load




Reference: [https://docs.openbb.co/sdk/reference/stocks/load](https://docs.openbb.co/sdk/reference/stocks/load)

MindsDB will provide an abstraction in full SQL for commands so that they look like virtual tables: 

```sql
select * from obb_db.stock_historical 
where  
    symbol = 'MSFT' 
    AND provider = "polygon" 
    AND interval = "1m" 
    AND date > "2023-09-01" 
    ORDER BY date desc;
```

You can also call the command like this:

```sql
SELECT *
FROM obb_db.openbb_fetcher
WHERE cmd = "obb.stocks.load"
    AND symbol = "'MSFT'"
    AND start_date = "'2023-09-01'"
    AND provider = "'polygon'"
```

is converted into:

```python
obb.stocks.load(symbol = 'MSFT', start_date = '2023-09-01', provider = 'polygon')
```
<img width="1115" alt="Screenshot 2023-10-07 at 7 06 28 PM" src="https://github.com/DidierRLopes/mindsdb/assets/25267873/a8ee9c5b-01c9-443f-bc50-d4514f55d3b7">


### **Example 2**: obb.economy.cpi

Reference: [https://docs.openbb.co/sdk/reference/economy/cpi](https://docs.openbb.co/sdk/reference/economy/cpi)

```sql
FROM obb_datab9.openbb_fetcher
WHERE cmd = "obb.economy.cpi"
    AND countries = "['portugal','italy']"
```

is converted into:

```python
obb.economy.cpi(countries = ['portugal','italy'])
```

<img width="881" alt="Screenshot 2023-10-07 at 7 05 57 PM" src="https://github.com/DidierRLopes/mindsdb/assets/25267873/3465dcc5-02f5-44cf-8aab-e8868fcfcbe7">


### **Example 3**: obb.fixedincome.ycrv

Reference:

```sql
SELECT *
FROM obb_datab9.openbb_fetcher
WHERE cmd = "obb.fixedincome.ycrv"
```

is converted into: [https://docs.openbb.co/sdk/reference/fixedincome/ycrv](https://docs.openbb.co/sdk/reference/fixedincome/ycrv)

```python
obb.fixedincome.ycrv()
```

<img width="469" alt="Screenshot 2023-10-07 at 7 05 25 PM" src="https://github.com/DidierRLopes/mindsdb/assets/25267873/90538eda-15db-4a6f-816a-07ca0f35af52">


## Enhance data access through OpenBB extensions

When install the OpenBB platform, you'll get by default access to core data that OpenBB official supports. However, there's a broad range of additional data that can be utilized - for that all you need to do is to install OpenBB extensions and the python library will automatically recognize where they belong.

Thus, you can go to the [requirements.txt](requirements.txt) file and add the extension of your choice.

E.g. if you want to access data from yfinance, all you need to do is add `openbb-yfinance` to that file, and now you can re-run the _Example 1_ with:

```sql

select * from obb_db.stock_historical 
where  
    symbol = 'RELIANCE.NS' 
    AND provider = "yfinance" 
    AND interval = "1m" 
    AND date >= "2023-09-01" 
    ORDER BY date desc;


```

which will allow you to access financial data from stocks from India (which wasn't possible before).
