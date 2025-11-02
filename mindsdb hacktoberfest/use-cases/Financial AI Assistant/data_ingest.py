import pandas as pd
import json

def load_nifty50_symbols(csv_path='ind_nifty500list_stocks.csv'):
    """Load stock symbols from CSV and return list of dicts with keys:
       'Company Name', 'Industry', 'Symbol'"""
    try:
        df = pd.read_csv(csv_path)
        records = df[['Company Name', 'Industry', 'Symbol']].to_dict('records')
        print(f"Loaded {len(records)} stocks from {csv_path}")
        return records
    except Exception as e:
        print(f"Error loading CSV {csv_path}: {e}")
        return []

def fetch_crawler_data(mindsdb_project, ticker, url_template='https://stocktwits.com/symbol/{ticker}.NSE'):
    """Fetch crawler data for a ticker from the MindsDB project.
       Returns text representation (string) and raw DataFrame-like result when available."""
    url = url_template.format(ticker=ticker)
    query = f"""
    SELECT *
    FROM my_web.crawler
    WHERE url = '{url}'
    AND crawl_depth = 1
    """
    try:
        print(f"Fetching crawler data for {ticker} ({url})...")
        result = mindsdb_project.query(query)
        df = result.fetch()
        if len(df) == 0:
            return None, f"No specific data available for {ticker} at {url}"
        # try return pandas-like df and text
        try:
            text = df.to_string()
        except Exception:
            text = json.dumps(df)
        return df, text
    except Exception as e:
        print(f"Error fetching crawler data for {ticker}: {e}")
        return None, f"Error fetching data for {ticker}: {e}"