# ...existing code...
import os
import time
import mindsdb_sdk
import pandas as pd
import json
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# --- Configuration (from env with sane defaults) ---
MDBG_HOST = os.getenv('MDBG_HOST', 'http://127.0.0.1:47334')
MODEL_SCHEMA = os.getenv('MODEL_SCHEMA', 'mindsdb')
MODEL_NAME = os.getenv('MODEL_NAME', 'financial_analyzer')
FULL_MODEL_NAME = f"{MODEL_SCHEMA}.{MODEL_NAME}"

# --- PostgreSQL Configuration from env ---
POSTGRES_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'database': os.getenv('POSTGRES_DB', 'stock_analysis'),
    'user': os.getenv('POSTGRES_USER', 'bseetharaman'),
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres'),
    'port': int(os.getenv('POSTGRES_PORT', 5432))
}

# Connect to MindsDB server
server = mindsdb_sdk.connect(MDBG_HOST)
print(f"Connected to MindsDB server at {MDBG_HOST}")

mindsdb = server.get_project(MODEL_SCHEMA)

# Import ingestion helpers (kept local to avoid circular imports)
from data_ingest import load_nifty50_symbols, fetch_crawler_data

# --- Setup PostgreSQL Database ---
def setup_postgres_db():
    """Create PostgreSQL table for storing results"""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS analysis_results (
            id SERIAL PRIMARY KEY,
            ticker VARCHAR(20),
            analysis_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            summary TEXT,
            overall_sentiment VARCHAR(20),
            confidence_score DOUBLE PRECISION,
            credibility VARCHAR(20),
            bullish_reasons TEXT,
            bearish_reasons TEXT,
            speculative_posts TEXT,
            pe_ratio DOUBLE PRECISION,
            eps DOUBLE PRECISION,
            market_cap VARCHAR(50),
            dividend_yield VARCHAR(20),
            price_to_book DOUBLE PRECISION,
            week_52_high DOUBLE PRECISION,
            week_52_low DOUBLE PRECISION,
            valuation_status VARCHAR(50),
            valuation_comment TEXT,
            company_summary TEXT,
            news_summary TEXT,
            news_bullish_factors TEXT,
            news_bearish_factors TEXT,
            action_points TEXT,
            trading_recommendations TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        '''
        cursor.execute(create_table_query)
        conn.commit()
        print("PostgreSQL table 'analysis_results' ready")
        return conn
    except Exception as e:
        print(f"Postgres error: {e}")
        return None

# --- Analyze single stock ---
def analyze_single_stock(stock_info, postgres_conn):
    """Analyze a single stock and store results in PostgreSQL"""
    ticker = stock_info['Symbol']
    company_name = stock_info['Company Name']
    industry = stock_info['Industry']

    # Check if already analyzed today
    try:
        cursor = postgres_conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM analysis_results 
            WHERE ticker = %s AND DATE(analysis_date) = CURRENT_DATE
        """, (ticker,))
        if cursor.fetchone()[0] > 0:
            print(f"{ticker} already analyzed today, skipping...")
            return "skipped"
    except Exception as e:
        print(f"Error checking existing analysis: {e}")

    # Fetch crawler data via data_ingest helper
    df, data_text = fetch_crawler_data(mindsdb, ticker)
    if df is None:
        print(f"No crawler rows for {ticker}, using fallback text")

    # Prepare prompt (same structure as before)
    analysis_prompt = f"""You are a professional financial analyst and sentiment expert.

You will receive data about a company from web scraping. Analyze this data and provide a comprehensive report.

**Company Information:**
- Ticker: {ticker}
- Company Name: {company_name}
- Industry: {industry}

**IMPORTANT: You must respond ONLY with valid JSON. No other text before or after the JSON.**

... (trimmed for brevity in code block) ...

Here is the data to analyze:

{data_text}

Remember: Respond ONLY with the JSON object. No additional text.
"""
    # Escape for MindsDB SQL literal
    escaped_prompt = analysis_prompt.replace("'", "''")

    # Query the model
    llm_query = f"""
    SELECT answer
    FROM {FULL_MODEL_NAME}
    WHERE question = '{escaped_prompt}'
    """
    try:
        llm_result = mindsdb.query(llm_query)
        analysis = llm_result.fetch()
        if hasattr(analysis, 'iloc') and len(analysis) > 0:
            answer_text = analysis.iloc[0]['answer']
            # strip possible code fences
            if '```json' in answer_text:
                answer_text = answer_text.split('```json')[1].split('```')[0].strip()
            elif '```' in answer_text:
                answer_text = answer_text.split('```')[1].split('```')[0].strip()
            try:
                json_result = json.loads(answer_text)
            except json.JSONDecodeError as je:
                print(f"JSON parse error for {ticker}: {je}")
                return False
            # save file and insert
            with open(f'analysis_{ticker}.json', 'w') as f:
                json.dump(json_result, f, indent=2)
            return insert_analysis_to_postgres(ticker, json_result, postgres_conn)
        else:
            print(f"No response from model for {ticker}")
            return False
    except Exception as e:
        print(f"Error analyzing {ticker}: {e}")
        return False

# --- Insert analysis to PostgreSQL ---
def insert_analysis_to_postgres(ticker, json_result, postgres_conn):
    """Insert analysis results into PostgreSQL - only columns that exist in DB"""
    try:
        cursor = postgres_conn.cursor()
        summary = json_result.get('summary', '')
        community_sentiment = json_result.get('community_sentiment', {})
        overall_sentiment = community_sentiment.get('overall', 'Neutral')
        confidence_score = community_sentiment.get('confidence_score', 0.0)
        credibility = community_sentiment.get('credibility', 'Low')
        bullish_reasons = "; ".join(json_result.get('bullish_reasons', []))
        bearish_reasons = "; ".join(json_result.get('bearish_reasons', []))
        speculative_posts = "; ".join(json_result.get('speculative_posts', []))
        valuation_analysis = json_result.get('valuation_analysis', {})
        pe_ratio = valuation_analysis.get('PE_ratio', None)
        eps = valuation_analysis.get('EPS', None)
        market_cap = str(valuation_analysis.get('market_cap', ''))
        dividend_yield = str(valuation_analysis.get('dividend_yield', ''))
        price_to_book = valuation_analysis.get('price_to_book', None)
        week_52_high = valuation_analysis.get('52_week_high', None)
        week_52_low = valuation_analysis.get('52_week_low', None)
        valuation_status = valuation_analysis.get('valuation_status', '')
        valuation_comment = valuation_analysis.get('comment', '')
        company_summary = json_result.get('company_summary', '')
        news_items = json_result.get('news_summary', [])
        news_text = "; ".join([f"{item.get('headline','')} ({item.get('impact','')}, {item.get('sentiment','')})" 
                             for item in news_items if isinstance(item, dict)])
        news_bullish_factors = "; ".join(json_result.get('news_bullish_factors', []))
        news_bearish_factors = "; ".join(json_result.get('news_bearish_factors', []))
        action_points = "; ".join(json_result.get('action_points', []))
        trading_recommendations = "; ".join(json_result.get('trading_recommendations', []))

        insert_query = '''
        INSERT INTO analysis_results (
            ticker, summary, overall_sentiment, confidence_score, credibility,
            bullish_reasons, bearish_reasons, speculative_posts,
            pe_ratio, eps, market_cap, dividend_yield, price_to_book, 
            week_52_high, week_52_low, valuation_status, valuation_comment,
            company_summary, news_summary, news_bullish_factors, 
            news_bearish_factors, action_points, trading_recommendations
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s
        ) RETURNING id, analysis_date
        '''
        cursor.execute(insert_query, (
            ticker, summary, overall_sentiment, confidence_score, credibility,
            bullish_reasons, bearish_reasons, speculative_posts,
            pe_ratio, eps, market_cap, dividend_yield, price_to_book,
            week_52_high, week_52_low, valuation_status, valuation_comment,
            company_summary, news_text, news_bullish_factors,
            news_bearish_factors, action_points, trading_recommendations
        ))
        inserted_record = cursor.fetchone()
        postgres_conn.commit()
        print(f"{ticker} saved to PostgreSQL (id={inserted_record[0]})")
        return True
    except Exception as pg_error:
        print(f"Postgres insert error for {ticker}: {pg_error}")
        postgres_conn.rollback()
        return False

# --- Main execution ---
def main():
    print("Starting Nifty analysis pipeline")
    postgres_conn = setup_postgres_db()
    if not postgres_conn:
        print("Cannot connect to Postgres, exiting")
        return

    stocks = load_nifty50_symbols()
    if not stocks:
        print("No stocks to analyze, exiting")
        postgres_conn.close()
        return

    success_count = error_count = skip_count = 0
    for i, stock in enumerate(stocks):
        try:
            result = analyze_single_stock(stock, postgres_conn)
            if result is True:
                success_count += 1
            elif result == "skipped":
                skip_count += 1
            else:
                error_count += 1
            if i < len(stocks) - 1:
                time.sleep(30)
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Unexpected error: {e}")
            error_count += 1
            if i < len(stocks) - 1:
                time.sleep(30)

    postgres_conn.close()
    print(f"Done. success={success_count}, skipped={skip_count}, errors={error_count}")

if __name__ == "__main__":
    main()