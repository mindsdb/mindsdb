CREATE KNOWLEDGE_BASE equity_analysis_kb
USING
  embedding_model = {
    "provider": "google",
    "model_name": "text-embedding-004",
    "api_key": "<YOUR_GOOGLE_API_KEY>"
  },
  reranking_model = {
    "provider": "google",
    "model_name": "gemini-2.5-flash",
    "api_key": "<YOUR_GOOGLE_API_KEY>"
  },
  content_columns = [
    'summary',
    'company_summary',
    'news_summary',
    'bullish_reasons',
    'bearish_reasons',
    'speculative_posts',
    'valuation_comment',
    'news_bullish_factors',
    'news_bearish_factors',
    'action_points',
    'trading_recommendations'
  ],
  metadata_columns = [
    'ticker',
    'analysis_date',
    'overall_sentiment',
    'confidence_score',
    'credibility',
    'pe_ratio',
    'eps',
    'market_cap',
    'dividend_yield',
    'price_to_book',
    'week_52_high',
    'week_52_low',
    'valuation_status',
    'created_at'
  ],
  id_column = 'id',
  preprocessing = {
    "text_chunking_config": {
      "chunk_size": 800,
      "chunk_overlap": 150
    }
  };