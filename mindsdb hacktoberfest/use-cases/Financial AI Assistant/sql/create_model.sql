CREATE MODEL mindsdb.financial_analyzer
PREDICT answer
USING
  engine = 'google_gemini_engine',
  model_name = 'gemini-2.5-flash',  -- or 'gemini-pro'
  api_key = '<YOUR_GOOGLE_API_KEY>',
  question_column = 'question';