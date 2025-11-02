CREATE KNOWLEDGE_BASE research_report_kb
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
  content_columns  = ['text_content'],
  metadata_columns = ['url'],
  id_column        = 'url',
  preprocessing = {
    "text_chunking_config": {
      "chunk_size": 800,
      "chunk_overlap": 150
    }
  };