-- Make sure pgvector extension is enabled
DROP EXTENSION IF EXISTS vector;
CREATE EXTENSION vector;

-- Create the table
CREATE TABLE IF NOT EXISTS items (
  id text PRIMARY KEY,
  content text NOT NULL,
  embeddings vector NOT NULL,
  metadata jsonb
);

-- Dummy data will be handled in tests themselves. Leave it empty.
COMMIT;
