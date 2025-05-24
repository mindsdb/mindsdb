-- Make sure pgvector extension is enabled
DROP EXTENSION IF EXISTS vector;
CREATE EXTENSION vector;

-- Create the table
CREATE TABLE items (
  id bigserial PRIMARY KEY,
  content text NOT NULL,
  embeddings vector(3) NOT NULL, -- 3 dimensions
  metadata jsonb
);

-- Dummy data
INSERT INTO items (content, embeddings, metadata) VALUES
  ('a fat cat sat on a mat and ate a fat rat', '[1, 2, 3]', '{"location": "Wonderland", "author": "Taishan"}'),
  ('a fat dog sat on a mat and ate a fat rat', '[4, 5, 6]', '{"location": "Wonderland", "author": "Taishan"}'),
  ('a thin cat sat on a mat and ate a thin rat', '[7, 8, 9]', '{"location": "Zimbabwe", "author": "Taishan"}'),
  ('a thin dog sat on a mat and ate a thin rat', '[10, 11, 12]', '{"location": "Springfield", "author": "Muhammad"}');

COMMIT;
