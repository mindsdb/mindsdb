#!/usr/bin/env python3
import sys
sys.path.append('/app')
from src.knowledge_base.kb_manager import KBManager

kb_manager = KBManager(mindsdb_url='http://mindsdb:47334', db_path='/app/data/academic_papers.duckdb')
server = kb_manager.connect()

print('\n🔍 Checking Knowledge Base status...\n')

# Try SELECT to see if data exists
query = 'SELECT COUNT(*) as total FROM academic_kb;'
result = server.query(query).fetch()
count = result['total'][0]

print(f'📊 Total chunks in KB: {count}')

if count > 0:
    print('\n🎉 SUCCESS! Gemini embeddings are working!\n')
    query2 = '''
    SELECT id, chunk_content, relevance
    FROM academic_kb
    WHERE content = 'deep learning and neural networks'
    LIMIT 5;
    '''
    result2 = server.query(query2).fetch()
    print(f'🔍 Semantic search test - found {len(result2)} results:\n')
    for idx, row in result2.iterrows():
        print(f'{idx+1}. Relevance: {row["relevance"]:.4f}')
        print(f'   Content: {row["chunk_content"][:100]}...\n')
else:
    print('\n⚠️  KB is empty. Embeddings may still be processing.')
