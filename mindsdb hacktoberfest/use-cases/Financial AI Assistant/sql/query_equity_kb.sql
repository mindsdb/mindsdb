SELECT chunk_content, relevance
FROM equity_analysis_kb
WHERE content = 'Which Stocks will do well in India growth story'
ORDER BY relevance DESC
LIMIT 5;