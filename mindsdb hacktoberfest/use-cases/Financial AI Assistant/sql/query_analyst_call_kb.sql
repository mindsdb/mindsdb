SELECT chunk_content, relevance
FROM analyst_call_kb
WHERE content = 'What is the nividia GPU utilization currently by Tata Communications?'
ORDER BY relevance DESC
LIMIT 5;