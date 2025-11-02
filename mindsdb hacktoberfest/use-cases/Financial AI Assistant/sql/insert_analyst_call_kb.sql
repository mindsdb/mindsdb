INSERT INTO analyst_call_kb 
SELECT url, text_content
FROM my_web.crawler
WHERE url = 'https://www.tatacommunications.com/hubfs/Quarterly%20report/q2-fy2026-earning-call-transcript.pdf';