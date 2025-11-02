INSERT INTO research_report_kb
SELECT url, text_content
FROM my_web.crawler
WHERE url = 'https://www.sbisecurities.in/fileserver/research/reports/Macro%20Trends_Monthly%20Economic%20Report_Oct''25.pdf';