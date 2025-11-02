INSERT INTO equity_analysis_kb
SELECT * FROM postgresql_conn.analysis_results WHERE id BETWEEN 1 AND 10;