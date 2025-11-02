CREATE DATABASE postgresql_conn
WITH ENGINE = 'postgres'
PARAMETERS = {
  "host": "host.docker.internal",         -- or host.docker.internal if MindsDB runs in Docker
  "port": 5432,
  "database": "stock_analysis",
  "user": "bseetharaman",
  "schema": "public",
  "password": "postgres"
};