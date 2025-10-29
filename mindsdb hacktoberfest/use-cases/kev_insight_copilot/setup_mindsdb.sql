CREATE DATABASE cyber_postgres
WITH ENGINE = 'postgres',
PARAMETERS = {
  "user": "mindsdb_user",
  "password": "mindsdb_pass",
  "host": "host.docker.internal",
  "port": "5432",
  "database": "cyber_threat_db"
};

SELECT *
FROM cyber_postgres.public.cisa_known_exploited
LIMIT 10;

CREATE KNOWLEDGE_BASE kev_kb
USING
    embedding_model = {
        "provider": "openai",
        "model_name": "text-embedding-3-large",
        "api_key": "your-model-api-key"
    },
    reranking_model = {
        "provider": "openai",
        "model_name": "gpt-4o",
        "api_key": "your-model-api-key"
    },
    metadata_columns = ["vendorProject", "product", "cveID", "dateAdded"],
    content_columns = ["shortDescription", "vulnerabilityName", "notes"],
    id_column = "cveID";

INSERT INTO kev_kb
SELECT *
FROM cyber_postgres.public.cisa_known_exploited;

SELECT *
FROM kev_kb
WHERE content = 'Microsoft';

SELECT *
FROM kev_kb
WHERE vendorProject = 'Microsoft'
AND content = 'privilege escalation'
AND relevance >= 0.50;

CREATE AGENT kev_security_agent
USING
    model = {
        "provider": "openai",
        "model_name" : "gpt-4o",
        "api_key": "your-model-api-key"
    },
    data = {
         "knowledge_bases": ["kev_kb"]
    },
    prompt_template='
        You are a cybersecurity expert specializing in Known Exploited Vulnerabilities.
        describe data from kev_kb to answer questions precisely with references to CVE IDs.Always mention the product/vendor and type of vulnerability.
    ';

SELECT answer
FROM kev_security_agent
WHERE question = 'List all vulnerabilities from Cisco?';
