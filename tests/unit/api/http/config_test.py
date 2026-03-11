from mindsdb.utilities.config import config


def test_get_config_returns_knowledge_bases_storage(client):
    previous_storage = config["knowledge_bases"].get("storage", [])

    try:
        config.update({"knowledge_bases": {"storage": ["duckdb_faiss", "pgvector"]}})

        response = client.get("/api/config/")

        assert response.status_code == 200
        payload = response.get_json()
        assert payload["knowledge_bases"]["storage"] == ["duckdb_faiss", "pgvector"]
        assert payload["knowledge_bases"]["default_storage"] in {"duckdb_faiss", "pgvector", None}
        assert [item["engine"] for item in payload["knowledge_bases"]["resolved_storage"]] == [
            "duckdb_faiss",
            "pgvector",
        ]
    finally:
        config.update({"knowledge_bases": {"storage": previous_storage}})
