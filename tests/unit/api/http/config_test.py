from mindsdb.utilities.config import config


def test_get_config_returns_knowledge_bases_storage(client):
    previous_storage = config["knowledge_bases"].get("storage", None)

    try:
        config.update({"knowledge_bases": {"storage": "faiss"}})

        response = client.get("/api/config/")

        assert response.status_code == 200
        payload = response.get_json()
        assert payload["knowledge_bases"]["storage"] == "faiss"
        assert "available_vector_engines" in payload["knowledge_bases"]
        assert "pgvector_enabled" in payload["knowledge_bases"]
    finally:
        config.update({"knowledge_bases": {"storage": previous_storage}})
