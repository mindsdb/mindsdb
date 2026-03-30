def test_get_config_returns_knowledge_bases_storage(client):
    response = client.get("/api/config/")

    assert response.status_code == 200
    payload = response.get_json()
    assert "knowledge_bases" in payload
    assert "storage" in payload["knowledge_bases"]
    assert "available_vector_engines" in payload["knowledge_bases"]
    assert "pgvector_enabled" in payload["knowledge_bases"]
