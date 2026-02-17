from http import HTTPStatus

from unittest.mock import patch


@patch("mindsdb.integrations.handlers.chromadb_handler.chromadb_handler.ChromaDBHandler")
@patch("mindsdb.integrations.handlers.litellm_handler.litellm_handler.embedding")
def test_update_kb_embeddings(mock_embedding, chroma, client):
    # for test of embeddings
    mock_embedding().data = [{"embedding": [0.1, 0.2]}]

    integration_data = {
        "database": {
            "name": "kb_vector_db",
            "engine": "chromadb",
            "parameters": {"persist_directory": "kb_vector_db"},
        }
    }
    response = client.post("/api/databases", json=integration_data, follow_redirects=True)
    assert response.status_code == HTTPStatus.CREATED

    create_response = client.post(
        "/api/projects/mindsdb/knowledge_bases",
        follow_redirects=True,
        json={
            "knowledge_base": {
                "name": "test_kb",
                "storage": {"database": "kb_vector_db", "table": "default_collection"},
                "params": {
                    "embedding_model": {
                        "provider": "gemini",
                        "model_name": "dummy_model",
                        "api_key": "embed-key-1",
                    }
                },
            }
        },
    )
    assert create_response.status_code == HTTPStatus.CREATED

    mock_embedding.reset_mock()
    update_response = client.put(
        "/api/projects/mindsdb/knowledge_bases/test_kb",
        json={
            "knowledge_base": {
                "params": {
                    "embedding_model": {
                        "api_key": "embed-key-2",
                    }
                }
            }
        },
        follow_redirects=True,
    )

    assert update_response.status_code == HTTPStatus.OK
    kwargs = mock_embedding.call_args_list[0][1]
    assert kwargs["api_key"] == "embed-key-2"
