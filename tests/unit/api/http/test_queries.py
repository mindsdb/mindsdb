class TestParameters:
    def test_query_parameters(self, client):
        # test filter, target
        response = client.post(
            "/api/sql/query",
            json={
                "query": "select NAME, :x from information_schema.databases where NAME=:db_name",
                "params": {"db_name": "mindsdb", "x": 1, "not_used": "abc"},
            },
        )
        data = response.json["data"]
        assert data[0] == ["mindsdb", 1]

        # tuples
        response = client.post(
            "/api/sql/query",
            json={
                "query": "select NAME, :x from information_schema.databases where NAME in :db_name",
                "params": {"db_name": ["mindsdb", "my_pg"], "x": None},
            },
        )
        data = response.json["data"]
        assert data[0] == ["mindsdb", None]
        print(response)
