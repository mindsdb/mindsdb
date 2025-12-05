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

    def test_parameter_extract(self, client):
        def req(query):
            response = client.post(
                "/api/sql/query/utils/parametrize_constants",
                json={"query": query},
            )
            return response.json

        res = req(
            "select 1 year, SUM(case when month = 1 then total_sales else 0 end) as January from pg_demo.sales where total_sales = 100"
        )

        expected = "SELECT :year, sum(CASE WHEN month = :month THEN total_sales ELSE :January END) AS January FROM pg_demo.sales WHERE total_sales = :total_sales"
        assert res["query"] == expected
        assert res["databases"] == {"pg_demo": ["sales"]}
        assert res["parameters"] == [
            {"name": "year", "value": 1, "type": "int"},
            {"name": "month", "value": 1, "type": "int"},
            {"name": "January", "value": 0, "type": "int"},
            {"name": "total_sales", "value": 100, "type": "int"},
        ]

        res = req("INSERT INTO postgres.employees (employee_id, first_name, last_name) VALUES (101, 'John', 'Doe')")
        expected = "INSERT INTO postgres.employees(employee_id, first_name, last_name) VALUES (:employee_id, :first_name, :last_name)"
        assert res["query"] == expected
        assert res["databases"] == {"postgres": ["employees"]}
        assert res["parameters"] == [
            {"name": "employee_id", "value": 101, "type": "int"},
            {"name": "first_name", "value": "John", "type": "str"},
            {"name": "last_name", "value": "Doe", "type": "str"},
        ]

        res = req(
            "UPDATE postgres.products SET price = 10, comments = 'test comment' WHERE price = 11 AND brand='CoverON'"
        )
        expected = (
            "update postgres.products set price=:price, comments=:comments where price = :price2 AND brand = :brand"
        )
        assert res["query"] == expected
        assert res["databases"] == {"postgres": ["products"]}
        assert res["parameters"] == [
            {"name": "price", "value": 10, "type": "int"},
            {"name": "comments", "value": "test comment", "type": "str"},
            {"name": "price2", "value": 11, "type": "int"},
            {"name": "brand", "value": "CoverON", "type": "str"},
        ]
