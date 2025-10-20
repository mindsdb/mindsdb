import tempfile
import shutil
from pathlib import Path
import os
import sys
import pytest

import pandas as pd

from mindsdb.api.executor.utilities.sql import query_df
from tests.unit.executor_test_base import BaseExecutorDummyML
from mindsdb.utilities.exception import QueryError


class TestFiles(BaseExecutorDummyML):
    def test_create_table(self):
        df = pd.DataFrame(
            [
                {"a": 6, "c": 1},
                {"a": 7, "c": 2},
            ]
        )
        self.set_data("table1", df)

        self.run_sql(
            """
            create table files.myfile
            select * from dummy_data.table1
        """
        )

        self.run_sql(
            """
            create or replace table files.myfile
            select * from dummy_data.table1
        """
        )

        ret = self.run_sql("select count(*) c from files.myfile")
        assert ret["c"][0] == 2

        self.run_sql(
            """
            insert into files.myfile (
              select * from dummy_data.table1
            )
        """
        )

        ret = self.run_sql("select count(*) c from files.myfile")
        assert ret["c"][0] == 4

        self.run_sql(
            """
            insert into files.myfile (a)
            values (9)
        """
        )

        ret = self.run_sql("select count(*) c from files.myfile")
        assert ret["c"][0] == 5

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Fixme: Open file handle somewhere makes this fail on Windows.",
    )
    def test_multipage(self):
        # copy test file because source will be removed after uloading
        source_path = Path(__file__).parent / "data" / "test.xlsx"
        fd, file_path = tempfile.mkstemp()
        os.close(fd)
        shutil.copy(source_path, file_path)

        self.file_controller.save_file("test", file_path, source_path.name)

        ret = self.run_sql("select * from files.test")
        assert len(ret) == 2
        first, second = ret[ret.columns[0]]

        # first page
        ret = self.run_sql(f"select * from files.test.{first}")
        assert len(ret.columns) == 4

        # second page
        ret = self.run_sql(f"select * from files.test.{second}")
        assert len(ret.columns) == 2

    def test_query_df_single_dataframe(self):
        """Test the query_df function with a single dataframe"""

        df = pd.DataFrame(
            {
                "tab_num": [1, 2, 3, 4],
                "fio": ["Stark", "Baratheon", "Lannister", "Targaryen"],
                "city": [
                    "Winterfell",
                    "King's Landing",
                    "Casterly Rock",
                    "Dragonstone",
                ],
            }
        )

        print("Test 1: Simple SELECT query")
        query1 = "SELECT * FROM mytable WHERE tab_num > 1"
        result1 = query_df(df, query1)
        assert len(result1) == 3

        query2 = "SELECT COUNT(*) as count FROM mytable WHERE city LIKE '%Rock%'"
        result2 = query_df(df, query2)
        assert result2["count"][0] == 1

        try:
            query3 = "SELEC* FORM mytable"
            result3 = query_df(df, query3)
            assert False, "Expected an exception for invalid SQL syntax"
        except Exception as e:
            print(f"Expected error caught: {e}")

    def test_multiple_dataframes(self):
        """Test the updated query_df function with multiple dataframes"""

        df_a5 = pd.DataFrame(
            {"tab_num": [1, 1, 1, 2, 2, 2, 3], "shop": [1, 2, 3, 1, 2, 3, 1]}
        )

        df_b5 = pd.DataFrame({"shop": [1, 2, 3]})

        df_a2 = pd.DataFrame(
            {
                "tab_num": [1, 2, 3, 4],
                "fio": ["Stark", "Baratheon", "Lannister", "Targaryen"],
                "city": [
                    "Winterfell",
                    "King's Landing",
                    "Casterly Rock",
                    "Dragonstone",
                ],
            }
        )

        query = """
            SELECT DISTINCT a1.tab_num
            FROM A5 a1
            WHERE NOT EXISTS (
                SELECT *
                FROM B5 b
                WHERE NOT EXISTS (
                    SELECT *
                    FROM A5 a2
                    WHERE a2.tab_num = a1.tab_num AND a2.shop = b.shop
                )
            )
        """
        dataframes3 = {"a5": df_a5, "b5": df_b5}
        result = query_df(dataframes3, query)
        assert len(result) == 2

        query2 = """
            SELECT DISTINCT a1.tab_num, a2.fio, a2.city
            FROM A5 as a1
            JOIN A2 as a2 ON a1.tab_num = a2.tab_num
            WHERE a1.shop = 1
        """
        dataframes4 = {"a5": df_a5, "a2": df_a2}
        result2 = query_df(dataframes4, query2)
        assert len(result2) == 3

        query3 = "SELECT * FROM mytable WHERE tab_num > 1"
        result3 = query_df(df_a5, query3)
        assert len(result3) == 4

    def test_query_df_complex_mixed_operations(self):
        """Test the query_df function with complex mixed operations"""

        df_orders = pd.DataFrame(
            {
                "order_id": [1, 2, 3, 4, 5],
                "customer_id": [101, 102, 101, 103, 102],
                "amount": [250, 450, 300, 150, 500],
            }
        )

        df_customers = pd.DataFrame(
            {
                "customer_id": [101, 102, 103],
                "customer_name": ["Alice", "Bob", "Charlie"],
            }
        )

        query = """
            SELECT c.customer_name, SUM(o.amount) as total_amount
            FROM Orders o
            JOIN Customers c ON o.customer_id = c.customer_id
            WHERE o.amount > 200
            GROUP BY c.customer_name
            HAVING total_amount > 400
            ORDER BY total_amount DESC
        """
        dataframes = {"Orders": df_orders, "Customers": df_customers}
        result = query_df(dataframes, query)
        assert len(result) == 2
        assert result["customer_name"].tolist() == ["Bob", "Alice"]

    def test_query_df_ensures_valid_ast_after_traversal(self):
        """Test that adapt_query doesn't accidentally replace nodes with None"""

        df = pd.DataFrame({"tab_num": [1, 2, 3], "amount": [100, 200, 300]})

        query = """
            SELECT
                tab_num,
                ABS(amount - 150) as diff,
                ROUND(amount / 100.0, 2) as ratio
            FROM mytable
            WHERE tab_num IN (1, 2, 3)
            AND amount > 50
        """

        try:
            result = query_df(df, query)
            assert len(result) == 3
            assert "diff" in result.columns
            assert "ratio" in result.columns

            assert result["diff"].tolist() == [50, 50, 150]

        except Exception as e:
            pytest.fail(f"Query failed due to AST traversal issue: {e}")

    def test_query_df_with_subquery_from(self):
        """Test query_df when from_table is a subquery (not an Identifier)"""

        df = pd.DataFrame({"tab_num": [1, 2, 3, 4], "amount": [100, 200, 300, 400]})

        query = """
            SELECT outer_table.tab_num, outer_table.amount
            FROM (
                SELECT tab_num, amount 
                FROM mytable 
                WHERE amount > 150
            ) AS outer_table
            WHERE outer_table.amount < 350
        """

        result = query_df(df, query)
        assert len(result) == 2
        assert result["amount"].tolist() == [200, 300]

    def test_query_df_with_qualified_columns_in_subquery(self):
        """Test that column qualifiers work correctly with subqueries"""

        df = pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})

        query = """
            SELECT t.id, t.value * 2 as doubled
            FROM (SELECT id, value FROM mytable) AS t
            WHERE t.value > 15
        """

        result = query_df(df, query)
        assert len(result) == 2
        assert result["doubled"].tolist() == [40, 60]

    def test_query_df_single_df_with_table_qualifier(self):
        """Test that table qualifiers are handled when from_table_name is None"""

        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

        query = """
            SELECT mytable.col1, mytable.col2
            FROM mytable
            WHERE mytable.col1 > 1
        """

        result = query_df(df, query)
        assert len(result) == 2

    def test_query_df_table_not_found_error(self):
        """Test that appropriate error is raised when table not found with multiple dataframes"""

        df_a = pd.DataFrame({"col1": [1, 2, 3]})
        df_b = pd.DataFrame({"col2": [4, 5, 6]})

        dataframes = {"table_a": df_a, "table_b": df_b}

        query = """
            SELECT * FROM nonexistent_table
        """

        with pytest.raises(QueryError) as exc_info:
            query_df(dataframes, query)

        assert "not found in provided dataframes" in str(exc_info.value)
        assert "nonexistent_table" in str(exc_info.value).lower()

    def test_query_df_multiple_tables_not_found(self):
        """Test error when referencing table not in dict"""

        df_a = pd.DataFrame({"id": [1, 2]})
        df_b = pd.DataFrame({"id": [3, 4]})

        dataframes = {"table_a": df_a, "table_b": df_b}

        query = """
            SELECT * FROM table_a
            JOIN table_c ON table_a.id = table_c.id
        """

        with pytest.raises(QueryError) as exc_info:
            query_df(dataframes, query)

        assert "not found in provided dataframes" in str(exc_info.value)

    def test_query_df_unknown_function_error(self):
        """Test that unknown functions raise appropriate error"""

        df = pd.DataFrame({"value": [1, 2, 3]})

        # SOME_INVALID_FUNCTION doesn't exist in DuckDB
        query = """
            SELECT SOME_INVALID_FUNCTION(value) as result
            FROM mytable
        """

        with pytest.raises(QueryError) as exc_info:
            query_df(df, query)

        assert "Unknown function" in str(exc_info.value)

    def test_query_df_empty_dataframe(self):
        """Test query_df with empty dataframe"""

        df = pd.DataFrame({"col1": [], "col2": []})

        query = "SELECT * FROM mytable WHERE col1 > 0"

        result = query_df(df, query)
        assert len(result) == 0
        assert list(result.columns) == ["col1", "col2"]

    def test_query_df_invalid_column_reference(self):
        """Test error when column doesn't exist"""

        df = pd.DataFrame({"col1": [1, 2, 3]})

        query = """
            SELECT col1, nonexistent_column
            FROM mytable
        """

        with pytest.raises(QueryError):
            query_df(df, query)

    def test_query_df_type_safety_with_mixed_types(self):
        """Test query_df handles mixed types gracefully"""

        df = pd.DataFrame({"id": [1, 2, 3], "mixed": [1, "text", 3.14]})  # Mixed types

        query = "SELECT * FROM mytable WHERE id > 1"

        # Should handle mixed types without crashing
        result = query_df(df, query)
        assert len(result) == 2

    def test_query_df_dict_not_dataframe_error(self):
        """Test that passing wrong type raises ValueError"""

        invalid_input = "not a dataframe"

        query = "SELECT * FROM mytable"

        with pytest.raises(ValueError) as exc_info:
            query_df(invalid_input, query)

        assert "should be pandas.DataFrame or dict" in str(exc_info.value)

    def test_query_df_non_select_with_single_df_error(self):
        """Test that non-SELECT queries with single DataFrame raise error"""

        df = pd.DataFrame({"col1": [1, 2, 3]})

        # INSERT query (not Select)
        from mindsdb_sql_parser import parse_sql

        insert_query = parse_sql("INSERT INTO mytable VALUES (1)")

        with pytest.raises(ValueError) as exc_info:
            query_df(df, insert_query)

        assert "only Select queries are supported" in str(exc_info.value)
