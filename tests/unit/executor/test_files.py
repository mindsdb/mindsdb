import tempfile
import shutil
from pathlib import Path
import os
import sys
import pytest

import pandas as pd

from mindsdb.api.executor.utilities.sql import query_df
from tests.unit.executor_test_base import BaseExecutorDummyML


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
            {"tab_num": [1, 1, 1, 2, 2, 3], "shop": [1, 2, 3, 1, 2, 1]}
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
            FROM A5 a1
            JOIN A2 a2 ON a1.tab_num = a2.tab_num
            WHERE a1.shop = 1
        """
        dataframes4 = {"a5": df_a5, "a2": df_a2}
        result2 = query_df(dataframes4, query2)
        assert len(result2) == 3

        # print("Test 6: Backward compatibility with single DataFrame")
        # query6 = "SELECT * FROM mytable WHERE tab_num > 1"
        # result6 = query_df(df_a5, query6)  # Pass DataFrame directly
        # print(result6)
        # print()

        # print("All tests completed!")
