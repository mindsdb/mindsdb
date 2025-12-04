import tempfile
import shutil
from pathlib import Path
import os
import sys
import pytest

import pandas as pd

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


class TestMultiTableFiles(BaseExecutorDummyML):
    @pytest.fixture(autouse=True)
    def create_tables_and_data(self):
        self.run_sql(
            """
            CREATE TABLE files.A5 (tab_num INT, shop INT)
        """
        )
        self.run_sql(
            """
            INSERT INTO files.A5 VALUES
            (1, 1), (1, 2), (1, 3),
            (2, 1), (2, 2), (2, 3),
            (3, 1)
        """
        )

        self.run_sql(
            """
            CREATE TABLE files.B5 (shop INT)
        """
        )
        self.run_sql(
            """
            INSERT INTO files.B5 (shop) VALUES (1), (2), (3)
        """
        )

        self.run_sql(
            """
            CREATE TABLE files.A2 (
                tab_num INT,
                fio VARCHAR(50),
                city VARCHAR(50)
            )
        """
        )
        self.run_sql(
            """
            INSERT INTO files.A2 VALUES
            (1, 'Stark', 'Winterfell'),
            (2, 'Baratheon', 'King''s Landing'),
            (3, 'Targaryen', 'Dragonstone'),
            (4, 'Lannister', 'Casterly Rock')
        """
        )

        yield

    def test_multi_table_relational_division(self):
        """Test complex multi-table relational division"""

        result = self.run_sql(
            """
            SELECT DISTINCT a1.tab_num
            FROM files.A5 a1
            WHERE NOT EXISTS (
                SELECT *
                FROM files.B5 b
                WHERE NOT EXISTS (
                    SELECT *
                    FROM files.A5 a2
                    WHERE a2.tab_num = a1.tab_num AND a2.shop = b.shop
                )
            )
        """
        )

        assert len(result) == 3
        assert sorted(result["tab_num"].tolist()) == [1, 2, 3]

    def test_multi_table_join_with_aliases(self):
        """Test JOIN with aliases and database prefixes"""
        result = self.run_sql(
            """
            SELECT DISTINCT a1.tab_num, a2.fio, a2.city
            FROM files.A5 AS a1
            JOIN files.A2 AS a2 ON a1.tab_num = a2.tab_num
            WHERE a1.shop = 1
        """
        )
        assert len(result) == 3
        assert sorted(result["tab_num"].tolist()) == [1, 2, 3]
        assert sorted(result["fio"].tolist()) == [
            "Baratheon",
            "Stark",
            "Targaryen",
        ]
        assert sorted(result["city"].tolist()) == [
            "Dragonstone",
            "King's Landing",
            "Winterfell",
        ]

    def test_multi_table_join_without_aliases(self):
        """Test JOIN without aliases and without database prefixes"""
        with pytest.raises(Exception) as excinfo:
            self.run_sql(
                """
                SELECT DISTINCT A5.tab_num
                FROM A5
                JOIN A2 ON A5.tab_num = A2.tab_num
                WHERE A5.shop = 1
            """
            )
        msg = str(excinfo.value).lower()
        assert "table 'a5' not found in database" in msg

    def test_create_table_with_existing_name(self):
        """Test creating a table with an existing name without REPLACE"""
        with pytest.raises(Exception) as excinfo:
            self.run_sql(
                """
                CREATE TABLE files.A5 (tab_num INT, shop INT)
                """
            )
        msg = str(excinfo.value).lower()
        assert "table 'a5' already exists" in msg
