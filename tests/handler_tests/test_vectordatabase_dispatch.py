from unittest.mock import Mock

import pandas as pd
import pytest
from mindsdb_sql import parse_sql
from pandas.testing import assert_frame_equal

from mindsdb.integrations.libs.vectordatabase_handler import (
    FilterCondition,
    FilterOperator,
    VectorStoreHandler,
)


@pytest.fixture
def vector_store_handler():
    # patch the actual "execute" methods of the handler with Mock
    vector_store_handler: VectorStoreHandler = VectorStoreHandler("test")
    vector_store_handler.create_table = Mock()
    vector_store_handler.drop_table = Mock()
    vector_store_handler.insert = Mock()
    vector_store_handler.update = Mock()
    vector_store_handler.delete = Mock()
    vector_store_handler.select = Mock()

    return vector_store_handler


def test_vectordatabase_parsing(vector_store_handler):
    # create a table
    # due to the limitation of the parser
    # we can only create a table with a select statement
    sql = """
        CREATE TABLE chroma_db.test_table (
            SELECT *
            FROM chroma_db.test_table
        )
    """
    query = parse_sql(sql, dialect="mindsdb")
    vector_store_handler._dispatch(query)
    vector_store_handler.create_table.assert_called_once()
    vector_store_handler.create_table.assert_called_with(
        "test_table", if_not_exists=False
    )

    # drop a table
    sql = """
        DROP TABLE chroma_db.test_table
    """
    query = parse_sql(sql, dialect="mindsdb")
    vector_store_handler._dispatch(query)
    vector_store_handler.drop_table.assert_called_once()
    vector_store_handler.drop_table.assert_called_with("test_table", if_exists=False)

    # insert into a table
    sql = """
        INSERT INTO chroma_db.test_table (
            id,content,metadata,embeddings
        )
        VALUES (
            1, 'test', '{"some_field": "some_value"}', '[1,2,3]'
        ),
        (
            2, 'test', '{"some_field": "some_value"}', '[1,2,3]'
        )
    """

    data = pd.DataFrame(
        {
            "id": [1, 2],
            "content": ["test", "test"],
            "embeddings": [[1, 2, 3], [1, 2, 3]],
            "metadata": [{"some_field": "some_value"}, {"some_field": "some_value"}],
        }
    )

    query = parse_sql(sql, dialect="mindsdb")
    vector_store_handler._dispatch(query)
    vector_store_handler.insert.assert_called_once()
    # get the args passed to the insert method
    args, kwargs = vector_store_handler.insert.call_args
    # get the data passed to the insert method
    assert args[0] == "test_table"
    data = args[1]
    # assert the data is the same
    assert_frame_equal(data, data)
    assert kwargs["columns"] == ["id", "content", "metadata", "embeddings"]

    # select from a table
    # select without filters
    sql = """
        SELECT *
        FROM chroma_db.test_table
    """
    query = parse_sql(sql, dialect="mindsdb")
    vector_store_handler._dispatch(query)
    vector_store_handler.select.assert_called_once()
    vector_store_handler.select.assert_called_with(
        "test_table",
        columns=["id", "content", "embeddings", "metadata"],
        conditions=None,
        offset=None,
        limit=None,
    )
    # select with search_vector filter
    sql = """
        SELECT *
        FROM chroma_db.test_table
        WHERE search_vector = '[1, 2, 3]'
        LIMIT 10
    """
    query = parse_sql(sql, dialect="mindsdb")
    # reset the mock
    vector_store_handler.select.reset_mock()
    vector_store_handler._dispatch(query)
    vector_store_handler.select.assert_called_once()
    vector_store_handler.select.assert_called_with(
        "test_table",
        columns=["id", "content", "embeddings", "metadata"],
        conditions=[
            FilterCondition(
                column="search_vector", op=FilterOperator.EQUAL, value=[1, 2, 3]
            )
        ],
        limit=10,
        offset=None,
    )

    # select with limit and offset
    sql = """
        SELECT *
        FROM chroma_db.test_table
        LIMIT 10
        OFFSET 5
    """
    query = parse_sql(sql, dialect="mindsdb")
    # reset the mock
    vector_store_handler.select.reset_mock()
    vector_store_handler._dispatch(query)
    vector_store_handler.select.assert_called_once()
    vector_store_handler.select.assert_called_with(
        "test_table",
        columns=["id", "content", "embeddings", "metadata"],
        conditions=None,
        limit=10,
        offset=5,
    )

    # select with a subset of columns
    sql = """
        SELECT id, content
        FROM chroma_db.test_table
    """
    query = parse_sql(sql, dialect="mindsdb")
    # reset the mock
    vector_store_handler.select.reset_mock()
    vector_store_handler._dispatch(query)
    vector_store_handler.select.assert_called_once()
    vector_store_handler.select.assert_called_with(
        "test_table",
        columns=["id", "content"],
        conditions=None,
        limit=None,
        offset=None,
    )

    # select with metadata filter
    sql = """
        SELECT *
        FROM chroma_db.test_table
        WHERE metadata.created_at = '2021-01-01'
        AND metadata.some_field in ('some_value', 'some_other_value')
        AND search_vector = '[1, 2, 3]'
    """
    query = parse_sql(sql, dialect="mindsdb")
    # reset the mock
    vector_store_handler.select.reset_mock()
    vector_store_handler._dispatch(query)
    vector_store_handler.select.assert_called_once()
    vector_store_handler.select.assert_called_with(
        "test_table",
        columns=["id", "content", "embeddings", "metadata"],
        conditions=[
            FilterCondition(
                column="metadata.created_at",
                op=FilterOperator.EQUAL,
                value="2021-01-01",
            ),
            FilterCondition(
                column="metadata.some_field",
                op=FilterOperator.IN,
                value=["some_value", "some_other_value"],
            ),
            FilterCondition(
                column="search_vector", op=FilterOperator.EQUAL, value=[1, 2, 3]
            ),
        ],
        limit=None,
        offset=None,
    )

    # delete from a table
    sql = """
        DELETE FROM chroma_db.test_table
        WHERE id = 1
    """
    query = parse_sql(sql, dialect="mindsdb")
    vector_store_handler._dispatch(query)
    vector_store_handler.delete.assert_called_once()
    vector_store_handler.delete.assert_called_with(
        "test_table",
        conditions=[FilterCondition(column="id", op=FilterOperator.EQUAL, value=1)],
    )


def test_unsupported_ops(vector_store_handler):
    # select unsupported columns
    sql = """
        SELECT id, some_column_not_supported
        FROM chroma_db.test_table
    """
    query = parse_sql(sql, dialect="mindsdb")
    with pytest.raises(Exception) as e:
        vector_store_handler._dispatch(query)
    assert "not allowed" in str(e.value)

    # insert unsupported columns
    sql = """
        INSERT INTO chroma_db.test_table (
            id, some_column_not_supported
        )
        VALUES (
            1, 'test'
        )
    """
    query = parse_sql(sql, dialect="mindsdb")
    with pytest.raises(Exception) as e:
        vector_store_handler._dispatch(query)
    assert "not allowed" in str(e.value)

    # unsupported filter
    sql = """
        SELECT *
        FROM chroma_db.test_table
        WHERE metadata.created_at > '2021-01-01'
        AND unknown_column = 'some_value'
    """
    query = parse_sql(sql, dialect="mindsdb")
    # reset the mock
    vector_store_handler.select.reset_mock()
    vector_store_handler._dispatch(query)
    vector_store_handler.select.assert_called_once()
    vector_store_handler.select.assert_called_with(
        "test_table",
        columns=["id", "content", "embeddings", "metadata"],
        conditions=[
            FilterCondition(
                column="metadata.created_at",
                op=FilterOperator.GREATER_THAN,
                value="2021-01-01",
            ),
            FilterCondition(
                column="metadata.unknown_column",  # we will treat this as a metadata filter
                op=FilterOperator.EQUAL,
                value="some_value",
            ),
        ],
        limit=None,
        offset=None,
    )


@pytest.mark.xfail(reason="not implemented yet")
def test_unimplemented_yet(vector_store_handler):
    # select count(*) is not implemented yet
    sql = """
        SELECT count(*)
        FROM chroma_db.test_table
    """
    query = parse_sql(sql, dialect="mindsdb")
    with pytest.raises(Exception):
        vector_store_handler._dispatch(query)
