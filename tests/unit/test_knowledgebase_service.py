"""
Tests for knowledge base service
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from mindsdb.interfaces.knowledge_base.service import (
    FilterCondition,
    FilterOP,
    KnowledgeBaseService,
)
from mindsdb.interfaces.storage.db import KnowledgeBase


@pytest.fixture
def mock_embedding_model_handler():
    """
    Mock embedding model handler
    # TODO: to be replaced with a real embedding model handler
    """
    mock_handler = MagicMock()

    # for the predict method, we append a column to the input dataframe,
    # which is the embedding vector
    def predict(df: pd.DataFrame, args=None):
        embedding_vector = [1, 2, 3]
        vector_col = []
        for _ in range(df.shape[0]):
            vector_col.append(embedding_vector)
        # make a copy of the input dataframe
        df = df.copy()
        df["embedding_vector"] = vector_col
        return df

    mock_handler.predict.side_effect = predict

    # for the describe method, when attribute is 'args', we return a dataframe
    # which contains the target and output_column info
    def describe(attribute):
        if attribute == "args":
            return pd.DataFrame(
                {
                    "key": ["target", "output_column"],
                    "value": ["embedding_vector", "embedding_vector"],
                }
            )
        else:
            return None

    mock_handler.describe.side_effect = describe

    return mock_handler


@pytest.fixture
def mock_vector_database_handler():
    """
    Mock vector database handler
    # TODO: to be replaced with a real vector database handler
    """
    mock_handler = MagicMock()

    def select(table_name, search_vector, metadata_filters, limit):
        # return a dataframe with a column named 'content'
        return pd.DataFrame(
            {
                "id": [1, 2, 3],
                "content": ["test", "test", "test"],
                "metadata": [{"test": "test"}, {"test": "test"}, {"test": "test"}],
            }
        )

    mock_handler.select.side_effect = select

    return mock_handler


@pytest.fixture
def mock_knowledgebase():
    """
    Mock knowledge base
    """
    return KnowledgeBase(
        name="test knowledge base",
        company_id=-1,
        project_id=-1,
        embedding_model_id=-1,
        vector_database_id=-1,
        vector_database_table_name="test",
    )


@pytest.fixture
def knowledgebase_service(
    mock_knowledgebase, mock_embedding_model_handler, mock_vector_database_handler
) -> KnowledgeBaseService:
    """
    Knowledge base service
    """
    # patch KnowledgeBaseService's _get_embedding_model_handler method
    # patch KnowledgeBaseService's _get_vector_database_handler method
    with patch.object(
        KnowledgeBaseService,
        "_get_embedding_model_handler",
        return_value=mock_embedding_model_handler,
    ), patch.object(
        KnowledgeBaseService,
        "_get_vector_database_handler",
        return_value=mock_vector_database_handler,
    ):
        return KnowledgeBaseService(mock_knowledgebase)


def test_select_from_knowledgebase(knowledgebase_service):
    """
    Test select from knowledge base
    """

    # test select by providing search texts
    result = knowledgebase_service.select(
        search_texts=[
            FilterCondition(column="content", operator=FilterOP.EQUAL, value="test")
        ]
    )
    assert result.shape[0] == 3
    # the embedding model predict method should be called
    # with the input dataframe
    # get the dataframe passed to the predict method
    input_df = knowledgebase_service.embedding_model_handler.predict.call_args[0][0]
    pd.testing.assert_frame_equal(input_df, pd.DataFrame({"content": ["test"]}))

    # multiple search texts
    result = knowledgebase_service.select(
        search_texts=[
            FilterCondition(column="content", operator=FilterOP.EQUAL, value="test"),
            FilterCondition(column="content2", operator=FilterOP.EQUAL, value="test2"),
        ]
    )
    input_df = knowledgebase_service.embedding_model_handler.predict.call_args[0][0]
    pd.testing.assert_frame_equal(
        input_df, pd.DataFrame({"content": ["test"], "content2": ["test2"]})
    )
    assert result.shape[0] == 3

    # test select by providing metadata filters
    # reset the mock embedding model handler
    knowledgebase_service.embedding_model_handler.reset_mock()
    result = knowledgebase_service.select(
        metadata_filters=[
            FilterCondition(
                column="created_at", operator=FilterOP.GREATER_THAN, value="2020-01-01"
            )
        ]
    )
    # we should not call the embedding model predict method
    knowledgebase_service.embedding_model_handler.predict.assert_not_called()
    assert result.shape[0] == 3

    # test select by providing both search texts and metadata filters
    result = knowledgebase_service.select(
        search_texts=[
            FilterCondition(column="content", operator=FilterOP.EQUAL, value="test")
        ],
        metadata_filters=[
            FilterCondition(
                column="created_at", operator=FilterOP.GREATER_THAN, value="2020-01-01"
            )
        ],
    )

    assert result.shape[0] == 3


def test_delete_from_knowledgebase(knowledgebase_service):
    """
    Test delete from knowledge base
    """
    knowledgebase_service.delete(
        metadata_filters=[
            FilterCondition(
                column="created_at", operator=FilterOP.GREATER_THAN, value="2020-01-01"
            )
        ]
    )
    # vector database handler's delete method should be called
    knowledgebase_service.vector_database_handler.delete.assert_called_once()

    # specifying search texts should raise an error
    with pytest.raises(TypeError):
        knowledgebase_service.delete(
            search_texts=[
                FilterCondition(column="content", operator=FilterOP.EQUAL, value="test")
            ]
        )


def test_update_knowledgebase(knowledgebase_service):
    """
    Test update knowledge base
    """
    data = pd.DataFrame(data={"content": ["test"], "content2": ["test2"]})

    knowledgebase_service.update(df=data, columns=["content", "content2"])
    # vector database handler's insert method should be called
    knowledgebase_service.vector_database_handler.insert.assert_called_once()


def test_insert_into_knowledgebase(knowledgebase_service):
    """
    Test insert into knowledge base
    """
    data = pd.DataFrame(data={"content": ["test"], "content2": ["test2"]})

    knowledgebase_service.insert(df=data, columns=["content", "content2"])

    # vector database handler's insert method should be called
    knowledgebase_service.vector_database_handler.insert.assert_called_once()
