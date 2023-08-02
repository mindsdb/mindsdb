import os
from unittest.mock import patch

import pandas as pd
import pytest

from mindsdb.interfaces.knowledge_base.service import (
    KnowledgeBaseError,
    KnowledgeBaseService,
)
from mindsdb.interfaces.storage.db import (
    KBEmbeddingModel,
    KBRetrievalStrategy,
    KBVectorDatabase,
    KnowledgeBase,
)


# patch mindsdb.utilities.fs.get_or_create_data_dir
# to return a temp dir
@pytest.fixture(autouse=True, scope="function")
def temp_data_dir(tmp_path):
    with patch(
        "mindsdb.interfaces.knowledge_base.service.get_or_create_data_dir"
    ) as mock:
        mock.return_value = str(tmp_path)
        yield mock


@pytest.fixture
def knowledge_base_object():
    return KnowledgeBase(
        name="test",
        collection_handle="test",
        project_id=-1,
        params={
            "embedding_model": KBEmbeddingModel.DUMMY,
            "retrieval_strategy": KBRetrievalStrategy.SIMILARITY,
            "vector_database": KBVectorDatabase.CHROMADB,
            "content_field": "content",
            "id_field": "id",
        },
    )


@pytest.fixture
def docs_dataframe():
    return pd.DataFrame(
        [
            ["1", "content1", "metadata1"],
            ["2", "content2", "metadata2"],
            ["3", "content3", "metadata3"],
            ["4", "content4", "metadata4"],
        ],
        columns=["id", "content", "metadata"],
    )


def test_no_id_field(knowledge_base_object, docs_dataframe):
    """
    When the dataframe does not have the id_field, an error should be raised
    """
    # wrong id column name
    docs_dataframe = docs_dataframe.rename(columns={"id": "wrong_id"})
    # create the serivce
    service = KnowledgeBaseService(knowledge_base_object)

    with pytest.raises(KnowledgeBaseError):
        service.create_index(df=docs_dataframe)

    docs_dataframe = docs_dataframe.drop(columns=["wrong_id"])
    # create the serivce
    service = KnowledgeBaseService(knowledge_base_object)
    # create the index
    with pytest.raises(KnowledgeBaseError):
        service.create_index(df=docs_dataframe)


def test_no_content_field(knowledge_base_object, docs_dataframe):
    """
    When the dataframe does not have the content_field
    Every column besides the id_field should be considered contents
    """
    # providing the wrong content column name should raise an error
    docs_dataframe = docs_dataframe.rename(columns={"content": "wrong_content"})

    # create the serivce
    service = KnowledgeBaseService(knowledge_base_object)

    with pytest.raises(KnowledgeBaseError):
        service.create_index(df=docs_dataframe)

    # if user has not specified a content field, all columns
    # besides the id field should be considered content
    knowledge_base_object.params = {
        "embedding_model": KBEmbeddingModel.DUMMY,
        "retrieval_strategy": KBRetrievalStrategy.SIMILARITY,
        "vector_database": KBVectorDatabase.CHROMADB,
        "id_field": "id",
        "content_field": None,
    }
    assert knowledge_base_object.params.content_field is None
    # create the serivce
    service = KnowledgeBaseService(knowledge_base_object)
    # create the index
    service.create_index(df=docs_dataframe)

    # check that the index was created
    # the underlying number of docs in the index should be the same as the number of docs in the dataframe
    store = service._get_or_create_vector_store()
    assert store._collection.count() == docs_dataframe.shape[0]

    # the content column should be the concatenation of all columns besides the id field
    doc = store._collection.get(ids=["1"])["documents"][0]
    assert doc == "wrong_content: content1, metadata: metadata1"


def test_no_meta_field(knowledge_base_object, docs_dataframe):
    """
    When the dataframe does not have the meta_field
    Every column besides the content_field should be considered metadata
    """
    assert knowledge_base_object.params.metadata_fields is None
    # create the serivce
    service = KnowledgeBaseService(knowledge_base_object)
    # create the index
    service.create_index(df=docs_dataframe)

    metadata = service._get_or_create_vector_store()._collection.get(ids=["1"])[
        "metadatas"
    ][0]
    assert metadata == {
        "metadata": "metadata1",
        "id": "1",
    }


def test_create_index(knowledge_base_object: KnowledgeBase, docs_dataframe):
    # create the serivce
    service = KnowledgeBaseService(knowledge_base_object)
    # create the index
    service.create_index(df=docs_dataframe)
    # check that the index was created
    # the underlying number of docs in the index should be the same as the number of docs in the dataframe
    store = service._get_or_create_vector_store()
    assert store._collection.count() == docs_dataframe.shape[0]


def test_query_index(knowledge_base_object: KnowledgeBase, docs_dataframe):
    # create the serivce
    service = KnowledgeBaseService(knowledge_base_object)
    # create the index
    service.create_index(df=docs_dataframe)

    # query the index
    result = service.query_index(query="content1", top_k=10)
    # check that the result is correct
    assert type(result) == pd.DataFrame
    assert result.shape[0] == docs_dataframe.shape[0]
    assert result.columns.tolist() == ["id", "content", "metadata", "score"]

    # query the index, with top_k=1 should just return the top result
    result = service.query_index(query="content1", top_k=1)
    assert result.shape[0] == 1


def test_update_index(knowledge_base_object: KnowledgeBase, docs_dataframe):
    # create the serivce
    service = KnowledgeBaseService(knowledge_base_object)
    # create the index
    service.create_index(df=docs_dataframe)

    # update the index
    new_docs_data_frame = docs_dataframe.append(
        {"id": "5", "content": "totally different stuff", "metadata": "metadata5"},
        ignore_index=True,
    )

    service.update_index(df=new_docs_data_frame)

    # check that the index was updated
    # the underlying number of docs in the index should be the same as the number of docs in the dataframe
    store = service._get_or_create_vector_store()
    assert store._collection.count() == new_docs_data_frame.shape[0]

    # query the new index, you should see the new doc inside of it
    result = service.query_index(query="totally different stuff", top_k=10)
    assert "5" in result["id"].tolist()


def test_delete_index(knowledge_base_object: KnowledgeBase, docs_dataframe):
    # create the serivce
    service = KnowledgeBaseService(knowledge_base_object)
    # create the index
    service.create_index(df=docs_dataframe)
    assert (
        service._get_or_create_vector_store()._collection.count()
        == docs_dataframe.shape[0]
    )

    service.delete_index()
    assert service._get_or_create_vector_store()._collection.count() == 0


def test_multiple_indexes(knowledge_base_object: KnowledgeBase, docs_dataframe):
    # create another knowledge base object
    kb2 = KnowledgeBase(
        name="test_kb2",
        collection_handle="test_kb2",
        params={
            "embedding_model": KBEmbeddingModel.DUMMY,
            "retrieval_strategy": KBRetrievalStrategy.SIMILARITY,
            "vector_database": KBVectorDatabase.CHROMADB,
            "id_field": "id",
            "content_field": "content",
            "metadata_fields": ["metadata"],
        },
    )

    # create a new doc dataframe
    new_docs_data_frame = pd.DataFrame(
        {
            "id": ["5", "6"],
            "content": ["totally different stuff", "totally different stuff 2"],
            "metadata": ["metadata5", "metadata6"],
        }
    )

    # create the serivce
    service = KnowledgeBaseService(knowledge_base_object)
    # create the index
    service.create_index(df=docs_dataframe)

    # create the serivce
    service2 = KnowledgeBaseService(kb2)
    service2.create_index(df=new_docs_data_frame)

    # check that the index was created
    # the underlying number of docs in the index should be the same as the number of docs in the dataframe
    store = service._get_or_create_vector_store()
    assert store._collection.count() == docs_dataframe.shape[0]

    store2 = service2._get_or_create_vector_store()
    assert store2._collection.count() == new_docs_data_frame.shape[0]

    # query the index
    result = service.query_index(query="content1", top_k=10)
    # check that the result is correct
    assert type(result) == pd.DataFrame
    assert result.shape[0] == docs_dataframe.shape[0]

    # query the index
    result = service2.query_index(query="totally different stuff", top_k=10)
    # check that the result is correct
    assert type(result) == pd.DataFrame
    assert result.shape[0] == new_docs_data_frame.shape[0]
