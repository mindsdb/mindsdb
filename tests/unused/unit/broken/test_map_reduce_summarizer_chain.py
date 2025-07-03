from unittest.mock import AsyncMock, MagicMock

import pandas as pd
from langchain.chains.combine_documents.map_reduce import MapReduceDocumentsChain
from langchain_core.documents import Document

from mindsdb.integrations.libs.vectordatabase_handler import VectorStoreHandler
from mindsdb.integrations.utilities.rag.chains.map_reduce_summarizer_chain import MapReduceSummarizerChain
from mindsdb.integrations.utilities.rag.settings import SummarizationConfig
from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator


class TestMapReduceSummarizerChain:
    def test_summarizes_documents(self):
        mock_vector_store_handler = MagicMock(spec=VectorStoreHandler, wraps=VectorStoreHandler)
        mock_vector_store_handler.select.side_effect = [
            pd.DataFrame.from_records([
                {'content': 'Chunk 1'},
                {'content': 'Chunk 2'},
            ]),
            pd.DataFrame.from_records([
                {'content': 'Chunk 3'}
            ])
        ]
        mock_map_reduce_documents_chain = AsyncMock(spec=MapReduceDocumentsChain, wraps=MapReduceDocumentsChain)
        mock_map_reduce_documents_chain.ainvoke.side_effect = [{'output_text': 'Final summary 1'}, {'output_text': 'Final summary 2'}]
        test_summarizer_chain = MapReduceSummarizerChain(
            vector_store_handler=mock_vector_store_handler,
            map_reduce_documents_chain=mock_map_reduce_documents_chain,
            summarization_config=SummarizationConfig()
        )

        chain_input = {
            'context': [
                Document(page_content='Chunk 1', metadata={'original_row_id': '1'}),
                Document(page_content='Chunk 2', metadata={'original_row_id': '1'}),
                Document(page_content='Chunk 3', metadata={'original_row_id': '2'})
            ],
            'question': 'What is the answer to life?',
        }
        actual_chain_output = test_summarizer_chain.invoke(chain_input)

        # Make sure we select from the vector store correctly.
        mock_vector_store_handler.select.assert_any_call(
            'embeddings',
            columns=['content', 'metadata'],
            conditions=[FilterCondition(
                "metadata->>'original_row_id'",
                FilterOperator.EQUAL,
                '1'
            )]
        )
        mock_vector_store_handler.select.assert_any_call(
            'embeddings',
            columns=['content', 'metadata'],
            conditions=[FilterCondition(
                "metadata->>'original_row_id'",
                FilterOperator.EQUAL,
                '2'
            )]
        )

        # Make sure we are calling the summarization chain with the right chunks.
        mock_map_reduce_documents_chain.ainvoke.assert_awaited()

        # Make sure the summary is actually added to the context.
        expected_chain_output = {
            'context': [
                Document(page_content='Chunk 1', metadata={'original_row_id': '1', 'summary': 'Final summary 1'}),
                Document(page_content='Chunk 2', metadata={'original_row_id': '1', 'summary': 'Final summary 1'}),
                Document(page_content='Chunk 3', metadata={'original_row_id': '2', 'summary': 'Final summary 2'})
            ],
            'question': 'What is the answer to life?',
        }

        assert actual_chain_output == expected_chain_output
