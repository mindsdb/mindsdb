import unittest
from mindsdb.api.executor.data_types.response_type import (
    RESPONSE_TYPE,
)
from mindsdb.integrations.handlers.lancedb_handler.lancedb_handler import (
    LanceDBHandler,
)
from mindsdb.integrations.libs.vectordatabase_handler import (
    FilterCondition,
    FilterOperator,
)
import pandas as pd


class LanceDBHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {'connection_data': {'persist_directory': '~/lancedb'}}
        cls.handler = LanceDBHandler('test_lancedb_handler', **cls.kwargs)

    def test_0_connect(self):
        self.handler.connect()

    def test_1_check_connection(self):
        self.handler.check_connection()

    def test_2_create_table(self):
        res = self.handler.create_table('test_data17')
        assert res.resp_type is not RESPONSE_TYPE.ERROR

    def test_3_insert_into_table(self):
        data = [{'id': 'id1', 'content': 'this is a test', 'metadata': {'test': 'test1'}, 'embeddings': [1, 2, 3, 4, 3, 5, 2, 8]},
                {'id': 'id2', 'content': 'this is a test', 'metadata': {'test': 'test2'}, 'embeddings': [4, 2, 7, 4, 2, 5, 2, 9]},
                {'id': 'id3', 'content': 'this is a test', 'metadata': {'test': 'test3'}, 'embeddings': [5, 2, 3, 2, 3, 3, 2, 7]},
                {'id': 'id3', 'content': 'this is a test', 'metadata': {'test': 'test4'}, 'embeddings': [5, 2, 3, 2, 3, 4, 2, 7]}]
        df = pd.DataFrame(data)
        res = self.handler.insert('test_data17', df, None)
        assert res.resp_type is not RESPONSE_TYPE.ERROR

    def test_4_select(self):
        res = self.handler.select(
            'test_data17',
            ['id', 'content', 'metadata', 'embeddings'],
            [
                FilterCondition(
                    column="id",
                    op=FilterOperator.EQUAL,
                    value="id3",
                )
            ],
            None,
            None,
        )
        assert res.resp_type is RESPONSE_TYPE.TABLE

    def test_5_vector_distance(self):
        res = self.handler.select(
            'test_data17',
            ['id', 'content', 'metadata', 'embeddings'],
            [
                FilterCondition(
                    column="search_vector",
                    op=FilterOperator.EQUAL,
                    value="[4.0, 2.0, 7.0, 4.0, 2.0, 5.0, 2.0, 9.0]",
                )
            ],
            None,
            None,
        )
        assert res.resp_type is RESPONSE_TYPE.TABLE and 'distance' in res.data_frame.columns

    def test_6_delete(self):
        res = self.handler.delete(
            'test_data17',
            [
                FilterCondition(
                    column="id",
                    op=FilterOperator.EQUAL,
                    value="id1",
                )
            ]
        )
        assert res.resp_type is not RESPONSE_TYPE.ERROR

    def test_7_describe_table(self):
        res = self.handler.get_columns('test_data17')
        assert res.resp_type is RESPONSE_TYPE.TABLE

    def test_8_get_tables(self):
        res = self.handler.get_tables()
        assert res.resp_type is not RESPONSE_TYPE.ERROR

    def test_9_drop_table(self):
        res = self.handler.drop_table('test_data17')
        assert res.resp_type is not RESPONSE_TYPE.ERROR


if __name__ == '__main__':
    unittest.main()
