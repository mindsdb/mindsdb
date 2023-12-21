import unittest
from mindsdb.integrations.handlers.intercom_handler.intercom_handler import IntercomHandler
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
import pandas as pd
import os


class InstatusHandlerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.handler = IntercomHandler(name="mindsdb_intercon", connection_data={'access_token': os.environ.get('INTERCOM_ACCESS_TOKEN')})

    def setUp(self):
        df = pd.DataFrame(self.handler.call_intercom_api(endpoint='/articles', params={'page': 1, 'per_page': 1})['data'][0])
        self.articleId = df['id'][0]
        self.adminId = self.handler.call_intercom_api(endpoint='/admins')['admins'][0][0]['id']

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_connect(self):
        assert self.handler.connect()

    def test_2_call_instatus_api(self):
        self.assertIsInstance(self.handler.call_intercom_api(endpoint='/articles'), pd.DataFrame)

    def test_3_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_4_get_columns(self):
        columns = self.handler.get_columns(table_name='articles')
        assert type(columns) is not RESPONSE_TYPE.ERROR

    def test_5_select_articles(self):
        query = "SELECT * FROM articles"
        self.assertTrue(self.handler.native_query(query=query))

    def test_6_select_articles_by_condition(self):
        query = f"SELECT * FROM articles WHERE id = {self.articleId}"
        self.assertTrue(self.handler.native_query(query=query))

    def test_7_insert_article(self):
        query = f'''INSERT INTO myintercom.articles (title, description, body, author_id, state, parent_type)
                VALUES ('Thanks for everything',
                'Description of the Article',
                'Body of the Article',
                {self.adminId},
                'published',
                'collection'
                );'''
        self.assertTrue(self.handler.native_query(query=query))

    def test_8_update_article(self):
        query = f'''UPDATE myintercom.articles
                SET title = 'Christmas is here!',
                    body = '<p>New gifts in store for the jolly season</p>'
                WHERE id = {self.articleId};'''
        self.assertTrue(self.handler.native_query(query=query))

    def test_9_select_admin(self):
        query = f'''SELECT name, email
                FROM admins
                WHERE id = {self.adminId}'''
        self.assertTrue(self.handler.native_query(query))


if __name__ == '__main__':
    unittest.main()
