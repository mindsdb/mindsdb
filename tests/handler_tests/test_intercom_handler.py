import unittest
from mindsdb.integrations.handlers.intercom_handler.intercom_handler import IntercomHandler
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
import pandas as pd
import os


class IntercomHandlerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.handler = IntercomHandler(name="mindsdb_intercon", connection_data={'access_token': os.environ.get('INTERCOM_ACCESS_TOKEN')})

    def setUp(self):
        self.articleId = pd.DataFrame(self.handler.call_intercom_api(endpoint='/articles')['data'][0])['id'][0]
        self.conversationId = pd.DataFrame(self.handler.call_intercom_api(endpoint='/conversations')['conversations'][0])['id'][0]

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_connect(self):
        assert self.handler.connect()

    def test_2_call_intercom_api(self):
        self.assertIsInstance(self.handler.call_intercom_api(endpoint='/articles'), pd.DataFrame)

    def test_3_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_4_get_columns(self):
        articles_columns = self.handler.get_columns(table_name='articles')
        conversations_columns = self.handler.get_columns(table_name='conversations')
        assert type(articles_columns) is not RESPONSE_TYPE.ERROR
        assert type(conversations_columns) is not RESPONSE_TYPE.ERROR

    def test_5_select_articles(self):
        query = "SELECT * FROM articles"
        self.assertTrue(self.handler.native_query(query=query))

    def test_6_select_articles_by_condition(self):
        query = f"SELECT * FROM articles WHERE id = '{self.articleId}'"
        self.assertTrue(self.handler.native_query(query=query))

    def test_7_insert_article(self):
        query = '''INSERT INTO myintercom.articles (title, description, body, author_id, state, parent_id, parent_type)
                VALUES ('Thanks for everything',
                'Description of the Article',
                'Body of the Article',
                6840572,
                'published',
                6801839,
                'collection'
                );'''
        self.assertTrue(self.handler.native_query(query=query))

    def test_8_update_article(self):
        query = f'''UPDATE myintercom.articles
                SET title = 'Christmas is here!',
                    body = '<p>New gifts in store for the jolly season</p>'
                WHERE id = {self.articleId};'''
        self.assertTrue(self.handler.native_query(query=query))

    def test_9_select_conversations(self):
        query = '''SELECT *
                    FROM myintercom.conversations;'''
        self.assertTrue(self.handler.native_query(query))

    def test_9_where_conversations(self):
        query = f'''SELECT *
                FROM myintercom.conversations
                WHERE id = {self.conversationId};'''
        self.assertTrue(self.handler.native_query(query))

    def test_9_insert_conversations(self):
        query = f'''INSERT INTO myintercom.conversations (type, id, body)
                    VALUES ('user', '6547130cf077c0c9a9003ef1', 'Hello there');'''
        self.assertTrue(self.handler.native_query(query))

    def test_9_update_conversations(self):
        query = f'''UPDATE myintercom.conversations
                    SET `read` = true,
                        issue_type = 'Billing',
                        priority = 'High'
                    WHERE id = {self.conversationId};'''
        self.assertTrue(self.handler.native_query(query))


if __name__ == '__main__':
    unittest.main()
