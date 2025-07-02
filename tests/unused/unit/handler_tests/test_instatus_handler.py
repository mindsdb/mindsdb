import unittest
from mindsdb.integrations.handlers.instatus_handler.instatus_handler import InstatusHandler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
import pandas as pd
import os


class InstatusHandlerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.handler = InstatusHandler(name='mindsdb_instatus', connection_data={'api_key': os.environ.get('INSTATUS_API_KEY')})

    def setUp(self):
        self.pageId = self.handler.call_instatus_api(endpoint='/v2/pages')['id'][0]
        self.componentId = self.handler.call_instatus_api(endpoint=f'/v1/{self.pageId}/components')['id'][0]

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_call_instatus_api(self):
        self.assertIsInstance(self.handler.call_instatus_api(endpoint='/v2/pages'), pd.DataFrame)

    def test_2_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_3_get_columns(self):
        status_pages_columns = self.handler.get_columns(table_name='status_pages')
        components_columns = self.handler.get_columns(table_name='components')
        assert type(status_pages_columns) is not RESPONSE_TYPE.ERROR
        assert type(components_columns) is not RESPONSE_TYPE.ERROR

    def test_4_select_status_pages(self):
        query = '''SELECT *
                    FROM mindsdb_instatus.status_pages'''
        self.assertTrue(self.handler.native_query(query))

    def test_5_select_status_pages_by_conditions(self):
        query = '''SELECT name, status, subdomain
                    FROM mindsdb_instatus.status_pages
                    WHERE id = "clo3xshsk1114842hkn377y3lrap"'''
        self.assertTrue(self.handler.native_query(query))

    def test_6_insert_status_pages(self):
        query = f'''INSERT INTO mindsdb_instatus.status_pages (email, name, subdomain, components, logoUrl) VALUES ('{os.environ.get('EMAIL_ID')}', 'mindsdb', 'somtirtha-roy', '["Website", "App", "API"]', 'https://instatus.com/sample.png')'''
        try:
            self.assertTrue(self.handler.native_query(query))
        except Exception as e:
            error_message = str(e)
            if "This subdomain is taken by another status page" in error_message:
                print("Subdomain is already taken. Choose a different one.")

    def test_7_update_status_pages(self):
        # update the row with the id obtained
        query = f'''UPDATE mindsdb_instatus.status_pages
                SET logoUrl = 'https://instatus.com/sample.png',
                    faviconUrl = 'https://instatus.com/favicon-32x32.png',
                    websiteUrl = 'https://instatus.com',
                    language = 'en',
                    useLargeHeader = true,
                    brandColor = '#111',
                    okColor = '#33B17E',
                    disruptedColor = '#FF8C03',
                    degradedColor = '#ECC94B',
                    downColor = '#DC123D',
                    noticeColor = '#70808F',
                    unknownColor = '#DFE0E1',
                    googleAnalytics = 'UA-00000000-1',
                    subscribeBySms = true,
                    smsService = 'twilio',
                    twilioSid = 'YOUR_TWILIO_SID',
                    twilioToken = 'YOUR_TWILIO_TOKEN',
                    twilioSender = 'YOUR_TWILIO_SENDER',
                    nexmoKey = null,
                    nexmoSecret = null,
                    nexmoSender = null,
                    htmlInMeta = null,
                    htmlAboveHeader = null,
                    htmlBelowHeader = null,
                    htmlAboveFooter = null,
                    htmlBelowFooter = null,
                    htmlBelowSummary = null,
                    cssGlobal = null,
                    launchDate = null,
                    dateFormat = 'MMMMMM d, yyyy',
                    dateFormatShort = 'MMM yyyy',
                    timeFormat = 'p',
                    private = false,
                    useAllowList = false,
                    translations = '{{
                    "name": {{
                        "fr": "nasa"
                        }}
                    }}'
                WHERE id = "{self.pageId}"'''
        self.assertTrue(self.handler.native_query(query))

    def test_8_select_components(self):
        query = f'''SELECT *
                    FROM mindsdb_instatus.components
                    WHERE page_id = '{self.pageId}';'''
        self.assertTrue(self.handler.native_query(query))

    def test_9_select_components_by_conditions(self):
        query = f'''SELECT *
                    FROM mindsdb_instatus.components
                    WHERE page_id = '{self.pageId}'
                    AND component_id = '{self.componentId}';'''
        self.assertTrue(self.handler.native_query(query))

    def test_10_insert_components(self):
        query = f'''INSERT INTO mindsdb_instatus.components (page_id, name, description, status, order, showUptime, grouped, translations_name_in_fr, translations_desc_in_fr)
                    VALUES (
                        '{self.pageId}',
                        'Test component',
                        'Testing',
                        'OPERATIONAL',
                        6,
                        true,
                        false,
                        "Composant de test",
                        "En test"
                    );'''
        self.assertTrue(self.handler.native_query(query))

    def test_11_update_components(self):
        query = f'''UPDATE mindsdb_instatus.components
                    SET
                        name = 'Test component 4',
                        description = 'Test test test',
                        status = 'OPERATIONAL',
                        order = 6,
                        showUptime = true,
                        grouped = false,
                        translations_name_in_fr = "Composant de test 4",
                        translations_desc_in_fr = "Test test test"
                    WHERE page_id = '{self.pageId}'
                    AND component_id = '{self.componentId}';'''
        self.assertTrue(self.handler.native_query(query))


if __name__ == '__main__':
    unittest.main()
