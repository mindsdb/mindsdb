import unittest
from mindsdb.integrations.handlers.instatus_handler.instatus_handler import InstatusHandler
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
import pandas as pd
import os


class InstatusHandlerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.handler = InstatusHandler(name='mindsdb_instatus', connection_data={'api_key': os.environ.get('INSTATUS_API_KEY')})

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_call_instatus_api(self):
        self.assertIsInstance(self.handler.call_instatus_api(endpoint='/v2/pages'), pd.DataFrame)

    def test_2_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_3_get_columns(self):
        columns = self.handler.get_columns(table_name='status_pages')
        assert type(columns) is not RESPONSE_TYPE.ERROR

    def test_4_select(self):
        query = '''SELECT *
                    FROM mindsdb_instatus.status_pages'''
        self.assertTrue(self.handler.native_query(query))

    def test_5_select_by_conditions(self):
        query = '''SELECT name, status, subdomain
                    FROM mindsdb_instatus.status_pages
                    WHERE id = "clo3xshsk1114842hkn377y3lrap"'''
        self.assertTrue(self.handler.native_query(query))

    def test_6_insert(self):
        query = f'''INSERT INTO mindsdb_instatus.status_pages (email, name, subdomain, components, logoUrl) VALUES ('{os.environ.get('EMAIL_ID')}', 'mindsdb', 'somtirtha-roy', '["Website", "App", "API"]', 'https://instatus.com/sample.png')'''
        try:
            self.assertTrue(self.handler.native_query(query))
        except Exception as e:
            error_message = str(e)
            if "This subdomain is taken by another status page" in error_message:
                print("Subdomain is already taken. Choose a different one.")

    def test_7_update(self):
        # get the id of the row to be updated
        _id = self.handler.call_instatus_api(endpoint='/v2/pages')['id'][0]
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
                WHERE id = "{_id}"'''
        self.assertTrue(self.handler.native_query(query))


if __name__ == '__main__':
    unittest.main()
