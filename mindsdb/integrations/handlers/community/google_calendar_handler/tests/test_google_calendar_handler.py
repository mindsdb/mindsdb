import unittest
from mindsdb.integrations.handlers.google_calendar_handler.google_calendar_handler import GoogleCalendarHandler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class GoogleCalendarHandlerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {
                "credentials": "C:\\Users\\panagiotis\\Desktop\\GitHub\\mindsdb\\mindsdb\\integrations\\handlers"
                               "\\google_calendar_handler\\credentials.json",
            }
        }
        cls.handler = GoogleCalendarHandler('test_google_calendar_handler', **cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_2_select_query(self):
        query = "SELECT summary FROM calendar.events WHERE id = 1"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_3_get_columns(self):
        columns = self.handler.get_columns('id')
        assert columns.type is not RESPONSE_TYPE.ERROR

    def test_4_insert(self):
        res = self.handler.native_query(
            "INSERT INTO calendar.events VALUES (100, '2023-04-21 00:00:00', '2023-05-01 00:00:00',"
            "'summary', 'description','location', 'status', 'html_link', "
            "'creator', 'organizer', 'reminders', "
            "'timeZone', 'calendar_id', 'attendees'")
        assert res.type is not RESPONSE_TYPE.ERROR

    def test_5_update(self):
        res = self.handler.native_query("UPDATE calendar.events SET summary = 'ONE HUNDRED' WHERE id = 100")
        assert res.type is not RESPONSE_TYPE.ERROR

    def test_6_delete(self):
        res = self.handler.native_query("DELETE FROM calendar.events WHERE id = 100")
        assert res.type is not RESPONSE_TYPE.ERROR

    def test_7_delete_multiple(self):
        res = self.handler.native_query("DELETE FROM calendar.events WHERE id > 1 AND id < 20")
        assert res.type is not RESPONSE_TYPE.ERROR


if __name__ == '__main__':
    unittest.main()
