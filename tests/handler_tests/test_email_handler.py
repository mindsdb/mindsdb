import os
import pytest
from unittest.mock import MagicMock

from mindsdb_sql import parse_sql
from mindsdb.integrations.handlers.email_handler.email_tables import EmailsTable
from mindsdb.integrations.handlers.email_handler.email_handler import EmailHandler


class TestEmailHandler:
    def setup_class(self):
        # Check if env variables exist, if not fail the test
        email = os.getenv("EMAIL_USERNAME")
        password = os.getenv("EMAIL_PASSWORD")
        assert email is not None, "EMAIL_USERNAME environment variable not found e.g. example@gmail.com"
        assert password is not None, "EMAIL_PASSWORD environment variable not found"

        self.connection_data = {"email": email, "password": password}
        self.email_handler = EmailHandler(connection_data=self.connection_data)
        self.email_handler.connect()
        self.emails_table_instance = EmailsTable(self.email_handler)

    def test_connect_already_connected(self):
        self.email_handler.is_connected = True
        connection = self.email_handler.connect()
        assert connection is self.email_handler.connection, "The connection must be the same as the one in the handler."

    def test_check_connection(self):

        response = self.email_handler.check_connection()
        assert response.success is True, "The response success must be True."

    def test_select(self):
        """
        Test the select method of EmailsTable Class
        """

        self.emails_table_instance.handler.connection.search_email = MagicMock()

        query = parse_sql('SELECT * FROM emails limit 1')

        self.emails_table_instance.select(query)

        assert self.emails_table_instance.handler.connection.search_email.called, ("The search_email "
                                                                                   "method must be called.")

        # select using invalid column should raise Exception
        query = parse_sql('SELECT invalid_column FROM emails limit 1')

        with pytest.raises(Exception):
            self.emails_table_instance.select(query)

    def test_insert(self):
        """
        Test the insert method of EmailsTable Class
        """

        self.emails_table_instance.handler.connection.send_email = MagicMock()

        query = parse_sql(
            'INSERT INTO email_datasource.emails(to_field, subject, body) '
            'VALUES ("toemail@email.com", "MindsDB", "Hello from MindsDB!")')

        self.emails_table_instance.insert(query)
        assert self.emails_table_instance.handler.connection.send_email.called, "The send_email method must be called."

        # insert using invalid column should raise Exception
        query = parse_sql(
            'INSERT INTO email_datasource.emails(to_field, subject, body, invalid_column) '
            'VALUES ("toemail@email.com", "MindsDB", "blaha" , "invalid")')

        with pytest.raises(Exception):
            self.emails_table_instance.insert(query)

    def test_get_columns(self):
        """
        Test the get_columns method of EmailsTable Class
        """

        columns = self.emails_table_instance.get_columns()
        assert isinstance(columns, list), "The returned value must be a list."
        assert 'id' in columns, "Column 'id' must be in the columns list."
        assert 'body' in columns, "Column 'body' must be in the columns list."
        assert 'subject' in columns, "Column 'subject' must be in the columns list."
        assert 'to_field' in columns, "Column 'to_field' must be in the columns list."
        assert 'from_field' in columns, "Column 'from_field' must be in the columns list."
        assert 'datetime' in columns, "Column 'datetime' must be in the columns list."
