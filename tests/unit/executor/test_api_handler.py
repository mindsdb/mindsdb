import sys
import types

from unittest.mock import patch
import datetime as dt

import pandas as pd

from tests.unit.executor_test_base import BaseExecutorDummyML

from dataclasses import dataclass


# import modules virtually if it is not installed
try:
    import github  # noqa
except ImportError:
    module = types.ModuleType('')
    exec('Github=None', module.__dict__)
    sys.modules['github'] = module

try:
    import chardet  # noqa
except ImportError:
    sys.modules['chardet'] = types.ModuleType('')


class TestApiHandler(BaseExecutorDummyML):

    @patch('github.Github')
    def test_github(self, Github):
        """
        Test for APIResource
        """

        # --- create ---
        self.run_sql('''
             CREATE DATABASE gh
                WITH
                  ENGINE = 'github',
                  PARAMETERS = {
                    "repository": "mindsdb/mindsdb",
                    "api_key": "-"
                  }
        ''')

        # --- select ---
        @dataclass
        class User:
            login: str = 'user1'

        @dataclass
        class Issue:
            number: int
            title: str
            state = 'open'
            user = User()
            labels = []
            assignees = [User()]
            comments: int = 0
            body = 'body'
            created_at = dt.datetime.now()
            updated_at = dt.datetime.now()
            closed_at = dt.datetime.now()
            closed_by = User()

        data = [
            [123, 'bug', 'open'],
            [124, 'feature', 'open'],
            [125, 'feature', 'open'],
        ]

        get_issues = Github().get_repo().get_issues

        get_issues.return_value = [Issue(*row) for row in data]

        ret = self.run_sql('''
            select max(number) number, title from gh.issues
            where state = 'open'
              and number between 124 and 126
            group by title
        ''')

        # state was used for github
        kwargs = get_issues.call_args_list[0][1]
        assert kwargs['state'] == 'open'

        # between was used outside of handler, output is only one row with number=125
        assert len(ret) == 1
        assert ret['number'][0] == 125

        # --- insert ---
        self.run_sql('''
            insert into gh.issues (title, body)
             values ('feature', 'do better')
        ''')
        create_issue = Github().get_repo().create_issue
        args = create_issue.call_args_list[0][0]
        kwargs = create_issue.call_args_list[0][1]

        assert args[0] == 'feature'
        assert kwargs['body'] == 'do better'

    @patch("mindsdb.integrations.handlers.email_handler.email_handler.EmailClient")
    def test_email(self, EmailClient):
        """
        Test for APITable
        """

        # --- create ---
        self.run_sql('''
            CREATE DATABASE em
                WITH ENGINE = 'email',
                PARAMETERS = {
                  "email": "e@mail.com",
                  "password": "-"
            }
        ''')

        # --- select ---
        search_email = EmailClient().search_email

        mock_df = pd.DataFrame({
            'date': ["Wed, 02 Feb 2022 15:30:00 +0000",
                     "Thu, 10 Mar 2022 10:45:15 +0530",
                     "Fri, 16 Dec 2022 20:15:30 -0400"
                     ],
            'body_content_type': ['text', 'text', 'text'],
            "body": ["info", "info", "info"],
            "from_field": ["x1@m.com", "x2@m.com", "x3@m.com"],
            "id": ["2", "3", "4"],
            "to_field": ["a@m.com", "a@m.com", "b@m.com"],
            "subject": ["info", "info", "info"],
        })

        search_email.return_value = mock_df

        ret = self.run_sql('''
            SELECT to_field, max(from_field) from_field
            FROM em.emails
            WHERE subject = 'info'
             and id > 1
            group by to_field
            order by to_field
        ''')

        args = search_email.call_args_list[0][0]

        # check input to search_email
        assert args[0].subject == 'info'
        assert args[0].since_email_id == 2

        # check response
        assert len(ret) == 2
        assert ret['from_field'][0] == 'x2@m.com'

        # --- insert ---
        self.run_sql('''
          INSERT INTO em.emails(to_field, subject, body)
          VALUES ("toemail@email.com", "MindsDB", "Hello from MindsDB!");
        ''')

        func_call = EmailClient().send_email.call_args_list[0]
        args = func_call[0]
        kwargs = func_call[1]

        assert args[0] == "toemail@email.com"
        assert kwargs['subject'] == 'MindsDB'
        assert kwargs['body'] == "Hello from MindsDB!"
