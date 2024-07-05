import sys
import types

from unittest.mock import patch
import datetime as dt

from tests.unit.executor_test_base import BaseExecutorDummyML

from dataclasses import dataclass


# import module virtually if it is not installed
try:
    import github  # noqa
except ImportError:
    module = types.ModuleType('github')
    exec('Github=None', module.__dict__)
    sys.modules['github'] = module


class TestApiHandler(BaseExecutorDummyML):

    @patch('github.Github')
    def test_github(self, Github):
        """
        Test for APIResource
        """

        # create
        self.run_sql('''
             CREATE DATABASE gh
                WITH
                  ENGINE = 'github',
                  PARAMETERS = {
                    "repository": "mindsdb/mindsdb",
                    "api_key": "-"
                  }
        ''')

        # select
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

        # insert
        self.run_sql('''
            insert into gh.issues (title, body)
             values ('feature', 'do better')
        ''')
        create_issue = Github().get_repo().create_issue
        args = create_issue.call_args_list[0][0]
        kwargs = create_issue.call_args_list[0][1]

        assert args[0] == 'feature'
        assert kwargs['body'] == 'do better'
