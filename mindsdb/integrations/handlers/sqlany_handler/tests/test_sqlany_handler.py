import os
import unittest

from mindsdb.integrations.handlers.sqlany_handler.sqlany_handler import SQLAnyHandler
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE


"""
create table TEST
(
    ID          INTEGER not null,
    NAME        NVARCHAR(1),
    DESCRIPTION NVARCHAR(1)
);

create unique index TEST_ID_INDEX
    on TEST (ID);

alter table TEST
    add constraint TEST_PK
        primary key (ID);

insert into TEST
values (1, 'h', 'w');
"""


class SQLAnyHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "host": os.environ.get('SQLANY_HOST', 'localhost'),
            "port": os.environ.get('SQLANY_PORT', 55505),
            "user": "DBA",
            "password": os.environ.get('SQLANY_PASSWORD','password'),
            "server": "TestMe",
            "database": "MINDSDB"
        }
        cls.handler = SQLAnyHandler('test_sqlany_handler', cls.kwargs)

    def test_0_connect(self):
        assert self.handler.connect()

    def test_1_check_connection(self):
        assert self.handler.check_connection().success is True

    def test_2_get_columns(self):
        assert self.handler.get_columns('TEST').resp_type is not RESPONSE_TYPE.ERROR

    def test_3_get_tables(self):
        assert self.handler.get_tables().resp_type is not RESPONSE_TYPE.ERROR

    def test_4_select_query(self):
        query = 'SELECT * FROM TEST WHERE ID=2'
        assert self.handler.query(query).resp_type is RESPONSE_TYPE.TABLE

    def test_5_update_query(self):
        query = 'UPDATE TEST SET NAME=\'s\' WHERE ID=1'
        assert self.handler.query(query).resp_type is RESPONSE_TYPE.OK


if __name__ == "__main__":
    unittest.main(failfast=True)
