import os
import unittest

from mindsdb.integrations.handlers.teradata_handler.teradata_handler import TeradataHandler
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE


"""
CREATE
DATABASE HR
AS PERMANENT = 60e6, -- 60MB
   SPOOL = 120e6; -- 120MB

CREATE
SET TABLE HR.Employees (
   GlobalID INTEGER,
   FirstName VARCHAR(30),
   LastName VARCHAR(30),
   DateOfBirth DATE FORMAT 'YYYY-MM-DD',
   JoinedDate DATE FORMAT 'YYYY-MM-DD',
   DepartmentCode BYTEINT
)
UNIQUE PRIMARY INDEX ( GlobalID );

INSERT INTO HR.Employees (GlobalID,
                          FirstName,
                          LastName,
                          DateOfBirth,
                          JoinedDate,
                          DepartmentCode)
VALUES (101,
        'Adam',
        'Tworkowski',
        '1980-01-05',
        '2004-08-01',
        01);
"""


class TeradataHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "host": os.environ.get('TERADATA_HOST', 'localhost'),
            "user": "dbc",
            "password": "dbc",
            "database": "HR"
        }
        cls.handler = TeradataHandler('test_teradata_handler', cls.kwargs)

    def test_0_connect(self):
        assert self.handler.connect()

    def test_1_check_connection(self):
        assert self.handler.check_connection().success is True

    def test_2_get_columns(self):
        assert self.handler.get_columns('Employees').resp_type is not RESPONSE_TYPE.ERROR

    def test_3_get_tables(self):
        assert self.handler.get_tables().resp_type is not RESPONSE_TYPE.ERROR

    def test_4_select_query(self):
        query = 'SELECT * FROM HR.Employees WHERE GlobalID=101'
        assert self.handler.query(query).resp_type is RESPONSE_TYPE.TABLE


if __name__ == "__main__":
    unittest.main(failfast=True)
