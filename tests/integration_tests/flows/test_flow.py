import unittest
from test_clickhouse import ClickhouseTest
from test_mariadb import MariaDBTest

def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(ClickhouseTest())
    suite.addTest(MariaDBTest())
    return suite


if __name__ == '__main__':
    runner = unittest.TextTestRunner()
    runner.run(test_suite())