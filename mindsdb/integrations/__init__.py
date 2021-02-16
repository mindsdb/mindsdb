from .clickhouse.clickhouse import ClickhouseConnectionChecker
from .mariadb.mariadb import MariadbConnectionChecker
from .mongodb.mongodb import MongoConnectionChecker
from .mssql.mssql import MSSQLConnectionChecker
from .mysql.mysql import MySQLConnectionChecker
from .postgres.postgres import PostgreSQLConnectionChecker

CHECKERS = {
        "clickhouse": ClickhouseConnectionChecker,
        "mariadb": MariadbConnectionChecker,
        "mongodb": MongoConnectionChecker,
        "mssql": MSSQLConnectionChecker,
        "mysql": MySQLConnectionChecker,
        "postgres": PostgreSQLConnectionChecker
        }
