from .clickhouse.clickhouse import ClickhouseConnectionChecker
from .mariadb.mariadb import MariadbConnectionChecker
from .mongodb.mongodb import MongoConnectionChecker
from .mssql.mssql import MSSQLConnectionChecker
from .mysql.mysql import MySQLConnectionChecker
from .postgres.postgres import PostgreSQLConnectionChecker
from .redis.redisdb import RedisConnectionChecker
from .kafka.kafkadb import KafkaConnectionChecker
from .snowflake.snowflake import SnowflakeConnectionChecker


try:
    from .scylladb.scylladb import ScyllaDBConnectionChecker
except ImportError:
    print("ScyllaDB Datasource is not available by default. If you wish to use it, please install mindsdb_datasources[scylla]")
    ScyllaDBConnectionChecker = None
try:
    from .cassandra.cassandra import CassandraConnectionChecker
except ImportError:
    print("Cassandra Datasource is not available by default. If you wish to use it, please install mindsdb_datasources[cassandra]")
    CassandraConnectionChecker = None


CHECKERS = {
    "clickhouse": ClickhouseConnectionChecker,
    "mariadb": MariadbConnectionChecker,
    "mongodb": MongoConnectionChecker,
    "mssql": MSSQLConnectionChecker,
    "mysql": MySQLConnectionChecker,
    "singlestore": MySQLConnectionChecker,
    "postgres": PostgreSQLConnectionChecker,
    "cockroachdb": PostgreSQLConnectionChecker,
    "redis": RedisConnectionChecker,
    "kafka": KafkaConnectionChecker,
    "snowflake": SnowflakeConnectionChecker
}


if ScyllaDBConnectionChecker is not None:
    CHECKERS['scylladb'] = ScyllaDBConnectionChecker

if CassandraConnectionChecker is not None:
    CHECKERS['cassandra'] = CassandraConnectionChecker
