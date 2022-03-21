from .clickhouse.clickhouse import ClickhouseConnectionChecker
from .mariadb.mariadb import MariaDS
from .mongodb.mongodb import MongoConnectionChecker
from .mssql.mssql import MSSQLConnectionChecker
from .mysql.mysql import MySqlDS
from .postgres.postgres import PostgresDS
from .redis.redisdb import RedisConnectionChecker
from .kafka.kafkadb import KafkaConnectionChecker
from .snowflake.snowflake import SnowflakeConnectionChecker
from .trinodb.trinodb import TrinodbConnectionChecker

try:
    from .scylladb.scylladb import ScyllaDBConnectionChecker
except ImportError:
    ScyllaDBConnectionChecker = None
try:
    from .cassandra.cassandra import CassandraConnectionChecker
except ImportError:
    CassandraConnectionChecker = None


CHECKERS = {
    "clickhouse": ClickhouseConnectionChecker,
    "mariadb": MariaDS,
    "mongodb": MongoConnectionChecker,
    "mssql": MSSQLConnectionChecker,
    "mysql": MySqlDS,
    "singlestore": MySqlDS,
    "postgres": PostgresDS,
    "cockroachdb": PostgresDS,
    "redis": RedisConnectionChecker,
    "kafka": KafkaConnectionChecker,
    "snowflake": SnowflakeConnectionChecker,
    "trinodb": TrinodbConnectionChecker
}


if ScyllaDBConnectionChecker is not None:
    CHECKERS['scylladb'] = ScyllaDBConnectionChecker

if CassandraConnectionChecker is not None:
    CHECKERS['cassandra'] = CassandraConnectionChecker
