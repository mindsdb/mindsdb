"""
*******************************************************
 * Copyright (C) 2017 MindsDB Inc. <copyright@mindsdb.com>
 *
 * This file is part of MindsDB Server.
 *
 * MindsDB Server can not be copied and/or distributed without the express
 * permission of MindsDB Inc
 *******************************************************
"""

import enum
from dataclasses import dataclass, field

# CAPABILITIES
# As defined in : https://dev.mysql.com/doc/dev/mysql-server/8.0.0/group__group__cs__capabilities__flags.html

MAX_PACKET_SIZE = 16777215


# capabilities description can be found on page 67 https://books.google.ru/books?id=5TjrxYHRAwEC&printsec=frontcover#v=onepage&q&f=false
# https://mariadb.com/kb/en/connection/
# https://dev.mysql.com/doc/internals/en/capability-flags.html
class CAPABILITIES(object):
    __slots__ = ()
    CLIENT_LONG_PASSWORD = 1
    CLIENT_FOUND_ROWS = 2
    CLIENT_LONG_FLAG = 4
    CLIENT_CONNECT_WITH_DB = 8
    CLIENT_NO_SCHEMA = 16
    CLIENT_COMPRESS = 32
    CLIENT_ODBC = 64
    CLIENT_LOCAL_FILES = 128
    CLIENT_IGNORE_SPACE = 256
    CLIENT_PROTOCOL_41 = 512
    CLIENT_INTERACTIVE = 1024
    CLIENT_SSL = 2048
    CLIENT_IGNORE_SIGPIPE = 4096
    CLIENT_TRANSACTIONS = 8192
    CLIENT_RESERVED = 16384
    CLIENT_RESERVED2 = 32768
    CLIENT_MULTI_STATEMENTS = 1 << 16
    CLIENT_MULTI_RESULTS = 1 << 17
    CLIENT_PS_MULTI_RESULTS = 1 << 18
    CLIENT_PLUGIN_AUTH = 1 << 19
    CLIENT_CONNECT_ATTRS = 1 << 20
    CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 1 << 21
    CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS = 1 << 22
    CLIENT_SESSION_TRACK = 1 << 23
    CLIENT_DEPRECATE_EOF = 1 << 24
    CLIENT_SSL_VERIFY_SERVER_CERT = 1 << 30
    CLIENT_REMEMBER_OPTIONS = 1 << 31
    CLIENT_SECURE_CONNECTION = 0x00008000


CAPABILITIES = CAPABILITIES()


# SERVER STATUS
class SERVER_STATUS(object):
    __slots__ = ()
    SERVER_STATUS_IN_TRANS = 1  # A transaction is currently active
    SERVER_STATUS_AUTOCOMMIT = 2  # Autocommit mode is set
    SERVER_MORE_RESULTS_EXISTS = 8  # more results exists (more packet follow)
    SERVER_QUERY_NO_GOOD_INDEX_USED = 16
    SERVER_QUERY_NO_INDEX_USED = 32
    SERVER_STATUS_CURSOR_EXISTS = (
        64  # when using COM_STMT_FETCH, indicate that current cursor still has result (deprecated)
    )
    SERVER_STATUS_LAST_ROW_SENT = (
        128  # when using COM_STMT_FETCH, indicate that current cursor has finished to send results (deprecated)
    )
    SERVER_STATUS_DB_DROPPED = 1 << 8  # database has been dropped
    SERVER_STATUS_NO_BACKSLASH_ESCAPES = 1 << 9  # current escape mode is "no backslash escape"
    SERVER_STATUS_METADATA_CHANGED = (
        1 << 10
    )  # A DDL change did have an impact on an existing PREPARE (an automatic reprepare has been executed)
    SERVER_QUERY_WAS_SLOW = 1 << 11
    SERVER_PS_OUT_PARAMs = 1 << 12  # this resultset contain stored procedure output parameter
    SERVER_STATUS_IN_TRANS_READONLY = 1 << 13  # current transaction is a read-only transaction
    SERVER_SESSION_STATE_CHANGED = 1 << 14  # session state change. see Session change type for more information


SERVER_STATUS = SERVER_STATUS()


# COMMANDS
class COMMANDS(object):
    __slots__ = ()
    COM_CHANGE_USER = int("0x11", 0)
    COM_DEBUG = int("0x0D", 0)
    COM_INIT_DB = int("0x02", 0)
    COM_PING = int("0x0e", 0)
    COM_PROCESS_KILL = int("0xC", 0)
    COM_QUERY = int("0x03", 0)
    COM_QUIT = int("0x01", 0)
    COM_RESET_CONNECTION = int("0x1f", 0)
    COM_SET_OPTION = int("0x1b", 0)
    COM_SHUTDOWN = int("0x0a", 0)
    COM_SLEEP = int("0x00", 0)
    COM_STATISTICS = int("0x09", 0)
    COM_STMT_PREPARE = int("0x16", 0)
    COM_STMT_EXECUTE = int("0x17", 0)
    COM_STMT_FETCH = int("0x1c", 0)
    COM_STMT_RESET = int("0x1a", 0)
    COM_STMT_CLOSE = int("0x19", 0)
    COM_FIELD_LIST = int("0x04", 0)  # deprecated


COMMANDS = COMMANDS()


# FIELD TYPES
# https://dev.mysql.com/doc/dev/mysql-server/latest/field__types_8h_source.html
# https://mariadb.com/kb/en/result-set-packets/
class TYPES(object):
    __slots__ = ()
    MYSQL_TYPE_DECIMAL = 0
    MYSQL_TYPE_TINY = 1
    MYSQL_TYPE_SHORT = 2
    MYSQL_TYPE_LONG = 3
    MYSQL_TYPE_FLOAT = 4
    MYSQL_TYPE_DOUBLE = 5
    MYSQL_TYPE_NULL = 6
    MYSQL_TYPE_TIMESTAMP = 7
    MYSQL_TYPE_LONGLONG = 8
    MYSQL_TYPE_INT24 = 9
    MYSQL_TYPE_DATE = 10
    MYSQL_TYPE_TIME = 11
    MYSQL_TYPE_DATETIME = 12
    MYSQL_TYPE_YEAR = 13
    MYSQL_TYPE_NEWDATE = 14
    MYSQL_TYPE_VARCHAR = 15
    MYSQL_TYPE_BIT = 16
    MYSQL_TYPE_TIMESTAMP2 = 17
    MYSQL_TYPE_DATETIME2 = 18
    MYSQL_TYPE_TIME2 = 19
    MYSQL_TYPE_TYPED_ARRAY = 20
    MYSQL_TYPE_VECTOR = 242
    MYSQL_TYPE_INVALID = 243
    MYSQL_TYPE_BOOL = 244
    MYSQL_TYPE_JSON = 245
    MYSQL_TYPE_NEWDECIMAL = 246
    MYSQL_TYPE_ENUM = 247
    MYSQL_TYPE_SET = 248
    MYSQL_TYPE_TINY_BLOB = 249
    MYSQL_TYPE_MEDIUM_BLOB = 250
    MYSQL_TYPE_LONG_BLOB = 251
    MYSQL_TYPE_BLOB = 252
    MYSQL_TYPE_VAR_STRING = 253
    MYSQL_TYPE_STRING = 254
    MYSQL_TYPE_GEOMETRY = 255


C_TYPES = TYPES()
TYPES = TYPES()


class MYSQL_DATA_TYPE(enum.Enum):
    TINYINT = "TINYINT"
    SMALLINT = "SMALLINT"
    MEDIUMINT = "MEDIUMINT"
    INT = "INT"
    BIGINT = "BIGINT"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    DECIMAL = "DECIMAL"
    YEAR = "YEAR"
    TIME = "TIME"
    DATE = "DATE"
    DATETIME = "DATETIME"
    TIMESTAMP = "TIMESTAMP"
    CHAR = "CHAR"
    BINARY = "BINARY"
    VARCHAR = "VARCHAR"
    VARBINARY = "VARBINARY"
    TINYBLOB = "TINYBLOB"
    TINYTEXT = "TINYTEXT"
    BLOB = "BLOB"
    TEXT = "TEXT"
    MEDIUMBLOB = "MEDIUMBLOB"
    MEDIUMTEXT = "MEDIUMTEXT"
    LONGBLOB = "LONGBLOB"
    LONGTEXT = "LONGTEXT"
    BIT = "BIT"
    BOOL = "BOOL"
    BOOLEAN = "BOOLEAN"
    JSON = "JSON"
    VECTOR = "VECTOR"


# Default values for attributes of MySQL data types as they appear in information_schema.columns
# These values match the MySQL v8.0.37 defaults and are used to properly represent column metadata
MYSQL_DATA_TYPE_COLUMNS_DEFAULT = {
    MYSQL_DATA_TYPE.TINYINT: {"NUMERIC_PRECISION": 3, "NUMERIC_SCALE": 0},
    MYSQL_DATA_TYPE.SMALLINT: {"NUMERIC_PRECISION": 5, "NUMERIC_SCALE": 0},
    MYSQL_DATA_TYPE.MEDIUMINT: {"NUMERIC_PRECISION": 7, "NUMERIC_SCALE": 0},
    MYSQL_DATA_TYPE.INT: {"NUMERIC_PRECISION": 10, "NUMERIC_SCALE": 0},
    MYSQL_DATA_TYPE.BIGINT: {"NUMERIC_PRECISION": 19, "NUMERIC_SCALE": 0},
    MYSQL_DATA_TYPE.FLOAT: {"NUMERIC_PRECISION": 12},
    MYSQL_DATA_TYPE.DOUBLE: {"NUMERIC_PRECISION": 22},
    MYSQL_DATA_TYPE.DECIMAL: {"NUMERIC_PRECISION": 10, "NUMERIC_SCALE": 0, "COLUMN_TYPE": "decimal(10,0)"},
    MYSQL_DATA_TYPE.YEAR: {
        # every column is null
    },
    MYSQL_DATA_TYPE.TIME: {"DATETIME_PRECISION": 0},
    MYSQL_DATA_TYPE.DATE: {
        # every column is null
    },
    MYSQL_DATA_TYPE.DATETIME: {"DATETIME_PRECISION": 0},
    MYSQL_DATA_TYPE.TIMESTAMP: {"DATETIME_PRECISION": 0},
    MYSQL_DATA_TYPE.CHAR: {
        "CHARACTER_MAXIMUM_LENGTH": 1,
        "CHARACTER_OCTET_LENGTH": 4,
        "CHARACTER_SET_NAME": "utf8",
        "COLLATION_NAME": "utf8_bin",
        "COLUMN_TYPE": "char(1)",
    },
    MYSQL_DATA_TYPE.BINARY: {"CHARACTER_MAXIMUM_LENGTH": 1, "CHARACTER_OCTET_LENGTH": 1, "COLUMN_TYPE": "binary(1)"},
    MYSQL_DATA_TYPE.VARCHAR: {
        "CHARACTER_MAXIMUM_LENGTH": 1024,  # NOTE mandatory for field creation
        "CHARACTER_OCTET_LENGTH": 4096,  # NOTE mandatory for field creation
        "CHARACTER_SET_NAME": "utf8",
        "COLLATION_NAME": "utf8_bin",
        "COLUMN_TYPE": "varchar(1024)",
    },
    MYSQL_DATA_TYPE.VARBINARY: {
        "CHARACTER_MAXIMUM_LENGTH": 1024,  # NOTE mandatory for field creation
        "CHARACTER_OCTET_LENGTH": 1024,  # NOTE mandatory for field creation
        "COLUMN_TYPE": "varbinary(1024)",
    },
    MYSQL_DATA_TYPE.TINYBLOB: {"CHARACTER_MAXIMUM_LENGTH": 255, "CHARACTER_OCTET_LENGTH": 255},
    MYSQL_DATA_TYPE.TINYTEXT: {
        "CHARACTER_MAXIMUM_LENGTH": 255,
        "CHARACTER_OCTET_LENGTH": 255,
        "CHARACTER_SET_NAME": "utf8",
        "COLLATION_NAME": "utf8_bin",
    },
    MYSQL_DATA_TYPE.BLOB: {"CHARACTER_MAXIMUM_LENGTH": 65535, "CHARACTER_OCTET_LENGTH": 65535},
    MYSQL_DATA_TYPE.TEXT: {
        "CHARACTER_MAXIMUM_LENGTH": 65535,
        "CHARACTER_OCTET_LENGTH": 65535,
        "CHARACTER_SET_NAME": "utf8",
        "COLLATION_NAME": "utf8_bin",
    },
    MYSQL_DATA_TYPE.MEDIUMBLOB: {"CHARACTER_MAXIMUM_LENGTH": 16777215, "CHARACTER_OCTET_LENGTH": 16777215},
    MYSQL_DATA_TYPE.MEDIUMTEXT: {
        "CHARACTER_MAXIMUM_LENGTH": 16777215,
        "CHARACTER_OCTET_LENGTH": 16777215,
        "CHARACTER_SET_NAME": "utf8",
        "COLLATION_NAME": "utf8_bin",
    },
    MYSQL_DATA_TYPE.LONGBLOB: {
        "CHARACTER_MAXIMUM_LENGTH": 4294967295,
        "CHARACTER_OCTET_LENGTH": 4294967295,
    },
    MYSQL_DATA_TYPE.LONGTEXT: {
        "CHARACTER_MAXIMUM_LENGTH": 4294967295,
        "CHARACTER_OCTET_LENGTH": 4294967295,
        "CHARACTER_SET_NAME": "utf8",
        "COLLATION_NAME": "utf8_bin",
    },
    MYSQL_DATA_TYPE.BIT: {
        "NUMERIC_PRECISION": 1,
        "COLUMN_TYPE": "bit(1)",
        # 'NUMERIC_SCALE': null
    },
    MYSQL_DATA_TYPE.BOOL: {
        "DATA_TYPE": "tinyint",
        "NUMERIC_PRECISION": 3,
        "NUMERIC_SCALE": 0,
        "COLUMN_TYPE": "tinyint(1)",
    },
    MYSQL_DATA_TYPE.BOOLEAN: {
        "DATA_TYPE": "tinyint",
        "NUMERIC_PRECISION": 3,
        "NUMERIC_SCALE": 0,
        "COLUMN_TYPE": "tinyint(1)",
    },
}


class FIELD_FLAG(object):
    __slots__ = ()
    NOT_NULL = 1  # field cannot be null
    PRIMARY_KEY = 2  # field is a primary key
    UNIQUE_KEY = 4  # field is unique
    MULTIPLE_KEY = 8  # field is in a multiple key
    BLOB = 16  # is this field a Blob
    UNSIGNED = 32  # is this field unsigned
    ZEROFILL_FLAG = 64  # is this field a zerofill
    BINARY_COLLATION = 128  # whether this field has a binary collation
    ENUM = 256  # Field is an enumeration
    AUTO_INCREMENT = 512  # field auto-increment
    TIMESTAMP = 1024  # field is a timestamp value
    SET = 2048  # field is a SET
    NO_DEFAULT_VALUE_FLAG = 4096  # field doesn't have default value
    ON_UPDATE_NOW_FLAG = 8192  # field is set to NOW on UPDATE
    NUM_FLAG = 32768  # field is num


FIELD_FLAG = FIELD_FLAG()


@dataclass(frozen=True)
class CTypeProperties:
    """Properties that describe int-representation of mysql column.

    Attributes:
        code (int): Code of the mysql type.
        size (int | None): Size of the column. If not specified, then size is variable (text/blob types).
        flags (list[int]): Flags of the mysql type.
    """

    code: int
    size: int | None = None
    flags: list[int] = field(default_factory=list)


# Map between data types and C types
# Fields size and flags been taken from tcp dump of mysql-server response
# https://dev.mysql.com/doc/c-api/8.0/en/c-api-prepared-statement-type-codes.html
DATA_C_TYPE_MAP = {
    MYSQL_DATA_TYPE.TINYINT: CTypeProperties(C_TYPES.MYSQL_TYPE_TINY, 4),
    MYSQL_DATA_TYPE.SMALLINT: CTypeProperties(C_TYPES.MYSQL_TYPE_SHORT, 6),
    MYSQL_DATA_TYPE.MEDIUMINT: CTypeProperties(C_TYPES.MYSQL_TYPE_INT24, 9),
    MYSQL_DATA_TYPE.INT: CTypeProperties(C_TYPES.MYSQL_TYPE_LONG, 11),
    MYSQL_DATA_TYPE.BIGINT: CTypeProperties(C_TYPES.MYSQL_TYPE_LONGLONG, 20),
    MYSQL_DATA_TYPE.FLOAT: CTypeProperties(C_TYPES.MYSQL_TYPE_FLOAT, 12),
    MYSQL_DATA_TYPE.DOUBLE: CTypeProperties(C_TYPES.MYSQL_TYPE_DOUBLE, 22),
    MYSQL_DATA_TYPE.DECIMAL: CTypeProperties(C_TYPES.MYSQL_TYPE_NEWDECIMAL),
    MYSQL_DATA_TYPE.YEAR: CTypeProperties(C_TYPES.MYSQL_TYPE_YEAR, 4, [FIELD_FLAG.UNSIGNED, FIELD_FLAG.ZEROFILL_FLAG]),
    MYSQL_DATA_TYPE.TIME: CTypeProperties(C_TYPES.MYSQL_TYPE_TIME, 10, [FIELD_FLAG.BINARY_COLLATION]),
    MYSQL_DATA_TYPE.DATE: CTypeProperties(C_TYPES.MYSQL_TYPE_DATE, 10, [FIELD_FLAG.BINARY_COLLATION]),
    MYSQL_DATA_TYPE.DATETIME: CTypeProperties(C_TYPES.MYSQL_TYPE_DATETIME, 19, [FIELD_FLAG.BINARY_COLLATION]),
    MYSQL_DATA_TYPE.TIMESTAMP: CTypeProperties(
        C_TYPES.MYSQL_TYPE_TIMESTAMP, 19, [FIELD_FLAG.BINARY_COLLATION, FIELD_FLAG.TIMESTAMP]
    ),
    MYSQL_DATA_TYPE.CHAR: CTypeProperties(C_TYPES.MYSQL_TYPE_STRING),
    MYSQL_DATA_TYPE.BINARY: CTypeProperties(C_TYPES.MYSQL_TYPE_STRING, flags=[FIELD_FLAG.BINARY_COLLATION]),
    MYSQL_DATA_TYPE.VARCHAR: CTypeProperties(C_TYPES.MYSQL_TYPE_VAR_STRING),
    MYSQL_DATA_TYPE.VARBINARY: CTypeProperties(C_TYPES.MYSQL_TYPE_VAR_STRING, flags=[FIELD_FLAG.BINARY_COLLATION]),
    MYSQL_DATA_TYPE.TINYBLOB: CTypeProperties(
        C_TYPES.MYSQL_TYPE_BLOB, flags=[FIELD_FLAG.BLOB, FIELD_FLAG.BINARY_COLLATION]
    ),
    MYSQL_DATA_TYPE.TINYTEXT: CTypeProperties(C_TYPES.MYSQL_TYPE_BLOB, flags=[FIELD_FLAG.BLOB]),
    MYSQL_DATA_TYPE.BLOB: CTypeProperties(
        C_TYPES.MYSQL_TYPE_BLOB, flags=[FIELD_FLAG.BLOB, FIELD_FLAG.BINARY_COLLATION]
    ),
    MYSQL_DATA_TYPE.TEXT: CTypeProperties(C_TYPES.MYSQL_TYPE_BLOB, flags=[FIELD_FLAG.BLOB]),
    MYSQL_DATA_TYPE.MEDIUMBLOB: CTypeProperties(
        C_TYPES.MYSQL_TYPE_BLOB, flags=[FIELD_FLAG.BLOB, FIELD_FLAG.BINARY_COLLATION]
    ),
    MYSQL_DATA_TYPE.MEDIUMTEXT: CTypeProperties(C_TYPES.MYSQL_TYPE_BLOB, flags=[FIELD_FLAG.BLOB]),
    MYSQL_DATA_TYPE.LONGBLOB: CTypeProperties(
        C_TYPES.MYSQL_TYPE_BLOB, flags=[FIELD_FLAG.BLOB, FIELD_FLAG.BINARY_COLLATION]
    ),
    MYSQL_DATA_TYPE.LONGTEXT: CTypeProperties(C_TYPES.MYSQL_TYPE_BLOB, flags=[FIELD_FLAG.BLOB]),
    MYSQL_DATA_TYPE.BIT: CTypeProperties(C_TYPES.MYSQL_TYPE_BIT, 8, [FIELD_FLAG.UNSIGNED]),
    MYSQL_DATA_TYPE.BOOL: CTypeProperties(C_TYPES.MYSQL_TYPE_TINY, 1),
    MYSQL_DATA_TYPE.BOOLEAN: CTypeProperties(C_TYPES.MYSQL_TYPE_TINY, 1),
    MYSQL_DATA_TYPE.JSON: CTypeProperties(
        C_TYPES.MYSQL_TYPE_JSON, flags=[FIELD_FLAG.BLOB, FIELD_FLAG.BINARY_COLLATION]
    ),
    MYSQL_DATA_TYPE.VECTOR: CTypeProperties(
        C_TYPES.MYSQL_TYPE_VECTOR, 4096, flags=[FIELD_FLAG.BLOB, FIELD_FLAG.BINARY_COLLATION]
    ),
}


# HANDSHAKE

DEFAULT_COALLITION_ID = 83
SERVER_STATUS_AUTOCOMMIT = 2

# NOTE real mysql-server returns by default all (capabilities 0xffff, extended 0xc1ff)
DEFAULT_CAPABILITIES = sum(
    [
        CAPABILITIES.CLIENT_LONG_PASSWORD,
        CAPABILITIES.CLIENT_LONG_FLAG,
        CAPABILITIES.CLIENT_CONNECT_WITH_DB,
        CAPABILITIES.CLIENT_PROTOCOL_41,
        CAPABILITIES.CLIENT_TRANSACTIONS,
        CAPABILITIES.CLIENT_FOUND_ROWS,
        CAPABILITIES.CLIENT_LOCAL_FILES,
        CAPABILITIES.CLIENT_CONNECT_ATTRS,
        CAPABILITIES.CLIENT_PLUGIN_AUTH,
        CAPABILITIES.CLIENT_SSL,
        CAPABILITIES.CLIENT_SECURE_CONNECTION,
        CAPABILITIES.CLIENT_DEPRECATE_EOF,
    ]
)

DEFAULT_AUTH_METHOD = "caching_sha2_password"  # [mysql_native_password|caching_sha2_password]

FILLER_FOR_WIRESHARK_DUMP = 21


# Datum lenenc encoding

NULL_VALUE = b"\xfb"
ONE_BYTE_ENC = b"\xfa"
TWO_BYTE_ENC = b"\xfc"
THREE_BYTE_ENC = b"\xfd"
EIGHT_BYTE_ENC = b"\xfe"


# ERROR CODES
class ERR(object):
    __slots__ = ()
    ER_OLD_TEMPORALS_UPGRADED = 1880
    ER_ONLY_FD_AND_RBR_EVENTS_ALLOWED_IN_BINLOG_STATEMENT = 1730
    ER_ONLY_INTEGERS_ALLOWED = 1578
    ER_ONLY_ON_RANGE_LIST_PARTITION = 1512
    ER_OPEN_AS_READONLY = 1036
    ER_OPERAND_COLUMNS = 1241
    ER_OPTION_PREVENTS_STATEMENT = 1290
    ER_ORDER_WITH_PROC = 1386
    ER_OUT_OF_RESOURCES = 1041
    ER_OUT_OF_SORTMEMORY = 1038
    ER_OUTOFMEMORY = 1037
    ER_PARSE_ERROR = 1064
    ER_PART_STATE_ERROR = 1522
    ER_PARTITION_CLAUSE_ON_NONPARTITIONED = 1747
    ER_PARTITION_COLUMN_LIST_ERROR = 1653
    ER_PARTITION_CONST_DOMAIN_ERROR = 1563
    ER_PARTITION_ENTRY_ERROR = 1496
    ER_PARTITION_EXCHANGE_DIFFERENT_OPTION = 1731
    ER_PARTITION_EXCHANGE_FOREIGN_KEY = 1740
    ER_PARTITION_EXCHANGE_PART_TABLE = 1732
    ER_PARTITION_EXCHANGE_TEMP_TABLE = 1733
    ER_PARTITION_FIELDS_TOO_LONG = 1660
    ER_PARTITION_FUNC_NOT_ALLOWED_ERROR = 1491
    ER_PARTITION_FUNCTION_FAILURE = 1521
    ER_PARTITION_FUNCTION_IS_NOT_ALLOWED = 1564
    ER_PARTITION_INSTEAD_OF_SUBPARTITION = 1734
    ER_PARTITION_MAXVALUE_ERROR = 1481
    ER_PARTITION_MERGE_ERROR = 1572
    ER_PARTITION_MGMT_ON_NONPARTITIONED = 1505
    ER_PARTITION_NAME = 1633
    ER_PARTITION_NO_TEMPORARY = 1562
    ER_PARTITION_NOT_DEFINED_ERROR = 1498
    ER_PARTITION_REQUIRES_VALUES_ERROR = 1479
    ER_PARTITION_SUBPART_MIX_ERROR = 1483
    ER_PARTITION_SUBPARTITION_ERROR = 1482
    ER_PARTITION_WRONG_NO_PART_ERROR = 1484
    ER_PARTITION_WRONG_NO_SUBPART_ERROR = 1485
    ER_PARTITION_WRONG_VALUES_ERROR = 1480
    ER_PARTITIONS_MUST_BE_DEFINED_ERROR = 1492
    ER_PASSWD_LENGTH = 1372
    ER_PASSWORD_ANONYMOUS_USER = 1131
    ER_PASSWORD_FORMAT = 1827
    ER_PASSWORD_NO_MATCH = 1133
    ER_PASSWORD_NOT_ALLOWED = 1132
    ER_PATH_LENGTH = 1680
    ER_PLUGIN_CANNOT_BE_UNINSTALLED = 1883
    ER_PLUGIN_IS_NOT_LOADED = 1524
    ER_PLUGIN_IS_PERMANENT = 1702
    ER_PLUGIN_NO_INSTALL = 1721
    ER_PLUGIN_NO_UNINSTALL = 1720
    ER_PRIMARY_CANT_HAVE_NULL = 1171
    ER_PROC_AUTO_GRANT_FAIL = 1404
    ER_PROC_AUTO_REVOKE_FAIL = 1405
    ER_PROCACCESS_DENIED_ERROR = 1370
    ER_PS_MANY_PARAM = 1390
    ER_PS_NO_RECURSION = 1444
    ER_QUERY_CACHE_DISABLED = 1651
    ER_QUERY_INTERRUPTED = 1317
    ER_QUERY_ON_FOREIGN_DATA_SOURCE = 1430
    ER_QUERY_ON_MASTER = 1219
    ER_RANGE_NOT_INCREASING_ERROR = 1493
    ER_RBR_NOT_AVAILABLE = 1574
    ER_READ_ONLY_MODE = 1836
    ER_READ_ONLY_TRANSACTION = 1207
    ER_READY = 1076
    ER_RECORD_FILE_FULL = 1114
    ER_REGEXP_ERROR = 1139
    ER_RELAY_LOG_FAIL = 1371
    ER_RELAY_LOG_INIT = 1380
    ER_REMOVED_SPACES = 1466
    ER_RENAMED_NAME = 1636
    ER_REORG_HASH_ONLY_ON_SAME_N = 1510
    ER_REORG_NO_PARAM_ERROR = 1511
    ER_REORG_OUTSIDE_RANGE = 1520
    ER_REORG_PARTITION_NOT_EXIST = 1516
    ER_REQUIRES_PRIMARY_KEY = 1173
    ER_RESERVED_SYNTAX = 1382
    ER_RESIGNAL_WITHOUT_ACTIVE_HANDLER = 1645
    ER_REVOKE_GRANTS = 1269
    ER_ROW_DOES_NOT_MATCH_GIVEN_PARTITION_SET = 1748
    ER_ROW_DOES_NOT_MATCH_PARTITION = 1737
    ER_ROW_IN_WRONG_PARTITION = 1863
    ER_ROW_IS_REFERENCED = 1217
    ER_ROW_IS_REFERENCED_2 = 1451
    ER_ROW_SINGLE_PARTITION_FIELD_ERROR = 1658
    ER_RPL_INFO_DATA_TOO_LONG = 1742
    ER_SAME_NAME_PARTITION = 1517
    ER_SAME_NAME_PARTITION_FIELD = 1652
    ER_SELECT_REDUCED = 1249
    ER_SERVER_IS_IN_SECURE_AUTH_MODE = 1275
    ER_SERVER_SHUTDOWN = 1053
    ER_SET_CONSTANTS_ONLY = 1204
    ER_SET_PASSWORD_AUTH_PLUGIN = 1699
    ER_SET_STATEMENT_CANNOT_INVOKE_FUNCTION = 1769
    ER_SHUTDOWN_COMPLETE = 1079
    ER_SIGNAL_BAD_CONDITION_TYPE = 1646
    ER_SIGNAL_EXCEPTION = 1644
    ER_SIGNAL_NOT_FOUND = 1643
    ER_SIGNAL_WARN = 1642
    ER_SIZE_OVERFLOW_ERROR = 1532
    ER_SKIPPING_LOGGED_TRANSACTION = 1771
    ER_SLAVE_CANT_CREATE_CONVERSION = 1678
    ER_SLAVE_CONFIGURATION = 1794
    ER_SLAVE_CONVERSION_FAILED = 1677
    ER_SLAVE_CORRUPT_EVENT = 1610
    ER_SLAVE_CREATE_EVENT_FAILURE = 1596
    ER_SLAVE_FATAL_ERROR = 1593
    ER_SLAVE_HAS_MORE_GTIDS_THAN_MASTER = 1885
    ER_SLAVE_HEARTBEAT_FAILURE = 1623
    ER_SLAVE_HEARTBEAT_VALUE_OUT_OF_RANGE = 1624
    ER_SLAVE_HEARTBEAT_VALUE_OUT_OF_RANGE_MAX = 1704
    ER_SLAVE_HEARTBEAT_VALUE_OUT_OF_RANGE_MIN = 1703
    ER_SLAVE_IGNORE_SERVER_IDS = 1650
    ER_SLAVE_IGNORED_SSL_PARAMS = 1274
    ER_SLAVE_IGNORED_TABLE = 1237
    ER_SLAVE_INCIDENT = 1590
    ER_SLAVE_MASTER_COM_FAILURE = 1597
    ER_SLAVE_MI_INIT_REPOSITORY = 1871
    ER_SLAVE_MUST_STOP = 1198
    ER_SLAVE_NOT_RUNNING = 1199
    ER_SLAVE_RELAY_LOG_READ_FAILURE = 1594
    ER_SLAVE_RELAY_LOG_WRITE_FAILURE = 1595
    ER_SLAVE_RLI_INIT_REPOSITORY = 1872
    ER_SLAVE_SILENT_RETRY_TRANSACTION = 1806
    ER_SLAVE_THREAD = 1202
    ER_SLAVE_WAS_NOT_RUNNING = 1255
    ER_SLAVE_WAS_RUNNING = 1254
    ER_SP_ALREADY_EXISTS = 1304
    ER_SP_BAD_CURSOR_QUERY = 1322
    ER_SP_BAD_CURSOR_SELECT = 1323
    ER_SP_BAD_SQLSTATE = 1407
    ER_SP_BAD_VAR_SHADOW = 1453
    ER_SP_BADRETURN = 1313
    ER_SP_BADSELECT = 1312
    ER_SP_BADSTATEMENT = 1314
    ER_SP_CANT_ALTER = 1334
    ER_SP_CANT_SET_AUTOCOMMIT = 1445
    ER_SP_CASE_NOT_FOUND = 1339
    ER_SP_COND_MISMATCH = 1319
    ER_SP_CURSOR_AFTER_HANDLER = 1338
    ER_SP_CURSOR_ALREADY_OPEN = 1325
    ER_SP_CURSOR_MISMATCH = 1324
    ER_SP_CURSOR_NOT_OPEN = 1326
    ER_SP_DOES_NOT_EXIST = 1305
    ER_SP_DROP_FAILED = 1306
    ER_SP_DUP_COND = 1332
    ER_SP_DUP_CURS = 1333
    ER_SP_DUP_HANDLER = 1413
    ER_SP_DUP_PARAM = 1330
    ER_SP_DUP_VAR = 1331
    ER_SP_FETCH_NO_DATA = 1329
    ER_SP_GOTO_IN_HNDLR = 1358
    ER_SP_LABEL_MISMATCH = 1310
    ER_SP_LABEL_REDEFINE = 1309
    ER_SP_LILABEL_MISMATCH = 1308
    ER_SP_NO_AGGREGATE = 1460
    ER_SP_NO_DROP_SP = 1357
    ER_SP_NO_RECURSION = 1424
    ER_SP_NO_RECURSIVE_CREATE = 1303
    ER_SP_NO_RETSET = 1415
    ER_SP_NORETURN = 1320
    ER_SP_NORETURNEND = 1321
    ER_SP_NOT_VAR_ARG = 1414
    ER_SP_PROC_TABLE_CORRUPT = 1457
    ER_SP_RECURSION_LIMIT = 1456
    ER_SP_STORE_FAILED = 1307
    ER_SP_SUBSELECT_NYI = 1335
    ER_SP_UNDECLARED_VAR = 1327
    ER_SP_UNINIT_VAR = 1311
    ER_SP_VARCOND_AFTER_CURSHNDLR = 1337
    ER_SP_WRONG_NAME = 1458
    ER_SP_WRONG_NO_OF_ARGS = 1318
    ER_SP_WRONG_NO_OF_FETCH_ARGS = 1328
    ER_SPATIAL_CANT_HAVE_NULL = 1252
    ER_SPATIAL_MUST_HAVE_GEOM_COL = 1687
    ER_SPECIFIC_ACCESS_DENIED_ERROR = 1227
    ER_SQL_SLAVE_SKIP_COUNTER_NOT_SETTABLE_IN_GTID_MODE = 1858
    ER_SQLTHREAD_WITH_SECURE_SLAVE = 1763
    ER_SR_INVALID_CREATION_CTX = 1601
    ER_STACK_OVERRUN = 1119
    ER_STACK_OVERRUN_NEED_MORE = 1436
    ER_STARTUP = 1408
    ER_STMT_CACHE_FULL = 1705
    ER_STMT_HAS_NO_OPEN_CURSOR = 1421
    ER_STMT_NOT_ALLOWED_IN_SF_OR_TRG = 1336
    ER_STOP_SLAVE_IO_THREAD_TIMEOUT = 1876
    ER_STOP_SLAVE_SQL_THREAD_TIMEOUT = 1875
    ER_STORED_FUNCTION_PREVENTS_SWITCH_BINLOG_FORMAT = 1560
    ER_STORED_FUNCTION_PREVENTS_SWITCH_SQL_LOG_BIN = 1695
    ER_STORED_FUNCTION_PREVENTS_SWITCH_BINLOG_DIRECT = 1686
    ER_SUBPARTITION_ERROR = 1500
    ER_SUBPARTITION_NAME = 1634
    ER_SUBQUERY_NO_1_ROW = 1242
    ER_SYNTAX_ERROR = 1149
    ER_TABLE_CANT_HANDLE_AUTO_INCREMENT = 1164
    ER_TABLE_CANT_HANDLE_BLOB = 1163
    ER_TABLE_CANT_HANDLE_FT = 1214
    ER_TABLE_CANT_HANDLE_SPKEYS = 1464
    ER_TABLE_CORRUPT = 1877
    ER_TABLE_DEF_CHANGED = 1412
    ER_TABLE_EXISTS_ERROR = 1050
    ER_TABLE_HAS_NO_FT = 1764
    ER_TABLE_IN_FK_CHECK = 1725
    ER_TABLE_IN_SYSTEM_TABLESPACE = 1809
    ER_TABLE_MUST_HAVE_COLUMNS = 1113
    ER_TABLE_NAME = 1632
    ER_TABLE_NEEDS_REBUILD = 1707
    ER_TABLE_NEEDS_UPGRADE = 1459
    ER_TABLE_NOT_LOCKED = 1100
    ER_TABLE_NOT_LOCKED_FOR_WRITE = 1099
    ER_TABLE_SCHEMA_MISMATCH = 1808
    ER_TABLEACCESS_DENIED_ERROR = 1142
    ER_TABLENAME_NOT_ALLOWED_HERE = 1250
    ER_TABLES_DIFFERENT_METADATA = 1736
    ER_TABLESPACE_AUTO_EXTEND_ERROR = 1530
    ER_TABLESPACE_DISCARDED = 1814
    ER_TABLESPACE_EXISTS = 1813
    ER_TABLESPACE_MISSING = 1812
    ER_TEMP_FILE_WRITE_FAILURE = 1878
    ER_TEMP_TABLE_PREVENTS_SWITCH_OUT_OF_RBR = 1559
    ER_TEMPORARY_NAME = 1635
    ER_TEXTFILE_NOT_READABLE = 1085
    ER_TOO_BIG_DISPLAYWIDTH = 1439
    ER_TOO_BIG_FIELDLENGTH = 1074
    ER_TOO_BIG_FOR_UNCOMPRESS = 1256
    ER_TOO_BIG_PRECISION = 1426
    ER_TOO_BIG_ROWSIZE = 1118
    ER_TOO_BIG_SCALE = 1425
    ER_TOO_BIG_SELECT = 1104
    ER_TOO_BIG_SET = 1097
    ER_TOO_HIGH_LEVEL_OF_NESTING_FOR_SELECT = 1473
    ER_TOO_LONG_BODY = 1437
    ER_TOO_LONG_FIELD_COMMENT = 1629
    ER_TOO_LONG_IDENT = 1059
    ER_TOO_LONG_INDEX_COMMENT = 1688
    ER_TOO_LONG_KEY = 1071
    ER_TOO_LONG_STRING = 1162
    ER_TOO_LONG_TABLE_COMMENT = 1628
    ER_TOO_LONG_TABLE_PARTITION_COMMENT = 1793
    ER_TOO_MANY_CONCURRENT_TRXS = 1637
    ER_TOO_MANY_DELAYED_THREADS = 1151
    ER_TOO_MANY_FIELDS = 1117
    ER_TOO_MANY_KEY_PARTS = 1070
    ER_TOO_MANY_KEYS = 1069
    ER_TOO_MANY_PARTITION_FUNC_FIELDS_ERROR = 1655
    ER_TOO_MANY_PARTITIONS_ERROR = 1499
    ER_TOO_MANY_ROWS = 1172
    ER_TOO_MANY_TABLES = 1116
    ER_TOO_MANY_USER_CONNECTIONS = 1203
    ER_TOO_MANY_VALUES_ERROR = 1657
    ER_TOO_MUCH_AUTO_TIMESTAMP_COLS = 1293
    ER_TRANS_CACHE_FULL = 1197
    ER_TRG_ALREADY_EXISTS = 1359
    ER_TRG_CANT_CHANGE_ROW = 1362
    ER_TRG_CANT_OPEN_TABLE = 1606
    ER_TRG_CORRUPTED_FILE = 1602
    ER_TRG_DOES_NOT_EXIST = 1360
    ER_TRG_IN_WRONG_SCHEMA = 1435
    ER_TRG_INVALID_CREATION_CTX = 1604
    ER_TRG_NO_CREATION_CTX = 1603
    ER_TRG_NO_DEFINER = 1454
    ER_TRG_NO_SUCH_ROW_IN_TRG = 1363
    ER_TRG_ON_VIEW_OR_TEMP_TABLE = 1361
    ER_TRUNCATE_ILLEGAL_FK = 1701
    ER_TRUNCATED_WRONG_VALUE = 1292
    ER_TRUNCATED_WRONG_VALUE_FOR_FIELD = 1366
    ER_UDF_EXISTS = 1125
    ER_UDF_NO_PATHS = 1124
    ER_UNDO_RECORD_TOO_BIG = 1713
    ER_UNEXPECTED_EOF = 1039
    ER_UNION_TABLES_IN_DIFFERENT_DIR = 1212
    ER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF = 1503
    ER_UNKNOWN_ALTER_ALGORITHM = 1800
    ER_UNKNOWN_ALTER_LOCK = 1801
    ER_UNKNOWN_CHARACTER_SET = 1115
    ER_UNKNOWN_COLLATION = 1273
    ER_UNKNOWN_COM_ERROR = 1047
    ER_UNKNOWN_ERROR = 1105
    ER_UNKNOWN_EXPLAIN_FORMAT = 1791
    ER_UNKNOWN_KEY_CACHE = 1284
    ER_UNKNOWN_LOCALE = 1649
    ER_UNKNOWN_PARTITION = 1735
    ER_UNKNOWN_PROCEDURE = 1106
    ER_UNKNOWN_STMT_HANDLER = 1243
    ER_UNKNOWN_STORAGE_ENGINE = 1286
    ER_UNKNOWN_SYSTEM_VARIABLE = 1193
    ER_UNKNOWN_TABLE = 1109
    ER_UNKNOWN_TARGET_BINLOG = 1373
    ER_UNKNOWN_TIME_ZONE = 1298
    ER_UNSUPORTED_LOG_ENGINE = 1579
    ER_UNSUPPORTED_ENGINE = 1726
    ER_UNSUPPORTED_EXTENSION = 1112
    ER_UNSUPPORTED_PS = 1295
    ER_UNTIL_COND_IGNORED = 1279
    ER_UPDATE_INF = 1134
    ER_UPDATE_LOG_DEPRECATED_IGNORED = 1315
    ER_UPDATE_LOG_DEPRECATED_TRANSLATED = 1316
    ER_UPDATE_TABLE_USED = 1093
    ER_UPDATE_WITHOUT_KEY_IN_SAFE_MODE = 1175
    ER_USER_LIMIT_REACHED = 1226
    ER_USERNAME = 1468
    ER_VALUES_IS_NOT_INT_TYPE_ERROR = 1697
    ER_VAR_CANT_BE_READ = 1233
    ER_VARIABLE_IS_NOT_STRUCT = 1272
    ER_VARIABLE_IS_READONLY = 1621
    ER_VARIABLE_NOT_SETTABLE_IN_SF_OR_TRIGGER = 1765
    ER_VARIABLE_NOT_SETTABLE_IN_SP = 1838
    ER_VARIABLE_NOT_SETTABLE_IN_TRANSACTION = 1766
    ER_VIEW_CHECK_FAILED = 1369
    ER_VIEW_CHECKSUM = 1392
    ER_VIEW_DELETE_MERGE_VIEW = 1395
    ER_VIEW_FRM_NO_USER = 1447
    ER_VIEW_INVALID = 1356
    ER_VIEW_INVALID_CREATION_CTX = 1600
    ER_VIEW_MULTIUPDATE = 1393
    ER_VIEW_NO_CREATION_CTX = 1599
    ER_VIEW_NO_EXPLAIN = 1345
    ER_VIEW_NO_INSERT_FIELD_LIST = 1394
    ER_VIEW_NONUPD_CHECK = 1368
    ER_VIEW_OTHER_USER = 1448
    ER_VIEW_PREVENT_UPDATE = 1443
    ER_VIEW_RECURSIVE = 1462
    ER_VIEW_SELECT_CLAUSE = 1350
    ER_VIEW_SELECT_DERIVED = 1349
    ER_VIEW_SELECT_TMPTABLE = 1352
    ER_VIEW_SELECT_VARIABLE = 1351
    ER_VIEW_WRONG_LIST = 1353
    ER_WARN_ALLOWED_PACKET_OVERFLOWED = 1301
    ER_WARN_CANT_DROP_DEFAULT_KEYCACHE = 1438
    ER_WARN_DATA_OUT_OF_RANGE = 1264
    ER_WARN_DEPRECATED_SYNTAX = 1287
    ER_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT = 1681
    ER_WARN_DEPRECATED_SYNTAX_WITH_VER = 1554
    ER_WARN_ENGINE_TRANSACTION_ROLLBACK = 1622
    ER_WARN_FIELD_RESOLVED = 1276
    ER_WARN_HOSTNAME_WONT_WORK = 1285
    ER_WARN_I_S_SKIPPED_TABLE = 1684
    ER_WARN_INDEX_NOT_APPLICABLE = 1739
    ER_WARN_INVALID_TIMESTAMP = 1299
    ER_WARN_NULL_TO_NOTNULL = 1263
    ER_WARN_PURGE_LOG_IN_USE = 1867
    ER_WARN_PURGE_LOG_IS_ACTIVE = 1868
    ER_WARN_QC_RESIZE = 1282
    ER_WARN_TOO_FEW_RECORDS = 1261
    ER_WARN_TOO_MANY_RECORDS = 1262
    ER_WARN_USING_OTHER_HANDLER = 1266
    ER_WARN_VIEW_MERGE = 1354
    ER_WARN_VIEW_WITHOUT_KEY = 1355
    ER_WARNING_NOT_COMPLETE_ROLLBACK = 1196
    ER_WARNING_NOT_COMPLETE_ROLLBACK_WITH_CREATED_TEMP_TABLE = 1751
    ER_WARNING_NOT_COMPLETE_ROLLBACK_WITH_DROPPED_TEMP_TABLE = 1752
    ER_WRONG_ARGUMENTS = 1210
    ER_WRONG_AUTO_KEY = 1075
    ER_WRONG_COLUMN_NAME = 1166
    ER_WRONG_DB_NAME = 1102
    ER_WRONG_EXPR_IN_PARTITION_FUNC_ERROR = 1486
    ER_WRONG_FIELD_SPEC = 1063
    ER_WRONG_FIELD_TERMINATORS = 1083
    ER_WRONG_FIELD_WITH_GROUP = 1055
    ER_WRONG_FK_DEF = 1239
    ER_WRONG_GROUP_FIELD = 1056
    ER_WRONG_KEY_COLUMN = 1167
    ER_WRONG_LOCK_OF_SYSTEM_TABLE = 1428
    ER_WRONG_MAGIC = 1389
    ER_WRONG_MRG_TABLE = 1168
    ER_WRONG_NAME_FOR_CATALOG = 1281
    ER_WRONG_NAME_FOR_INDEX = 1280
    ER_WRONG_NATIVE_TABLE_STRUCTURE = 1682
    ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT = 1222
    ER_WRONG_OBJECT = 1347
    ER_WRONG_OUTER_JOIN = 1120
    ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT = 1582
    ER_WRONG_PARAMCOUNT_TO_PROCEDURE = 1107
    ER_WRONG_PARAMETERS_TO_NATIVE_FCT = 1583
    ER_WRONG_PARAMETERS_TO_PROCEDURE = 1108
    ER_WRONG_PARAMETERS_TO_STORED_FCT = 1584
    ER_WRONG_PARTITION_NAME = 1567
    ER_WRONG_PERFSCHEMA_USAGE = 1683
    ER_WRONG_SIZE_NUMBER = 1531
    ER_WRONG_SPVAR_TYPE_IN_LIMIT = 1691
    ER_WRONG_STRING_LENGTH = 1470
    ER_WRONG_SUB_KEY = 1089
    ER_WRONG_SUM_SELECT = 1057
    ER_WRONG_TABLE_NAME = 1103
    ER_WRONG_TYPE_COLUMN_VALUE_ERROR = 1654
    ER_WRONG_TYPE_FOR_VAR = 1232
    ER_WRONG_USAGE = 1221
    ER_WRONG_VALUE = 1525
    ER_WRONG_VALUE_COUNT = 1058
    ER_WRONG_VALUE_COUNT_ON_ROW = 1136
    ER_WRONG_VALUE_FOR_TYPE = 1411
    ER_WRONG_VALUE_FOR_VAR = 1231
    ER_WSAS_FAILED = 1383
    ER_XA_RBDEADLOCK = 1614
    ER_XA_RBROLLBACK = 1402
    ER_XA_RBTIMEOUT = 1613
    ER_XAER_DUPID = 1440
    ER_XAER_INVAL = 1398
    ER_XAER_NOTA = 1397
    ER_XAER_OUTSIDE = 1400
    ER_XAER_RMERR = 1401
    ER_XAER_RMFAIL = 1399
    ER_YES = 1003
    ER_ZLIB_Z_BUF_ERROR = 1258
    ER_ZLIB_Z_DATA_ERROR = 1259
    ER_ZLIB_Z_MEM_ERROR = 1257
    ER_BAD_DB_ERROR = 1049
    ER_BAD_TABLE_ERROR = 1051
    ER_KEY_COLUMN_DOES_NOT_EXIST = 1072
    ER_DUP_FIELDNAME = 1060
    ER_DB_DROP_DELETE = 1009
    ER_NON_INSERTABLE_TABLE = 1471
    ER_NOT_SUPPORTED_YET = 1235


ERR = ERR()


class WARN(object):
    __slots__ = ()
    WARN_COND_ITEM_TRUNCATED = 1647
    WARN_DATA_TRUNCATED = 1265
    WARN_NO_MASTER_INF = 1617
    WARN_NON_ASCII_SEPARATOR_NOT_IMPLEMENTED = 1638
    WARN_ON_BLOCKHOLE_IN_RBR = 1870
    WARN_OPTION_BELOW_LIMIT = 1708
    WARN_OPTION_IGNORED = 1618
    WARN_PLUGIN_BUSY = 1620
    WARN_PLUGIN_DELETE_BUILTIN = 1619


WARN = WARN()

# CHARACTER SET NUMBERS

# noqa
CHARSET_NUMBERS = {
    "big5_chinese_ci": 1,
    "latin2_czech_cs": 2,
    "dec8_swedish_ci": 3,
    "cp850_general_ci": 4,
    "latin1_german1_ci": 5,
    "hp8_english_ci": 6,
    "koi8r_general_ci": 7,
    "latin1_swedish_ci": 8,
    "latin2_general_ci": 9,
    "swe7_swedish_ci": 10,
    "ascii_general_ci": 11,
    "ujis_japanese_ci": 12,
    "sjis_japanese_ci": 13,
    "cp1251_bulgarian_ci": 14,
    "latin1_danish_ci": 15,
    "hebrew_general_ci": 16,
    "tis620_thai_ci": 18,
    "euckr_korean_ci": 19,
    "latin7_estonian_cs": 20,
    "latin2_hungarian_ci": 21,
    "koi8u_general_ci": 22,
    "cp1251_ukrainian_ci": 23,
    "gb2312_chinese_ci": 24,
    "greek_general_ci": 25,
    "cp1250_general_ci": 26,
    "latin2_croatian_ci": 27,
    "gbk_chinese_ci": 28,
    "cp1257_lithuanian_ci": 29,
    "latin5_turkish_ci": 30,
    "latin1_german2_ci": 31,
    "armscii8_general_ci": 32,
    "utf8_general_ci": 33,
    "cp1250_czech_cs": 34,
    "ucs2_general_ci": 35,
    "cp866_general_ci": 36,
    "keybcs2_general_ci": 37,
    "macce_general_ci": 38,
    "macroman_general_ci": 39,
    "cp852_general_ci": 40,
    "latin7_general_ci": 41,
    "latin7_general_cs": 42,
    "macce_bin": 43,
    "cp1250_croatian_ci": 44,
    "utf8mb4_general_ci": 45,
    "utf8mb4_bin": 46,
    "latin1_bin": 47,
    "latin1_general_ci": 48,
    "latin1_general_cs": 49,
    "cp1251_bin": 50,
    "cp1251_general_ci": 51,
    "cp1251_general_cs": 52,
    "macroman_bin": 53,
    "utf16_general_ci": 54,
    "utf16_bin": 55,
    "utf16le_general_ci": 56,
    "cp1256_general_ci": 57,
    "cp1257_bin": 58,
    "cp1257_general_ci": 59,
    "utf32_general_ci": 60,
    "utf32_bin": 61,
    "utf16le_bin": 62,
    "binary": 63,
    "armscii8_bin": 64,
    "ascii_bin": 65,
    "cp1250_bin": 66,
    "cp1256_bin": 67,
    "cp866_bin": 68,
    "dec8_bin": 69,
    "greek_bin": 70,
    "hebrew_bin": 71,
    "hp8_bin": 72,
    "keybcs2_bin": 73,
    "koi8r_bin": 74,
    "koi8u_bin": 75,
    "latin2_bin": 77,
    "latin5_bin": 78,
    "latin7_bin": 79,
    "cp850_bin": 80,
    "cp852_bin": 81,
    "swe7_bin": 82,
    "utf8_bin": 83,
    "big5_bin": 84,
    "euckr_bin": 85,
    "gb2312_bin": 86,
    "gbk_bin": 87,
    "sjis_bin": 88,
    "tis620_bin": 89,
    "ucs2_bin": 90,
    "ujis_bin": 91,
    "geostd8_general_ci": 92,
    "geostd8_bin": 93,
    "latin1_spanish_ci": 94,
    "cp932_japanese_ci": 95,
    "cp932_bin": 96,
    "eucjpms_japanese_ci": 97,
    "eucjpms_bin": 98,
    "cp1250_polish_ci": 99,
    "utf16_unicode_ci": 101,
    "utf16_icelandic_ci": 102,
    "utf16_latvian_ci": 103,
    "utf16_romanian_ci": 104,
    "utf16_slovenian_ci": 105,
    "utf16_polish_ci": 106,
    "utf16_estonian_ci": 107,
    "utf16_spanish_ci": 108,
    "utf16_swedish_ci": 109,
    "utf16_turkish_ci": 110,
    "utf16_czech_ci": 111,
    "utf16_danish_ci": 112,
    "utf16_lithuanian_ci": 113,
    "utf16_slovak_ci": 114,
    "utf16_spanish2_ci": 115,
    "utf16_roman_ci": 116,
    "utf16_persian_ci": 117,
    "utf16_esperanto_ci": 118,
    "utf16_hungarian_ci": 119,
    "utf16_sinhala_ci": 120,
    "utf16_german2_ci": 121,
    "utf16_croatian_ci": 122,
    "utf16_unicode_520_ci": 123,
    "utf16_vietnamese_ci": 124,
    "ucs2_unicode_ci": 128,
    "ucs2_icelandic_ci": 129,
    "ucs2_latvian_ci": 130,
    "ucs2_romanian_ci": 131,
    "ucs2_slovenian_ci": 132,
    "ucs2_polish_ci": 133,
    "ucs2_estonian_ci": 134,
    "ucs2_spanish_ci": 135,
    "ucs2_swedish_ci": 136,
    "ucs2_turkish_ci": 137,
    "ucs2_czech_ci": 138,
    "ucs2_danish_ci": 139,
    "ucs2_lithuanian_ci": 140,
    "ucs2_slovak_ci": 141,
    "ucs2_spanish2_ci": 142,
    "ucs2_roman_ci": 143,
    "ucs2_persian_ci": 144,
    "ucs2_esperanto_ci": 145,
    "ucs2_hungarian_ci": 146,
    "ucs2_sinhala_ci": 147,
    "ucs2_german2_ci": 148,
    "ucs2_croatian_ci": 149,
    "ucs2_unicode_520_ci": 150,
    "ucs2_vietnamese_ci": 151,
    "ucs2_general_mysql500_ci": 159,
    "utf32_unicode_ci": 160,
    "utf32_icelandic_ci": 161,
    "utf32_latvian_ci": 162,
    "utf32_romanian_ci": 163,
    "utf32_slovenian_ci": 164,
    "utf32_polish_ci": 165,
    "utf32_estonian_ci": 166,
    "utf32_spanish_ci": 167,
    "utf32_swedish_ci": 168,
    "utf32_turkish_ci": 169,
    "utf32_czech_ci": 170,
    "utf32_danish_ci": 171,
    "utf32_lithuanian_ci": 172,
    "utf32_slovak_ci": 173,
    "utf32_spanish2_ci": 174,
    "utf32_roman_ci": 175,
    "utf32_persian_ci": 176,
    "utf32_esperanto_ci": 177,
    "utf32_hungarian_ci": 178,
    "utf32_sinhala_ci": 179,
    "utf32_german2_ci": 180,
    "utf32_croatian_ci": 181,
    "utf32_unicode_520_ci": 182,
    "utf32_vietnamese_ci": 183,
    "utf8_unicode_ci": 192,
    "utf8_icelandic_ci": 193,
    "utf8_latvian_ci": 194,
    "utf8_romanian_ci": 195,
    "utf8_slovenian_ci": 196,
    "utf8_polish_ci": 197,
    "utf8_estonian_ci": 198,
    "utf8_spanish_ci": 199,
    "utf8_swedish_ci": 200,
    "utf8_turkish_ci": 201,
    "utf8_czech_ci": 202,
    "utf8_danish_ci": 203,
    "utf8_lithuanian_ci": 204,
    "utf8_slovak_ci": 205,
    "utf8_spanish2_ci": 206,
    "utf8_roman_ci": 207,
    "utf8_persian_ci": 208,
    "utf8_esperanto_ci": 209,
    "utf8_hungarian_ci": 210,
    "utf8_sinhala_ci": 211,
    "utf8_german2_ci": 212,
    "utf8_croatian_ci": 213,
    "utf8_unicode_520_ci": 214,
    "utf8_vietnamese_ci": 215,
    "utf8_general_mysql500_ci": 223,
    "utf8mb4_unicode_ci": 224,
    "utf8mb4_icelandic_ci": 225,
    "utf8mb4_latvian_ci": 226,
    "utf8mb4_romanian_ci": 227,
    "utf8mb4_slovenian_ci": 228,
    "utf8mb4_polish_ci": 229,
    "utf8mb4_estonian_ci": 230,
    "utf8mb4_spanish_ci": 231,
    "utf8mb4_swedish_ci": 232,
    "utf8mb4_turkish_ci": 233,
    "utf8mb4_czech_ci": 234,
    "utf8mb4_danish_ci": 235,
    "utf8mb4_lithuanian_ci": 236,
    "utf8mb4_slovak_ci": 237,
    "utf8mb4_spanish2_ci": 238,
    "utf8mb4_roman_ci": 239,
    "utf8mb4_persian_ci": 240,
    "utf8mb4_esperanto_ci": 241,
    "utf8mb4_hungarian_ci": 242,
    "utf8mb4_sinhala_ci": 243,
    "utf8mb4_german2_ci": 244,
    "utf8mb4_croatian_ci": 245,
    "utf8mb4_unicode_520_ci": 246,
    "utf8mb4_vietnamese_ci": 247,
}


SQL_RESERVED_WORDS = [
    "ALL",
    "ANALYSE",
    "ANALYZE",
    "AND",
    "ANY",
    "AS",
    "ASC",
    "AUTHORIZATION",
    "BETWEEN",
    "BINARY",
    "BOTH",
    "CASE",
    "CAST",
    "CHECK",
    "COLLATE",
    "COLUMN",
    "CONSTRAINT",
    "CREATE",
    "CROSS",
    "CURRENT_DATE",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "CURRENT_USER",
    "DEFAULT",
    "DEFERRABLE",
    "DESC",
    "DISTINCT",
    "DO",
    "ELSE",
    "END",
    "EXCEPT",
    "FALSE",
    "FOR",
    "FOREIGN",
    "FREEZE",
    "FROM",
    "FULL",
    "GRANT",
    "GROUP",
    "HAVING",
    "ILIKE",
    "IN",
    "INITIALLY",
    "INNER",
    "INTERSECT",
    "INTO",
    "IS",
    "ISNULL",
    "JOIN",
    "LEADING",
    "LEFT",
    "LIKE",
    "LIMIT",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "NATURAL",
    "NEW",
    "NOT",
    "NOTNULL",
    "NULL",
    "OFF",
    "OFFSET",
    "OLD",
    "ON",
    "ONLY",
    "OR",
    "ORDER",
    "OUTER",
    "OVERLAPS",
    "PLACING",
    "PRIMARY",
    "REFERENCES",
    "RIGHT",
    "SELECT",
    "SESSION_USER",
    "SIMILAR",
    "SOME",
    "TABLE",
    "THEN",
    "TO",
    "TRAILING",
    "TRUE",
    "UNION",
    "UNIQUE",
    "USER",
    "USING",
    "VERBOSE",
    "WHEN",
    "WHERE",
]

SERVER_VARIABLES = {
    # var_name: (value, type, charset)
    "@@session.auto_increment_increment": (1, TYPES.MYSQL_TYPE_LONGLONG, CHARSET_NUMBERS["binary"]),
    "@@auto_increment_increment": (1, TYPES.MYSQL_TYPE_LONGLONG, CHARSET_NUMBERS["binary"]),
    "@@character_set_client": ("utf8", TYPES.MYSQL_TYPE_VAR_STRING, CHARSET_NUMBERS["utf8_general_ci"]),
    "@@character_set_connection": ("utf8", TYPES.MYSQL_TYPE_VAR_STRING, CHARSET_NUMBERS["utf8_general_ci"]),
    "@@character_set_results": ("utf8", TYPES.MYSQL_TYPE_VAR_STRING, CHARSET_NUMBERS["utf8_general_ci"]),
    "@@GLOBAL.character_set_server": ("latin1", TYPES.MYSQL_TYPE_VAR_STRING, CHARSET_NUMBERS["utf8_general_ci"]),
    "@@character_set_server": ("latin1", TYPES.MYSQL_TYPE_VAR_STRING, CHARSET_NUMBERS["utf8_general_ci"]),
    "@@GLOBAL.collation_server": ("latin1_swedish_ci", TYPES.MYSQL_TYPE_VAR_STRING, CHARSET_NUMBERS["utf8_general_ci"]),
    "@@collation_server": ("latin1_swedish_ci", TYPES.MYSQL_TYPE_VAR_STRING, CHARSET_NUMBERS["utf8_general_ci"]),
    "@@init_connect": ("", TYPES.MYSQL_TYPE_VAR_STRING, CHARSET_NUMBERS["utf8_general_ci"]),  # None or '' ?
    "@@interactive_timeout": (28800, TYPES.MYSQL_TYPE_LONGLONG, CHARSET_NUMBERS["binary"]),
    "@@license": ("GPL", TYPES.MYSQL_TYPE_VAR_STRING, CHARSET_NUMBERS["utf8_general_ci"]),
    "@@lower_case_table_names": (0, TYPES.MYSQL_TYPE_LONGLONG, CHARSET_NUMBERS["binary"]),
    "@@GLOBAL.lower_case_table_names": (0, TYPES.MYSQL_TYPE_LONGLONG, CHARSET_NUMBERS["binary"]),
    "@@max_allowed_packet": (16777216, TYPES.MYSQL_TYPE_LONGLONG, CHARSET_NUMBERS["binary"]),
    "@@net_buffer_length": (16384, TYPES.MYSQL_TYPE_LONGLONG, CHARSET_NUMBERS["binary"]),
    "@@net_write_timeout": (60, TYPES.MYSQL_TYPE_LONGLONG, CHARSET_NUMBERS["binary"]),
    "@@query_cache_size": (16777216, TYPES.MYSQL_TYPE_LONGLONG, CHARSET_NUMBERS["binary"]),
    "@@query_cache_type": ("OFF", TYPES.MYSQL_TYPE_VAR_STRING, CHARSET_NUMBERS["utf8_general_ci"]),
    "@@sql_mode": (
        "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION",
        TYPES.MYSQL_TYPE_VAR_STRING,
        CHARSET_NUMBERS["utf8_general_ci"],
    ),
    # '@@system_time_zone': ('MSK', TYPES.MYSQL_TYPE_VAR_STRING, CHARSET_NUMBERS['utf8_general_ci']),
    "@@system_time_zone": ("UTC", TYPES.MYSQL_TYPE_VAR_STRING, CHARSET_NUMBERS["utf8_general_ci"]),
    "@@time_zone": ("SYSTEM", TYPES.MYSQL_TYPE_VAR_STRING, CHARSET_NUMBERS["utf8_general_ci"]),
    "@@session.tx_isolation": ("REPEATABLE-READ", TYPES.MYSQL_TYPE_VAR_STRING, CHARSET_NUMBERS["utf8_general_ci"]),
    "@@tx_isolation": ("REPEATABLE-READ", TYPES.MYSQL_TYPE_VAR_STRING, CHARSET_NUMBERS["utf8_general_ci"]),
    "@@wait_timeout": (28800, TYPES.MYSQL_TYPE_LONGLONG, CHARSET_NUMBERS["binary"]),
    "@@session.tx_read_only": ("0", TYPES.MYSQL_TYPE_VAR_STRING, CHARSET_NUMBERS["utf8_general_ci"]),
    "@@version_comment": ("(MindsDB)", TYPES.MYSQL_TYPE_VAR_STRING, CHARSET_NUMBERS["utf8_general_ci"]),
    "@@version": ("8.0.17", TYPES.MYSQL_TYPE_VAR_STRING, CHARSET_NUMBERS["utf8_general_ci"]),
    "@@collation_connection": ("utf8_general_ci", TYPES.MYSQL_TYPE_VAR_STRING, CHARSET_NUMBERS["utf8_general_ci"]),
    "@@performance_schema": (1, TYPES.MYSQL_TYPE_LONGLONG, CHARSET_NUMBERS["binary"]),
    "@@GLOBAL.transaction_isolation": (
        "REPEATABLE-READ",
        TYPES.MYSQL_TYPE_VAR_STRING,
        CHARSET_NUMBERS["utf8_general_ci"],
    ),
    "@@transaction_isolation": ("REPEATABLE-READ", TYPES.MYSQL_TYPE_VAR_STRING, CHARSET_NUMBERS["utf8_general_ci"]),
    "@@event_scheduler": ("OFF", TYPES.MYSQL_TYPE_VAR_STRING, CHARSET_NUMBERS["utf8_general_ci"]),
    "@@default_storage_engine": ("InnoDB", TYPES.MYSQL_TYPE_VAR_STRING, CHARSET_NUMBERS["utf8_general_ci"]),
    "@@default_tmp_storage_engine": ("InnoDB", TYPES.MYSQL_TYPE_VAR_STRING, CHARSET_NUMBERS["utf8_general_ci"]),
}


class SESSION_TRACK(object):
    __slots__ = ()
    SESSION_TRACK_SYSTEM_VARIABLES = 0x00
    SESSION_TRACK_SCHEMA = 0x01
    SESSION_TRACK_STATE_CHANGE = 0x02
    SESSION_TRACK_GTIDS = 0x03
    SESSION_TRACK_TRANSACTION_CHARACTERISTICS = 0x04
    SESSION_TRACK_TRANSACTION_STATE = 0x05


SESSION_TRACK = SESSION_TRACK()

ALL = vars()


def VAR_NAME(val, prefix=""):
    global ALL

    for key in ALL.keys():
        value = ALL[key]
        if value == val and key != "val":
            if prefix == "" or (prefix != "" and prefix == key[: len(prefix)]):
                return key
    return None


def getConstName(consts, value):
    attrs = [x for x in dir(consts) if x.startswith("__") is False]
    constNames = {getattr(consts, x): x for x in attrs}
    if value in constNames:
        return constNames[value]
    return None
