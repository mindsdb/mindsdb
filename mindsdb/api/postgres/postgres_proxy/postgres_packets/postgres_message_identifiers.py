from enum import Enum


class PostgresBackendMessageIdentifier(Enum):
    NOTICE_RESPONSE = b'N'
    AUTHENTICATION_REQUEST = b'R'
    READY_FOR_QUERY = b'Z'
    COMPLETE = b'C'
    ERROR = b'E'
    ROW_DESCRIPTION = b'T'
    DATA_ROW = b'D'
    NEGOTIATE_VERSION = b'v'
    PARAMETER = b'S'
    PARSE_COMPLETE = b'1'
    BIND_COMPLETE = b'2'
    PARAMETER_DESCRIPTION = b't'


class PostgresFrontendMessageIdentifier(Enum):
    EXECUTE = b'E'
    QUERY = b'Q'
    TERMINATE = b'X'
    SSL_REQUEST = b'F'
    PARSE = b'P'
    BIND = b'B'
    SYNC = b'S'
    DESCRIBE = b'D'


class PostgresAuthType(Enum):
    PASSWORD = b'p'
