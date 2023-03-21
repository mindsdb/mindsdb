from enum import Enum


class PostgresBackendMessageIdentifier(Enum):
    NOTICE_RESPONSE = b'N'
    AUTHENTICATION_REQUEST = b'R'
    READY_FOR_QUERY = b'Z'
    COMPLETE = b'C'
    ERROR = b'E'
    ROW_DESCRIPTION = b'T'
    DATA_ROW = b'D'


class PostgresFrontendMessageIdentifier(Enum):
    EXECUTE = b'E'
    QUERY = b'Q'
    TERMINATE = b'X'


class PostgresAuthType(Enum):
    PASSWORD = b'p'


