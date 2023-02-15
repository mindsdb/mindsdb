from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class DefaultDBContext(_message.Message):
    __slots__ = ["context", "new_db"]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    NEW_DB_FIELD_NUMBER: _ClassVar[int]
    context: ExecutorContext
    new_db: str
    def __init__(self, context: _Optional[_Union[ExecutorContext, _Mapping]] = ..., new_db: _Optional[str] = ...) -> None: ...

class ExecutionContext(_message.Message):
    __slots__ = ["context", "sql"]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    SQL_FIELD_NUMBER: _ClassVar[int]
    context: ExecutorContext
    sql: str
    def __init__(self, context: _Optional[_Union[ExecutorContext, _Mapping]] = ..., sql: _Optional[str] = ...) -> None: ...

class ExecutorContext(_message.Message):
    __slots__ = ["connection_id", "context", "id", "session", "session_id"]
    CONNECTION_ID_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    connection_id: int
    context: str
    id: str
    session: str
    session_id: str
    def __init__(self, id: _Optional[str] = ..., connection_id: _Optional[int] = ..., session_id: _Optional[str] = ..., session: _Optional[str] = ..., context: _Optional[str] = ...) -> None: ...

class ExecutorResponse(_message.Message):
    __slots__ = ["columns", "data", "error_message", "is_executed", "params", "server_status", "session", "state_track"]
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    IS_EXECUTED_FIELD_NUMBER: _ClassVar[int]
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    SERVER_STATUS_FIELD_NUMBER: _ClassVar[int]
    SESSION_FIELD_NUMBER: _ClassVar[int]
    STATE_TRACK_FIELD_NUMBER: _ClassVar[int]
    columns: bytes
    data: bytes
    error_message: str
    is_executed: bool
    params: bytes
    server_status: str
    session: str
    state_track: str
    def __init__(self, columns: _Optional[bytes] = ..., params: _Optional[bytes] = ..., data: _Optional[bytes] = ..., state_track: _Optional[str] = ..., server_status: _Optional[str] = ..., is_executed: bool = ..., session: _Optional[str] = ..., error_message: _Optional[str] = ...) -> None: ...

class ExecutorStatusResponse(_message.Message):
    __slots__ = ["error_message", "success"]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    error_message: str
    success: bool
    def __init__(self, success: bool = ..., error_message: _Optional[str] = ...) -> None: ...

class StatementExecuteContext(_message.Message):
    __slots__ = ["context", "params"]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    context: ExecutorContext
    params: str
    def __init__(self, context: _Optional[_Union[ExecutorContext, _Mapping]] = ..., params: _Optional[str] = ...) -> None: ...
