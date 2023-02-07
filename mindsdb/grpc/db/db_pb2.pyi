import mindsdb.grpc.db.common_pb2 as _common_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class BinaryQueryContext(_message.Message):
    __slots__ = ["context", "query"]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    context: HandlerContext
    query: bytes
    def __init__(self, context: _Optional[_Union[HandlerContext, _Mapping]] = ..., query: _Optional[bytes] = ...) -> None: ...

class ColumnsContext(_message.Message):
    __slots__ = ["context", "table"]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    TABLE_FIELD_NUMBER: _ClassVar[int]
    context: HandlerContext
    table: str
    def __init__(self, context: _Optional[_Union[HandlerContext, _Mapping]] = ..., table: _Optional[str] = ...) -> None: ...

class HandlerContext(_message.Message):
    __slots__ = ["context", "handler_params", "handler_type"]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    HANDLER_PARAMS_FIELD_NUMBER: _ClassVar[int]
    HANDLER_TYPE_FIELD_NUMBER: _ClassVar[int]
    context: str
    handler_params: str
    handler_type: str
    def __init__(self, context: _Optional[str] = ..., handler_type: _Optional[str] = ..., handler_params: _Optional[str] = ...) -> None: ...

class NativeQueryContext(_message.Message):
    __slots__ = ["context", "query"]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    context: HandlerContext
    query: str
    def __init__(self, context: _Optional[_Union[HandlerContext, _Mapping]] = ..., query: _Optional[str] = ...) -> None: ...
