import mindsdb.grpc.ml.common_pb2 as _common_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CreateCall(_message.Message):
    __slots__ = ["args", "context", "df", "target"]
    ARGS_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    args: str
    context: HandlerContextML
    df: bytes
    target: str
    def __init__(self, context: _Optional[_Union[HandlerContextML, _Mapping]] = ..., target: _Optional[str] = ..., df: _Optional[bytes] = ..., args: _Optional[str] = ...) -> None: ...

class HandlerContextML(_message.Message):
    __slots__ = ["context", "handler_params", "integration_id", "predictor_id"]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    HANDLER_PARAMS_FIELD_NUMBER: _ClassVar[int]
    INTEGRATION_ID_FIELD_NUMBER: _ClassVar[int]
    PREDICTOR_ID_FIELD_NUMBER: _ClassVar[int]
    context: str
    handler_params: str
    integration_id: int
    predictor_id: int
    def __init__(self, integration_id: _Optional[int] = ..., predictor_id: _Optional[int] = ..., context: _Optional[str] = ..., handler_params: _Optional[str] = ...) -> None: ...

class PredictCall(_message.Message):
    __slots__ = ["args", "context", "df"]
    ARGS_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    args: str
    context: HandlerContextML
    df: bytes
    def __init__(self, context: _Optional[_Union[HandlerContextML, _Mapping]] = ..., df: _Optional[bytes] = ..., args: _Optional[str] = ...) -> None: ...

class UpdateCall(_message.Message):
    __slots__ = ["args", "context", "df"]
    ARGS_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    args: str
    context: HandlerContextML
    df: bytes
    def __init__(self, context: _Optional[_Union[HandlerContextML, _Mapping]] = ..., df: _Optional[bytes] = ..., args: _Optional[str] = ...) -> None: ...
