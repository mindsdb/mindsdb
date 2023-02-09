import mindsdb.grpc.ml.common_pb2 as _common_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class BinaryQueryContextML(_message.Message):
    __slots__ = ["context", "query"]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    context: HandlerContextML
    query: bytes
    def __init__(self, context: _Optional[_Union[HandlerContextML, _Mapping]] = ..., query: _Optional[bytes] = ...) -> None: ...

class ColumnsContextML(_message.Message):
    __slots__ = ["context", "table"]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    TABLE_FIELD_NUMBER: _ClassVar[int]
    context: HandlerContextML
    table: str
    def __init__(self, context: _Optional[_Union[HandlerContextML, _Mapping]] = ..., table: _Optional[str] = ...) -> None: ...

class HandlerContextML(_message.Message):
    __slots__ = ["context", "handler_params"]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    HANDLER_PARAMS_FIELD_NUMBER: _ClassVar[int]
    context: str
    handler_params: str
    def __init__(self, context: _Optional[str] = ..., handler_params: _Optional[str] = ...) -> None: ...

class LearnContextML(_message.Message):
    __slots__ = ["context", "learn_params"]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    LEARN_PARAMS_FIELD_NUMBER: _ClassVar[int]
    context: HandlerContextML
    learn_params: LearnParams
    def __init__(self, context: _Optional[_Union[HandlerContextML, _Mapping]] = ..., learn_params: _Optional[_Union[LearnParams, _Mapping]] = ...) -> None: ...

class LearnParams(_message.Message):
    __slots__ = ["data_integration_ref", "fetch_data_query", "is_retrain", "join_learn_process", "label", "model_name", "problem_definition", "project_name", "set_active", "version"]
    DATA_INTEGRATION_REF_FIELD_NUMBER: _ClassVar[int]
    FETCH_DATA_QUERY_FIELD_NUMBER: _ClassVar[int]
    IS_RETRAIN_FIELD_NUMBER: _ClassVar[int]
    JOIN_LEARN_PROCESS_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    PROBLEM_DEFINITION_FIELD_NUMBER: _ClassVar[int]
    PROJECT_NAME_FIELD_NUMBER: _ClassVar[int]
    SET_ACTIVE_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    data_integration_ref: str
    fetch_data_query: str
    is_retrain: bool
    join_learn_process: bool
    label: str
    model_name: str
    problem_definition: str
    project_name: str
    set_active: bool
    version: int
    def __init__(self, model_name: _Optional[str] = ..., project_name: _Optional[str] = ..., data_integration_ref: _Optional[str] = ..., fetch_data_query: _Optional[str] = ..., problem_definition: _Optional[str] = ..., join_learn_process: bool = ..., label: _Optional[str] = ..., version: _Optional[int] = ..., is_retrain: bool = ..., set_active: bool = ...) -> None: ...

class NativeQueryContextML(_message.Message):
    __slots__ = ["context", "query"]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    context: HandlerContextML
    query: str
    def __init__(self, context: _Optional[_Union[HandlerContextML, _Mapping]] = ..., query: _Optional[str] = ...) -> None: ...

class PredictContextML(_message.Message):
    __slots__ = ["context", "predict_params"]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    PREDICT_PARAMS_FIELD_NUMBER: _ClassVar[int]
    context: HandlerContextML
    predict_params: PredictParams
    def __init__(self, context: _Optional[_Union[HandlerContextML, _Mapping]] = ..., predict_params: _Optional[_Union[PredictParams, _Mapping]] = ...) -> None: ...

class PredictParams(_message.Message):
    __slots__ = ["data", "model_name", "params", "pred_format", "project_name", "version"]
    DATA_FIELD_NUMBER: _ClassVar[int]
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    PRED_FORMAT_FIELD_NUMBER: _ClassVar[int]
    PROJECT_NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    data: bytes
    model_name: str
    params: str
    pred_format: str
    project_name: str
    version: int
    def __init__(self, model_name: _Optional[str] = ..., data: _Optional[bytes] = ..., pred_format: _Optional[str] = ..., project_name: _Optional[str] = ..., version: _Optional[int] = ..., params: _Optional[str] = ...) -> None: ...

class UpdateContextML(_message.Message):
    __slots__ = ["context", "update_params"]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    UPDATE_PARAMS_FIELD_NUMBER: _ClassVar[int]
    context: HandlerContextML
    update_params: UpdateParams
    def __init__(self, context: _Optional[_Union[HandlerContextML, _Mapping]] = ..., update_params: _Optional[_Union[UpdateParams, _Mapping]] = ...) -> None: ...

class UpdateParams(_message.Message):
    __slots__ = ["data_integration_ref", "fetch_data_query", "join_learn_process", "label", "model_name", "project_name", "set_active", "version"]
    DATA_INTEGRATION_REF_FIELD_NUMBER: _ClassVar[int]
    FETCH_DATA_QUERY_FIELD_NUMBER: _ClassVar[int]
    JOIN_LEARN_PROCESS_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    PROJECT_NAME_FIELD_NUMBER: _ClassVar[int]
    SET_ACTIVE_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    data_integration_ref: str
    fetch_data_query: str
    join_learn_process: bool
    label: str
    model_name: str
    project_name: str
    set_active: bool
    version: int
    def __init__(self, model_name: _Optional[str] = ..., project_name: _Optional[str] = ..., data_integration_ref: _Optional[str] = ..., fetch_data_query: _Optional[str] = ..., join_learn_process: bool = ..., label: _Optional[str] = ..., version: _Optional[int] = ..., set_active: bool = ...) -> None: ...
