import abc
import json
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, TypeVar, Union, overload

from dataclasses_json.cfg import config, LetterCase
from dataclasses_json.core import (Json, _ExtendedEncoder, _asdict,
                                   _decode_dataclass)
from dataclasses_json.mm import (JsonData, SchemaType, build_schema)
from dataclasses_json.undefined import Undefined
from dataclasses_json.utils import (_handle_undefined_parameters_safe,
                                    _undefined_parameter_action_safe)

A = TypeVar('A', bound="DataClassJsonMixin")
T = TypeVar('T')
Fields = List[Tuple[str, Any]]


class DataClassJsonMixin(abc.ABC):
    """
    DataClassJsonMixin is an ABC that functions as a Mixin.

    As with other ABCs, it should not be instantiated directly.
    """
    dataclass_json_config: Optional[dict] = None

    def to_json(self,
                *,
                skipkeys: bool = False,
                ensure_ascii: bool = True,
                check_circular: bool = True,
                allow_nan: bool = True,
                indent: Optional[Union[int, str]] = None,
                separators: Optional[Tuple[str, str]] = None,
                default: Optional[Callable] = None,
                sort_keys: bool = False,
                **kw) -> str:
        return json.dumps(self.to_dict(encode_json=False),
                          cls=_ExtendedEncoder,
                          skipkeys=skipkeys,
                          ensure_ascii=ensure_ascii,
                          check_circular=check_circular,
                          allow_nan=allow_nan,
                          indent=indent,
                          separators=separators,
                          default=default,
                          sort_keys=sort_keys,
                          **kw)

    @classmethod
    def from_json(cls: Type[A],
                  s: JsonData,
                  *,
                  parse_float=None,
                  parse_int=None,
                  parse_constant=None,
                  infer_missing=False,
                  **kw) -> A:
        kvs = json.loads(s,
                         parse_float=parse_float,
                         parse_int=parse_int,
                         parse_constant=parse_constant,
                         **kw)
        return cls.from_dict(kvs, infer_missing=infer_missing)

    @classmethod
    def from_dict(cls: Type[A],
                  kvs: Json,
                  *,
                  infer_missing=False) -> A:
        return _decode_dataclass(cls, kvs, infer_missing)

    def to_dict(self, encode_json=False) -> Dict[str, Json]:
        return _asdict(self, encode_json=encode_json)

    @classmethod
    def schema(cls: Type[A],
               *,
               infer_missing: bool = False,
               only=None,
               exclude=(),
               many: bool = False,
               context=None,
               load_only=(),
               dump_only=(),
               partial: bool = False,
               unknown=None) -> "SchemaType[A]":
        Schema = build_schema(cls, DataClassJsonMixin, infer_missing, partial)

        if unknown is None:
            undefined_parameter_action = _undefined_parameter_action_safe(cls)
            if undefined_parameter_action is not None:
                # We can just make use of the same-named mm keywords
                unknown = undefined_parameter_action.name.lower()

        return Schema(only=only,
                      exclude=exclude,
                      many=many,
                      context=context,
                      load_only=load_only,
                      dump_only=dump_only,
                      partial=partial,
                      unknown=unknown)


@overload
def dataclass_json(_cls: None = ..., *, letter_case: Optional[LetterCase] = ...,
                   undefined: Optional[Union[str, Undefined]] = ...) -> Callable[[Type[T]], Type[T]]: ...


@overload
def dataclass_json(_cls: Type[T], *, letter_case: Optional[LetterCase] = ...,
                   undefined: Optional[Union[str, Undefined]] = ...) -> Type[T]: ...


def dataclass_json(_cls: Optional[Type[T]] = None, *, letter_case: Optional[LetterCase] = None,
                   undefined: Optional[Union[str, Undefined]] = None) -> Union[Callable[[Type[T]], Type[T]], Type[T]]:
    """
    Based on the code in the `dataclasses` module to handle optional-parens
    decorators. See example below:

    @dataclass_json
    @dataclass_json(letter_case=LetterCase.CAMEL)
    class Example:
        ...
    """

    def wrap(cls: Type[T]) -> Type[T]:
        return _process_class(cls, letter_case, undefined)

    if _cls is None:
        return wrap
    return wrap(_cls)


def _process_class(cls: Type[T], letter_case: Optional[LetterCase],
                   undefined: Optional[Union[str, Undefined]]) -> Type[T]:
    if letter_case is not None or undefined is not None:
        cls.dataclass_json_config = config(letter_case=letter_case,  # type: ignore[attr-defined]
                                           undefined=undefined)['dataclasses_json']

    cls.to_json = DataClassJsonMixin.to_json  # type: ignore[attr-defined]
    # unwrap and rewrap classmethod to tag it to cls rather than the literal
    # DataClassJsonMixin ABC
    cls.from_json = classmethod(DataClassJsonMixin.from_json.__func__)  # type: ignore[attr-defined]
    cls.to_dict = DataClassJsonMixin.to_dict  # type: ignore[attr-defined]
    cls.from_dict = classmethod(DataClassJsonMixin.from_dict.__func__)  # type: ignore[attr-defined]
    cls.schema = classmethod(DataClassJsonMixin.schema.__func__)  # type: ignore[attr-defined]

    cls.__init__ = _handle_undefined_parameters_safe(cls, kvs=(),  # type: ignore[attr-defined,method-assign]
                                                     usage="init")
    # register cls as a virtual subclass of DataClassJsonMixin
    DataClassJsonMixin.register(cls)
    return cls
