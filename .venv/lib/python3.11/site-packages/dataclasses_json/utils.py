import inspect
import sys
from datetime import datetime, timezone
from collections import Counter
from dataclasses import is_dataclass  # type: ignore
from typing import (Collection, Mapping, Optional, TypeVar, Any, Type, Tuple,
                    Union, cast)


def _get_type_cons(type_):
    """More spaghetti logic for 3.6 vs. 3.7"""
    if sys.version_info.minor == 6:
        try:
            cons = type_.__extra__
        except AttributeError:
            try:
                cons = type_.__origin__
            except AttributeError:
                cons = type_
            else:
                cons = type_ if cons is None else cons
        else:
            try:
                cons = type_.__origin__ if cons is None else cons
            except AttributeError:
                cons = type_
    else:
        cons = type_.__origin__
    return cons


_NO_TYPE_ORIGIN = object()


def _get_type_origin(type_):
    """Some spaghetti logic to accommodate differences between 3.6 and 3.7 in
    the typing api"""
    try:
        origin = type_.__origin__
    except AttributeError:
        # Issue #341 and PR #346:
        # For some cases, the type_.__origin__ exists but is set to None
        origin = _NO_TYPE_ORIGIN

    if sys.version_info.minor == 6:
        try:
            origin = type_.__extra__
        except AttributeError:
            origin = type_
        else:
            origin = type_ if origin in (None, _NO_TYPE_ORIGIN) else origin
    elif origin is _NO_TYPE_ORIGIN:
        origin = type_
    return origin


def _hasargs(type_, *args):
    try:
        res = all(arg in type_.__args__ for arg in args)
    except AttributeError:
        return False
    except TypeError:
        if (type_.__args__ is None):
            return False
        else:
            raise
    else:
        return res


class _NoArgs(object):
    def __bool__(self):
        return False

    def __len__(self):
        return 0

    def __iter__(self):
        return self

    def __next__(self):
        raise StopIteration


_NO_ARGS = _NoArgs()


def _get_type_args(tp: Type, default: Union[Tuple[Type, ...], _NoArgs] = _NO_ARGS) -> \
        Union[Tuple[Type, ...], _NoArgs]:
    if hasattr(tp, '__args__'):
        if tp.__args__ is not None:
            return tp.__args__
    return default


def _get_type_arg_param(tp: Type, index: int) -> Union[Type, _NoArgs]:
    _args = _get_type_args(tp)
    if _args is not _NO_ARGS:
        try:
            return cast(Tuple[Type, ...], _args)[index]
        except (TypeError, IndexError, NotImplementedError):
            pass

    return _NO_ARGS


def _isinstance_safe(o, t):
    try:
        result = isinstance(o, t)
    except Exception:
        return False
    else:
        return result


def _issubclass_safe(cls, classinfo):
    try:
        return issubclass(cls, classinfo)
    except Exception:
        return (_is_new_type_subclass_safe(cls, classinfo)
                if _is_new_type(cls)
                else False)


def _is_new_type_subclass_safe(cls, classinfo):
    super_type = getattr(cls, "__supertype__", None)

    if super_type:
        return _is_new_type_subclass_safe(super_type, classinfo)

    try:
        return issubclass(cls, classinfo)
    except Exception:
        return False


def _is_new_type(type_):
    return inspect.isfunction(type_) and hasattr(type_, "__supertype__")


def _is_optional(type_):
    return (_issubclass_safe(type_, Optional) or
            _hasargs(type_, type(None)) or
            type_ is Any)


def _is_counter(type_):
    return _issubclass_safe(_get_type_origin(type_), Counter)


def _is_mapping(type_):
    return _issubclass_safe(_get_type_origin(type_), Mapping)


def _is_collection(type_):
    return _issubclass_safe(_get_type_origin(type_), Collection)


def _is_tuple(type_):
    return _issubclass_safe(_get_type_origin(type_), Tuple)


def _is_nonstr_collection(type_):
    return (_issubclass_safe(_get_type_origin(type_), Collection)
            and not _issubclass_safe(type_, str))


def _is_generic_dataclass(type_):
    return is_dataclass(_get_type_origin(type_))


def _timestamp_to_dt_aware(timestamp: float):
    tz = datetime.now(timezone.utc).astimezone().tzinfo
    dt = datetime.fromtimestamp(timestamp, tz=tz)
    return dt


def _undefined_parameter_action_safe(cls):
    try:
        if cls.dataclass_json_config is None:
            return
        action_enum = cls.dataclass_json_config['undefined']
    except (AttributeError, KeyError):
        return

    if action_enum is None or action_enum.value is None:
        return

    return action_enum


def _handle_undefined_parameters_safe(cls, kvs, usage: str):
    """
    Checks if an undefined parameters action is defined and performs the
    according action.
    """
    undefined_parameter_action = _undefined_parameter_action_safe(cls)
    usage = usage.lower()
    if undefined_parameter_action is None:
        return kvs if usage != "init" else cls.__init__
    if usage == "from":
        return undefined_parameter_action.value.handle_from_dict(cls=cls,
                                                                 kvs=kvs)
    elif usage == "to":
        return undefined_parameter_action.value.handle_to_dict(obj=cls,
                                                               kvs=kvs)
    elif usage == "dump":
        return undefined_parameter_action.value.handle_dump(obj=cls)
    elif usage == "init":
        return undefined_parameter_action.value.create_init(obj=cls)
    else:
        raise ValueError(
            f"usage must be one of ['to', 'from', 'dump', 'init'], "
            f"but is '{usage}'")


# Define a type for the CatchAll field
# https://stackoverflow.com/questions/59360567/define-a-custom-type-that-behaves-like-typing-any
CatchAllVar = TypeVar("CatchAllVar", bound=Mapping)
