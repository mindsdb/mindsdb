import abc
import dataclasses
import functools
import inspect
import sys
from dataclasses import Field, fields
from typing import Any, Callable, Dict, Optional, Tuple, Union, Type, get_type_hints
from enum import Enum

from marshmallow.exceptions import ValidationError  # type: ignore

from dataclasses_json.utils import CatchAllVar

KnownParameters = Dict[str, Any]
UnknownParameters = Dict[str, Any]


class _UndefinedParameterAction(abc.ABC):
    @staticmethod
    @abc.abstractmethod
    def handle_from_dict(cls, kvs: Dict[Any, Any]) -> Dict[str, Any]:
        """
        Return the parameters to initialize the class with.
        """
        pass

    @staticmethod
    def handle_to_dict(obj, kvs: Dict[Any, Any]) -> Dict[Any, Any]:
        """
        Return the parameters that will be written to the output dict
        """
        return kvs

    @staticmethod
    def handle_dump(obj) -> Dict[Any, Any]:
        """
        Return the parameters that will be added to the schema dump.
        """
        return {}

    @staticmethod
    def create_init(obj) -> Callable:
        return obj.__init__

    @staticmethod
    def _separate_defined_undefined_kvs(cls, kvs: Dict) -> \
            Tuple[KnownParameters, UnknownParameters]:
        """
        Returns a 2 dictionaries: defined and undefined parameters
        """
        class_fields = fields(cls)
        field_names = [field.name for field in class_fields]
        unknown_given_parameters = {k: v for k, v in kvs.items() if
                                    k not in field_names}
        known_given_parameters = {k: v for k, v in kvs.items() if
                                  k in field_names}
        return known_given_parameters, unknown_given_parameters


class _RaiseUndefinedParameters(_UndefinedParameterAction):
    """
    This action raises UndefinedParameterError if it encounters an undefined
    parameter during initialization.
    """

    @staticmethod
    def handle_from_dict(cls, kvs: Dict) -> Dict[str, Any]:
        known, unknown = \
            _UndefinedParameterAction._separate_defined_undefined_kvs(
                cls=cls, kvs=kvs)
        if len(unknown) > 0:
            raise UndefinedParameterError(
                f"Received undefined initialization arguments {unknown}")
        return known


CatchAll = Optional[CatchAllVar]


class _IgnoreUndefinedParameters(_UndefinedParameterAction):
    """
    This action does nothing when it encounters undefined parameters.
    The undefined parameters can not be retrieved after the class has been
    created.
    """

    @staticmethod
    def handle_from_dict(cls, kvs: Dict) -> Dict[str, Any]:
        known_given_parameters, _ = \
            _UndefinedParameterAction._separate_defined_undefined_kvs(
                cls=cls, kvs=kvs)
        return known_given_parameters

    @staticmethod
    def create_init(obj) -> Callable:
        original_init = obj.__init__
        init_signature = inspect.signature(original_init)

        @functools.wraps(obj.__init__)
        def _ignore_init(self, *args, **kwargs):
            known_kwargs, _ = \
                _CatchAllUndefinedParameters._separate_defined_undefined_kvs(
                    obj, kwargs)
            num_params_takeable = len(
                init_signature.parameters) - 1  # don't count self
            num_args_takeable = num_params_takeable - len(known_kwargs)

            args = args[:num_args_takeable]
            bound_parameters = init_signature.bind_partial(self, *args,
                                                           **known_kwargs)
            bound_parameters.apply_defaults()

            arguments = bound_parameters.arguments
            arguments.pop("self", None)
            final_parameters = \
                _IgnoreUndefinedParameters.handle_from_dict(obj, arguments)
            original_init(self, **final_parameters)

        return _ignore_init


class _CatchAllUndefinedParameters(_UndefinedParameterAction):
    """
    This class allows to add a field of type utils.CatchAll which acts as a
    dictionary into which all
    undefined parameters will be written.
    These parameters are not affected by LetterCase.
    If no undefined parameters are given, this dictionary will be empty.
    """

    class _SentinelNoDefault:
        pass

    @staticmethod
    def handle_from_dict(cls, kvs: Dict) -> Dict[str, Any]:
        known, unknown = _UndefinedParameterAction \
            ._separate_defined_undefined_kvs(cls=cls, kvs=kvs)
        catch_all_field = _CatchAllUndefinedParameters._get_catch_all_field(
            cls=cls)

        if catch_all_field.name in known:

            already_parsed = isinstance(known[catch_all_field.name], dict)
            default_value = _CatchAllUndefinedParameters._get_default(
                catch_all_field=catch_all_field)
            received_default = default_value == known[catch_all_field.name]

            value_to_write: Any
            if received_default and len(unknown) == 0:
                value_to_write = default_value
            elif received_default and len(unknown) > 0:
                value_to_write = unknown
            elif already_parsed:
                # Did not receive default
                value_to_write = known[catch_all_field.name]
                if len(unknown) > 0:
                    value_to_write.update(unknown)
            else:
                error_message = f"Received input field with " \
                                f"same name as catch-all field: " \
                                f"'{catch_all_field.name}': " \
                                f"'{known[catch_all_field.name]}'"
                raise UndefinedParameterError(error_message)
        else:
            value_to_write = unknown

        known[catch_all_field.name] = value_to_write
        return known

    @staticmethod
    def _get_default(catch_all_field: Field) -> Any:
        # access to the default factory currently causes
        # a false-positive mypy error (16. Dec 2019):
        # https://github.com/python/mypy/issues/6910

        # noinspection PyProtectedMember
        has_default = not isinstance(catch_all_field.default,
                                     dataclasses._MISSING_TYPE)
        # noinspection PyProtectedMember
        has_default_factory = not isinstance(catch_all_field.default_factory,
                                             # type: ignore
                                             dataclasses._MISSING_TYPE)
        # TODO: black this for proper formatting
        default_value: Union[
            Type[_CatchAllUndefinedParameters._SentinelNoDefault], Any] = _CatchAllUndefinedParameters\
            ._SentinelNoDefault

        if has_default:
            default_value = catch_all_field.default
        elif has_default_factory:
            # This might be unwanted if the default factory constructs
            # something expensive,
            # because we have to construct it again just for this test
            default_value = catch_all_field.default_factory()  # type: ignore

        return default_value

    @staticmethod
    def handle_to_dict(obj, kvs: Dict[Any, Any]) -> Dict[Any, Any]:
        catch_all_field = \
            _CatchAllUndefinedParameters._get_catch_all_field(obj.__class__)
        undefined_parameters = kvs.pop(catch_all_field.name)
        if isinstance(undefined_parameters, dict):
            kvs.update(
                undefined_parameters)  # If desired handle letter case here
        return kvs

    @staticmethod
    def handle_dump(obj) -> Dict[Any, Any]:
        catch_all_field = _CatchAllUndefinedParameters._get_catch_all_field(
            cls=obj)
        return getattr(obj, catch_all_field.name)

    @staticmethod
    def create_init(obj) -> Callable:
        original_init = obj.__init__
        init_signature = inspect.signature(original_init)

        @functools.wraps(obj.__init__)
        def _catch_all_init(self, *args, **kwargs):
            known_kwargs, unknown_kwargs = \
                _CatchAllUndefinedParameters._separate_defined_undefined_kvs(
                    obj, kwargs)
            num_params_takeable = len(
                init_signature.parameters) - 1  # don't count self
            if _CatchAllUndefinedParameters._get_catch_all_field(
                    obj).name not in known_kwargs:
                num_params_takeable -= 1
            num_args_takeable = num_params_takeable - len(known_kwargs)

            args, unknown_args = args[:num_args_takeable], args[
                                                           num_args_takeable:]
            bound_parameters = init_signature.bind_partial(self, *args,
                                                           **known_kwargs)

            unknown_args = {f"_UNKNOWN{i}": v for i, v in
                            enumerate(unknown_args)}
            arguments = bound_parameters.arguments
            arguments.update(unknown_args)
            arguments.update(unknown_kwargs)
            arguments.pop("self", None)
            final_parameters = _CatchAllUndefinedParameters.handle_from_dict(
                obj, arguments)
            original_init(self, **final_parameters)

        return _catch_all_init

    @staticmethod
    def _get_catch_all_field(cls) -> Field:
        cls_globals = vars(sys.modules[cls.__module__])
        types = get_type_hints(cls, globalns=cls_globals)
        catch_all_fields = list(
            filter(lambda f: types[f.name] == Optional[CatchAllVar], fields(cls)))
        number_of_catch_all_fields = len(catch_all_fields)
        if number_of_catch_all_fields == 0:
            raise UndefinedParameterError(
                "No field of type dataclasses_json.CatchAll defined")
        elif number_of_catch_all_fields > 1:
            raise UndefinedParameterError(
                f"Multiple catch-all fields supplied: "
                f"{number_of_catch_all_fields}.")
        else:
            return catch_all_fields[0]


class Undefined(Enum):
    """
    Choose the behavior what happens when an undefined parameter is encountered
    during class initialization.
    """
    INCLUDE = _CatchAllUndefinedParameters
    RAISE = _RaiseUndefinedParameters
    EXCLUDE = _IgnoreUndefinedParameters


class UndefinedParameterError(ValidationError):
    """
    Raised when something has gone wrong handling undefined parameters.
    """
    pass
