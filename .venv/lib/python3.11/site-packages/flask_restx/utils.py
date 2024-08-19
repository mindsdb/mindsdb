import re
import warnings
import typing

from collections import OrderedDict
from copy import deepcopy

from ._http import HTTPStatus


FIRST_CAP_RE = re.compile("(.)([A-Z][a-z]+)")
ALL_CAP_RE = re.compile("([a-z0-9])([A-Z])")


__all__ = (
    "merge",
    "camel_to_dash",
    "default_id",
    "not_none",
    "not_none_sorted",
    "unpack",
    "BaseResponse",
    "import_check_view_func",
)


def import_werkzeug_response():
    """Resolve `werkzeug` `Response` class import because
    `BaseResponse` was renamed in version 2.* to `Response`"""
    import importlib.metadata

    werkzeug_major = int(importlib.metadata.version("werkzeug").split(".")[0])
    if werkzeug_major < 2:
        from werkzeug.wrappers import BaseResponse

        return BaseResponse

    from werkzeug.wrappers import Response

    return Response


BaseResponse = import_werkzeug_response()


class FlaskCompatibilityWarning(DeprecationWarning):
    pass


def merge(first, second):
    """
    Recursively merges two dictionaries.

    Second dictionary values will take precedence over those from the first one.
    Nested dictionaries are merged too.

    :param dict first: The first dictionary
    :param dict second: The second dictionary
    :return: the resulting merged dictionary
    :rtype: dict
    """
    if not isinstance(second, dict):
        return second
    result = deepcopy(first)
    for key, value in second.items():
        if key in result and isinstance(result[key], dict):
            result[key] = merge(result[key], value)
        else:
            result[key] = deepcopy(value)
    return result


def camel_to_dash(value):
    """
    Transform a CamelCase string into a low_dashed one

    :param str value: a CamelCase string to transform
    :return: the low_dashed string
    :rtype: str
    """
    first_cap = FIRST_CAP_RE.sub(r"\1_\2", value)
    return ALL_CAP_RE.sub(r"\1_\2", first_cap).lower()


def default_id(resource, method):
    """Default operation ID generator"""
    return "{0}_{1}".format(method, camel_to_dash(resource))


def not_none(data):
    """
    Remove all keys where value is None

    :param dict data: A dictionary with potentially some values set to None
    :return: The same dictionary without the keys with values to ``None``
    :rtype: dict
    """
    return dict((k, v) for k, v in data.items() if v is not None)


def not_none_sorted(data):
    """
    Remove all keys where value is None

    :param OrderedDict data: A dictionary with potentially some values set to None
    :return: The same dictionary without the keys with values to ``None``
    :rtype: OrderedDict
    """
    return OrderedDict((k, v) for k, v in sorted(data.items()) if v is not None)


def unpack(response, default_code=HTTPStatus.OK):
    """
    Unpack a Flask standard response.

    Flask response can be:
    - a single value
    - a 2-tuple ``(value, code)``
    - a 3-tuple ``(value, code, headers)``

    .. warning::

        When using this function, you must ensure that the tuple is not the response data.
        To do so, prefer returning list instead of tuple for listings.

    :param response: A Flask style response
    :param int default_code: The HTTP code to use as default if none is provided
    :return: a 3-tuple ``(data, code, headers)``
    :rtype: tuple
    :raise ValueError: if the response does not have one of the expected format
    """
    if not isinstance(response, tuple):
        # data only
        return response, default_code, {}
    elif len(response) == 1:
        # data only as tuple
        return response[0], default_code, {}
    elif len(response) == 2:
        # data and code
        data, code = response
        return data, code, {}
    elif len(response) == 3:
        # data, code and headers
        data, code, headers = response
        return data, code or default_code, headers
    else:
        raise ValueError("Too many response values")


def to_view_name(view_func: typing.Callable) -> str:
    """Helper that returns the default endpoint for a given
    function. This always is the function name.

    Note: copy of simple flask internal helper
    """
    assert view_func is not None, "expected view func if endpoint is not provided."
    return view_func.__name__


def import_check_view_func():
    """
    Resolve import flask _endpoint_from_view_func.

    Show warning if function cannot be found and provide copy of last known implementation.

    Note: This helper method exists because reoccurring problem with flask function, but
    actual method body remaining the same in each flask version.
    """
    import importlib.metadata

    flask_version = importlib.metadata.version("flask").split(".")
    try:
        if flask_version[0] == "1":
            from flask.helpers import _endpoint_from_view_func
        elif flask_version[0] == "2":
            from flask.scaffold import _endpoint_from_view_func
        elif flask_version[0] == "3":
            from flask.sansio.scaffold import _endpoint_from_view_func
        else:
            warnings.simplefilter("once", FlaskCompatibilityWarning)
            _endpoint_from_view_func = None
    except ImportError:
        warnings.simplefilter("once", FlaskCompatibilityWarning)
        _endpoint_from_view_func = None
    if _endpoint_from_view_func is None:
        _endpoint_from_view_func = to_view_name
    return _endpoint_from_view_func
