import decimal

try:
    from collections.abc import Hashable
except ImportError:
    from collections import Hashable
from copy import deepcopy
from flask import current_app, request

from werkzeug.datastructures import MultiDict, FileStorage
from werkzeug import exceptions

from .errors import abort, SpecsError
from .marshalling import marshal
from .model import Model
from ._http import HTTPStatus


class ParseResult(dict):
    """
    The default result container as an Object dict.
    """

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)

    def __setattr__(self, name, value):
        self[name] = value


_friendly_location = {
    "json": "the JSON body",
    "form": "the post body",
    "args": "the query string",
    "values": "the post body or the query string",
    "headers": "the HTTP headers",
    "cookies": "the request's cookies",
    "files": "an uploaded file",
}

#: Maps Flask-RESTX RequestParser locations to Swagger ones
LOCATIONS = {
    "args": "query",
    "form": "formData",
    "headers": "header",
    "json": "body",
    "values": "query",
    "files": "formData",
}

#: Maps Python primitives types to Swagger ones
PY_TYPES = {
    int: "integer",
    str: "string",
    bool: "boolean",
    float: "number",
    None: "void",
}

SPLIT_CHAR = ","


class Argument(object):
    """
    :param name: Either a name or a list of option strings, e.g. foo or -f, --foo.
    :param default: The value produced if the argument is absent from the request.
    :param dest: The name of the attribute to be added to the object
        returned by :meth:`~reqparse.RequestParser.parse_args()`.
    :param bool required: Whether or not the argument may be omitted (optionals only).
    :param string action: The basic type of action to be taken when this argument
        is encountered in the request. Valid options are "store" and "append".
    :param bool ignore: Whether to ignore cases where the argument fails type conversion
    :param type: The type to which the request argument should be converted.
        If a type raises an exception, the message in the error will be returned in the response.
        Defaults to  :class:`str`.
    :param location: The attributes of the :class:`flask.Request` object
        to source the arguments from (ex: headers, args, etc.), can be an
        iterator. The last item listed takes precedence in the result set.
    :param choices: A container of the allowable values for the argument.
    :param help: A brief description of the argument, returned in the
        response when the argument is invalid. May optionally contain
        an "{error_msg}" interpolation token, which will be replaced with
        the text of the error raised by the type converter.
    :param bool case_sensitive: Whether argument values in the request are
        case sensitive or not (this will convert all values to lowercase)
    :param bool store_missing: Whether the arguments default value should
        be stored if the argument is missing from the request.
    :param bool trim: If enabled, trims whitespace around the argument.
    :param bool nullable: If enabled, allows null value in argument.
    """

    def __init__(
        self,
        name,
        default=None,
        dest=None,
        required=False,
        ignore=False,
        type=str,
        location=(
            "json",
            "values",
        ),
        choices=(),
        action="store",
        help=None,
        operators=("=",),
        case_sensitive=True,
        store_missing=True,
        trim=False,
        nullable=True,
    ):
        self.name = name
        self.default = default
        self.dest = dest
        self.required = required
        self.ignore = ignore
        self.location = location
        self.type = type
        self.choices = choices
        self.action = action
        self.help = help
        self.case_sensitive = case_sensitive
        self.operators = operators
        self.store_missing = store_missing
        self.trim = trim
        self.nullable = nullable

    def source(self, request):
        """
        Pulls values off the request in the provided location
        :param request: The flask request object to parse arguments from
        """
        if isinstance(self.location, str):
            if self.location in {"json", "get_json"}:
                value = request.get_json(silent=True)
            else:
                value = getattr(request, self.location, MultiDict())
            if callable(value):
                value = value()
            if value is not None:
                return value
        else:
            values = MultiDict()
            for l in self.location:
                if l in {"json", "get_json"}:
                    value = request.get_json(silent=True)
                else:
                    value = getattr(request, l, None)
                if callable(value):
                    value = value()
                if value is not None:
                    values.update(value)
            return values

        return MultiDict()

    def convert(self, value, op):
        # Don't cast None
        if value is None:
            if not self.nullable:
                raise ValueError("Must not be null!")
            return None

        elif isinstance(self.type, Model) and isinstance(value, dict):
            return marshal(value, self.type)

        # and check if we're expecting a filestorage and haven't overridden `type`
        # (required because the below instantiation isn't valid for FileStorage)
        elif isinstance(value, FileStorage) and self.type == FileStorage:
            return value

        try:
            return self.type(value, self.name, op)
        except TypeError:
            try:
                if self.type is decimal.Decimal:
                    return self.type(str(value), self.name)
                else:
                    return self.type(value, self.name)
            except TypeError:
                return self.type(value)

    def handle_validation_error(self, error, bundle_errors):
        """
        Called when an error is raised while parsing. Aborts the request
        with a 400 status and an error message

        :param error: the error that was raised
        :param bool bundle_errors: do not abort when first error occurs, return a
            dict with the name of the argument and the error message to be
            bundled
        """
        error_str = str(error)
        error_msg = " ".join([str(self.help), error_str]) if self.help else error_str
        errors = {self.name: error_msg}

        if bundle_errors:
            return ValueError(error), errors
        abort(HTTPStatus.BAD_REQUEST, "Input payload validation failed", errors=errors)

    def parse(self, request, bundle_errors=False):
        """
        Parses argument value(s) from the request, converting according to
        the argument's type.

        :param request: The flask request object to parse arguments from
        :param bool bundle_errors: do not abort when first error occurs, return a
            dict with the name of the argument and the error message to be
            bundled
        """
        bundle_errors = current_app.config.get("BUNDLE_ERRORS", False) or bundle_errors
        source = self.source(request)

        results = []

        # Sentinels
        _not_found = False
        _found = True

        for operator in self.operators:
            name = self.name + operator.replace("=", "", 1)
            if name in source:
                # Account for MultiDict and regular dict
                if hasattr(source, "getlist"):
                    values = source.getlist(name)
                else:
                    values = [source.get(name)]

                for value in values:
                    if hasattr(value, "strip") and self.trim:
                        value = value.strip()
                    if hasattr(value, "lower") and not self.case_sensitive:
                        value = value.lower()

                        if hasattr(self.choices, "__iter__"):
                            self.choices = [choice.lower() for choice in self.choices]

                    try:
                        if self.action == "split":
                            value = [
                                self.convert(v, operator)
                                for v in value.split(SPLIT_CHAR)
                            ]
                        else:
                            value = self.convert(value, operator)
                    except Exception as error:
                        if self.ignore:
                            continue
                        return self.handle_validation_error(error, bundle_errors)

                    if self.choices and value not in self.choices:
                        msg = "The value '{0}' is not a valid choice for '{1}'.".format(
                            value, name
                        )
                        return self.handle_validation_error(msg, bundle_errors)

                    if name in request.unparsed_arguments:
                        request.unparsed_arguments.pop(name)
                    results.append(value)

        if not results and self.required:
            if isinstance(self.location, str):
                location = _friendly_location.get(self.location, self.location)
            else:
                locations = [_friendly_location.get(loc, loc) for loc in self.location]
                location = " or ".join(locations)
            error_msg = "Missing required parameter in {0}".format(location)
            return self.handle_validation_error(error_msg, bundle_errors)

        if not results:
            if callable(self.default):
                return self.default(), _not_found
            else:
                return self.default, _not_found

        if self.action == "append":
            return results, _found

        if self.action == "store" or len(results) == 1:
            return results[0], _found
        return results, _found

    @property
    def __schema__(self):
        if self.location == "cookie":
            return
        param = {"name": self.name, "in": LOCATIONS.get(self.location, "query")}
        _handle_arg_type(self, param)
        if self.required:
            param["required"] = True
        if self.help:
            param["description"] = self.help
        if self.default is not None:
            param["default"] = (
                self.default() if callable(self.default) else self.default
            )
        if self.action == "append":
            param["items"] = {"type": param["type"]}
            if "pattern" in param:
                param["items"]["pattern"] = param.pop("pattern")
            param["type"] = "array"
            param["collectionFormat"] = "multi"
        if self.action == "split":
            param["items"] = {"type": param["type"]}
            param["type"] = "array"
            param["collectionFormat"] = "csv"
        if self.choices:
            param["enum"] = self.choices
        return param


class RequestParser(object):
    """
    Enables adding and parsing of multiple arguments in the context of a single request.
    Ex::

        from flask_restx import RequestParser

        parser = RequestParser()
        parser.add_argument('foo')
        parser.add_argument('int_bar', type=int)
        args = parser.parse_args()

    :param bool trim: If enabled, trims whitespace on all arguments in this parser
    :param bool bundle_errors: If enabled, do not abort when first error occurs,
        return a dict with the name of the argument and the error message to be
        bundled and return all validation errors
    """

    def __init__(
        self,
        argument_class=Argument,
        result_class=ParseResult,
        trim=False,
        bundle_errors=False,
    ):
        self.args = []
        self.argument_class = argument_class
        self.result_class = result_class
        self.trim = trim
        self.bundle_errors = bundle_errors

    def add_argument(self, *args, **kwargs):
        """
        Adds an argument to be parsed.

        Accepts either a single instance of Argument or arguments to be passed
        into :class:`Argument`'s constructor.

        See :class:`Argument`'s constructor for documentation on the available options.
        """

        if len(args) == 1 and isinstance(args[0], self.argument_class):
            self.args.append(args[0])
        else:
            self.args.append(self.argument_class(*args, **kwargs))

        # Do not know what other argument classes are out there
        if self.trim and self.argument_class is Argument:
            # enable trim for appended element
            self.args[-1].trim = kwargs.get("trim", self.trim)

        return self

    def parse_args(self, req=None, strict=False):
        """
        Parse all arguments from the provided request and return the results as a ParseResult

        :param bool strict: if req includes args not in parser, throw 400 BadRequest exception
        :return: the parsed results as :class:`ParseResult` (or any class defined as :attr:`result_class`)
        :rtype: ParseResult
        """
        if req is None:
            req = request

        result = self.result_class()

        # A record of arguments not yet parsed; as each is found
        # among self.args, it will be popped out
        req.unparsed_arguments = (
            dict(self.argument_class("").source(req)) if strict else {}
        )
        errors = {}
        for arg in self.args:
            value, found = arg.parse(req, self.bundle_errors)
            if isinstance(value, ValueError):
                errors.update(found)
                found = None
            if found or arg.store_missing:
                result[arg.dest or arg.name] = value
        if errors:
            abort(
                HTTPStatus.BAD_REQUEST, "Input payload validation failed", errors=errors
            )

        if strict and req.unparsed_arguments:
            arguments = ", ".join(req.unparsed_arguments.keys())
            msg = "Unknown arguments: {0}".format(arguments)
            raise exceptions.BadRequest(msg)

        return result

    def copy(self):
        """Creates a copy of this RequestParser with the same set of arguments"""
        parser_copy = self.__class__(self.argument_class, self.result_class)
        parser_copy.args = deepcopy(self.args)
        parser_copy.trim = self.trim
        parser_copy.bundle_errors = self.bundle_errors
        return parser_copy

    def replace_argument(self, name, *args, **kwargs):
        """Replace the argument matching the given name with a new version."""
        new_arg = self.argument_class(name, *args, **kwargs)
        for index, arg in enumerate(self.args[:]):
            if new_arg.name == arg.name:
                del self.args[index]
                self.args.append(new_arg)
                break
        return self

    def remove_argument(self, name):
        """Remove the argument matching the given name."""
        for index, arg in enumerate(self.args[:]):
            if name == arg.name:
                del self.args[index]
                break
        return self

    @property
    def __schema__(self):
        params = []
        locations = set()
        for arg in self.args:
            param = arg.__schema__
            if param:
                params.append(param)
                locations.add(param["in"])
        if "body" in locations and "formData" in locations:
            raise SpecsError("Can't use formData and body at the same time")
        return params


def _handle_arg_type(arg, param):
    if isinstance(arg.type, Hashable) and arg.type in PY_TYPES:
        param["type"] = PY_TYPES[arg.type]
    elif hasattr(arg.type, "__apidoc__"):
        param["type"] = arg.type.__apidoc__["name"]
        param["in"] = "body"
    elif hasattr(arg.type, "__schema__"):
        param.update(arg.type.__schema__)
    elif arg.location == "files":
        param["type"] = "file"
    else:
        param["type"] = "string"
