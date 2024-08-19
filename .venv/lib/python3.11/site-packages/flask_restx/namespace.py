import inspect
import warnings
import logging
from collections import namedtuple, OrderedDict

from flask import request
from flask.views import http_method_funcs

from ._http import HTTPStatus
from .errors import abort
from .marshalling import marshal, marshal_with
from .model import Model, OrderedModel, SchemaModel
from .reqparse import RequestParser
from .utils import merge

# Container for each route applied to a Resource using @ns.route decorator
ResourceRoute = namedtuple("ResourceRoute", "resource urls route_doc kwargs")


class Namespace(object):
    """
    Group resources together.

    Namespace is to API what :class:`flask:flask.Blueprint` is for :class:`flask:flask.Flask`.

    :param str name: The namespace name
    :param str description: An optional short description
    :param str path: An optional prefix path. If not provided, prefix is ``/+name``
    :param list decorators: A list of decorators to apply to each resources
    :param bool validate: Whether or not to perform validation on this namespace
    :param bool ordered: Whether or not to preserve order on models and marshalling
    :param Api api: an optional API to attache to the namespace
    """

    def __init__(
        self,
        name,
        description=None,
        path=None,
        decorators=None,
        validate=None,
        authorizations=None,
        ordered=False,
        **kwargs
    ):
        self.name = name
        self.description = description
        self._path = path

        self._schema = None
        self._validate = validate
        self.models = {}
        self.urls = {}
        self.decorators = decorators if decorators else []
        self.resources = []  # List[ResourceRoute]
        self.error_handlers = OrderedDict()
        self.default_error_handler = None
        self.authorizations = authorizations
        self.ordered = ordered
        self.apis = []
        if "api" in kwargs:
            self.apis.append(kwargs["api"])
        self.logger = logging.getLogger(__name__ + "." + self.name)

    @property
    def path(self):
        return (self._path or ("/" + self.name)).rstrip("/")

    def add_resource(self, resource, *urls, **kwargs):
        """
        Register a Resource for a given API Namespace

        :param Resource resource: the resource ro register
        :param str urls: one or more url routes to match for the resource,
                         standard flask routing rules apply.
                         Any url variables will be passed to the resource method as args.
        :param str endpoint: endpoint name (defaults to :meth:`Resource.__name__.lower`
            Can be used to reference this route in :class:`fields.Url` fields
        :param list|tuple resource_class_args: args to be forwarded to the constructor of the resource.
        :param dict resource_class_kwargs: kwargs to be forwarded to the constructor of the resource.

        Additional keyword arguments not specified above will be passed as-is
        to :meth:`flask.Flask.add_url_rule`.

        Examples::

            namespace.add_resource(HelloWorld, '/', '/hello')
            namespace.add_resource(Foo, '/foo', endpoint="foo")
            namespace.add_resource(FooSpecial, '/special/foo', endpoint="foo")
        """
        route_doc = kwargs.pop("route_doc", {})
        self.resources.append(ResourceRoute(resource, urls, route_doc, kwargs))
        for api in self.apis:
            ns_urls = api.ns_urls(self, urls)
            api.register_resource(self, resource, *ns_urls, **kwargs)

    def route(self, *urls, **kwargs):
        """
        A decorator to route resources.
        """

        def wrapper(cls):
            doc = kwargs.pop("doc", None)
            if doc is not None:
                # build api doc intended only for this route
                kwargs["route_doc"] = self._build_doc(cls, doc)
            self.add_resource(cls, *urls, **kwargs)
            return cls

        return wrapper

    def _build_doc(self, cls, doc):
        if doc is False:
            return False
        unshortcut_params_description(doc)
        handle_deprecations(doc)
        for http_method in http_method_funcs:
            if http_method in doc:
                if doc[http_method] is False:
                    continue
                unshortcut_params_description(doc[http_method])
                handle_deprecations(doc[http_method])
                if "expect" in doc[http_method] and not isinstance(
                    doc[http_method]["expect"], (list, tuple)
                ):
                    doc[http_method]["expect"] = [doc[http_method]["expect"]]
        return merge(getattr(cls, "__apidoc__", {}), doc)

    def doc(self, shortcut=None, **kwargs):
        """A decorator to add some api documentation to the decorated object"""
        if isinstance(shortcut, str):
            kwargs["id"] = shortcut
        show = shortcut if isinstance(shortcut, bool) else True

        def wrapper(documented):
            documented.__apidoc__ = self._build_doc(
                documented, kwargs if show else False
            )
            return documented

        return wrapper

    def hide(self, func):
        """A decorator to hide a resource or a method from specifications"""
        return self.doc(False)(func)

    def abort(self, *args, **kwargs):
        """
        Properly abort the current request

        See: :func:`~flask_restx.errors.abort`
        """
        abort(*args, **kwargs)

    def add_model(self, name, definition):
        self.models[name] = definition
        for api in self.apis:
            api.models[name] = definition
        return definition

    def model(self, name=None, model=None, mask=None, strict=False, **kwargs):
        """
        Register a model

        :param bool strict - should model validation raise error when non-specified param
                             is provided?

        .. seealso:: :class:`Model`
        """
        cls = OrderedModel if self.ordered else Model
        model = cls(name, model, mask=mask, strict=strict)
        model.__apidoc__.update(kwargs)
        return self.add_model(name, model)

    def schema_model(self, name=None, schema=None):
        """
        Register a model

        .. seealso:: :class:`Model`
        """
        model = SchemaModel(name, schema)
        return self.add_model(name, model)

    def extend(self, name, parent, fields):
        """
        Extend a model (Duplicate all fields)

        :deprecated: since 0.9. Use :meth:`clone` instead
        """
        if isinstance(parent, list):
            parents = parent + [fields]
            model = Model.extend(name, *parents)
        else:
            model = Model.extend(name, parent, fields)
        return self.add_model(name, model)

    def clone(self, name, *specs):
        """
        Clone a model (Duplicate all fields)

        :param str name: the resulting model name
        :param specs: a list of models from which to clone the fields

        .. seealso:: :meth:`Model.clone`

        """
        model = Model.clone(name, *specs)
        return self.add_model(name, model)

    def inherit(self, name, *specs):
        """
        Inherit a model (use the Swagger composition pattern aka. allOf)

        .. seealso:: :meth:`Model.inherit`
        """
        model = Model.inherit(name, *specs)
        return self.add_model(name, model)

    def expect(self, *inputs, **kwargs):
        """
        A decorator to Specify the expected input model

        :param ModelBase|Parse inputs: An expect model or request parser
        :param bool validate: whether to perform validation or not

        """
        expect = []
        params = {"validate": kwargs.get("validate", self._validate), "expect": expect}
        for param in inputs:
            expect.append(param)
        return self.doc(**params)

    def parser(self):
        """Instanciate a :class:`~RequestParser`"""
        return RequestParser()

    def as_list(self, field):
        """Allow to specify nested lists for documentation"""
        field.__apidoc__ = merge(getattr(field, "__apidoc__", {}), {"as_list": True})
        return field

    def marshal_with(
        self, fields, as_list=False, code=HTTPStatus.OK, description=None, **kwargs
    ):
        """
        A decorator specifying the fields to use for serialization.

        :param bool as_list: Indicate that the return type is a list (for the documentation)
        :param int code: Optionally give the expected HTTP response code if its different from 200

        """

        def wrapper(func):
            doc = {
                "responses": {
                    str(code): (description, [fields], kwargs)
                    if as_list
                    else (description, fields, kwargs)
                },
                "__mask__": kwargs.get(
                    "mask", True
                ),  # Mask values can't be determined outside app context
            }
            func.__apidoc__ = merge(getattr(func, "__apidoc__", {}), doc)
            return marshal_with(fields, ordered=self.ordered, **kwargs)(func)

        return wrapper

    def marshal_list_with(self, fields, **kwargs):
        """A shortcut decorator for :meth:`~Api.marshal_with` with ``as_list=True``"""
        return self.marshal_with(fields, True, **kwargs)

    def marshal(self, *args, **kwargs):
        """A shortcut to the :func:`marshal` helper"""
        return marshal(*args, **kwargs)

    def errorhandler(self, exception):
        """A decorator to register an error handler for a given exception"""
        if inspect.isclass(exception) and issubclass(exception, Exception):
            # Register an error handler for a given exception
            def wrapper(func):
                self.error_handlers[exception] = func
                return func

            return wrapper
        else:
            # Register the default error handler
            self.default_error_handler = exception
            return exception

    def param(self, name, description=None, _in="query", **kwargs):
        """
        A decorator to specify one of the expected parameters

        :param str name: the parameter name
        :param str description: a small description
        :param str _in: the parameter location `(query|header|formData|body|cookie)`
        """
        param = kwargs
        param["in"] = _in
        param["description"] = description
        return self.doc(params={name: param})

    def response(self, code, description, model=None, **kwargs):
        """
        A decorator to specify one of the expected responses

        :param int code: the HTTP status code
        :param str description: a small description about the response
        :param ModelBase model: an optional response model

        """
        return self.doc(responses={str(code): (description, model, kwargs)})

    def header(self, name, description=None, **kwargs):
        """
        A decorator to specify one of the expected headers

        :param str name: the HTTP header name
        :param str description: a description about the header

        """
        header = {"description": description}
        header.update(kwargs)
        return self.doc(headers={name: header})

    def produces(self, mimetypes):
        """A decorator to specify the MIME types the API can produce"""
        return self.doc(produces=mimetypes)

    def deprecated(self, func):
        """A decorator to mark a resource or a method as deprecated"""
        return self.doc(deprecated=True)(func)

    def vendor(self, *args, **kwargs):
        """
        A decorator to expose vendor extensions.

        Extensions can be submitted as dict or kwargs.
        The ``x-`` prefix is optionnal and will be added if missing.

        See: http://swagger.io/specification/#specification-extensions-128
        """
        for arg in args:
            kwargs.update(arg)
        return self.doc(vendor=kwargs)

    @property
    def payload(self):
        """Store the input payload in the current request context"""
        return request.get_json()


def unshortcut_params_description(data):
    if "params" in data:
        for name, description in data["params"].items():
            if isinstance(description, str):
                data["params"][name] = {"description": description}


def handle_deprecations(doc):
    if "parser" in doc:
        warnings.warn(
            "The parser attribute is deprecated, use expect instead",
            DeprecationWarning,
            stacklevel=2,
        )
        doc["expect"] = doc.get("expect", []) + [doc.pop("parser")]
    if "body" in doc:
        warnings.warn(
            "The body attribute is deprecated, use expect instead",
            DeprecationWarning,
            stacklevel=2,
        )
        doc["expect"] = doc.get("expect", []) + [doc.pop("body")]
