import difflib
import inspect
from itertools import chain
import logging
import operator
import re
import sys
import warnings

from collections import OrderedDict
from functools import wraps, partial
from types import MethodType

from flask import url_for, request, current_app
from flask import make_response as original_flask_make_response

from flask.signals import got_request_exception

from jsonschema import RefResolver

from werkzeug.utils import cached_property
from werkzeug.datastructures import Headers
from werkzeug.exceptions import (
    HTTPException,
    MethodNotAllowed,
    NotFound,
    NotAcceptable,
    InternalServerError,
)

from . import apidoc
from .mask import ParseError, MaskError
from .namespace import Namespace
from .postman import PostmanCollectionV1
from .resource import Resource
from .swagger import Swagger
from .utils import (
    default_id,
    camel_to_dash,
    unpack,
    import_check_view_func,
    BaseResponse,
)
from .representations import output_json
from ._http import HTTPStatus

endpoint_from_view_func = import_check_view_func()


RE_RULES = re.compile("(<.*>)")

# List headers that should never be handled by Flask-RESTX
HEADERS_BLACKLIST = ("Content-Length",)

DEFAULT_REPRESENTATIONS = [("application/json", output_json)]

log = logging.getLogger(__name__)


class Api(object):
    """
    The main entry point for the application.
    You need to initialize it with a Flask Application: ::

    >>> app = Flask(__name__)
    >>> api = Api(app)

    Alternatively, you can use :meth:`init_app` to set the Flask application
    after it has been constructed.

    The endpoint parameter prefix all views and resources:

        - The API root/documentation will be ``{endpoint}.root``
        - A resource registered as 'resource' will be available as ``{endpoint}.resource``

    :param flask.Flask|flask.Blueprint app: the Flask application object or a Blueprint
    :param str version: The API version (used in Swagger documentation)
    :param str title: The API title (used in Swagger documentation)
    :param str description: The API description (used in Swagger documentation)
    :param str terms_url: The API terms page URL (used in Swagger documentation)
    :param str contact: A contact email for the API (used in Swagger documentation)
    :param str license: The license associated to the API (used in Swagger documentation)
    :param str license_url: The license page URL (used in Swagger documentation)
    :param str endpoint: The API base endpoint (default to 'api).
    :param str default: The default namespace base name (default to 'default')
    :param str default_label: The default namespace label (used in Swagger documentation)
    :param str default_mediatype: The default media type to return
    :param bool validate: Whether or not the API should perform input payload validation.
    :param bool ordered: Whether or not preserve order models and marshalling.
    :param str doc: The documentation path. If set to a false value, documentation is disabled.
                (Default to '/')
    :param list decorators: Decorators to attach to every resource
    :param bool catch_all_404s: Use :meth:`handle_error`
        to handle 404 errors throughout your app
    :param dict authorizations: A Swagger Authorizations declaration as dictionary
    :param bool serve_challenge_on_401: Serve basic authentication challenge with 401
        responses (default 'False')
    :param FormatChecker format_checker: A jsonschema.FormatChecker object that is hooked into
        the Model validator. A default or a custom FormatChecker can be provided (e.g., with custom
        checkers), otherwise the default action is to not enforce any format validation.
    :param url_scheme: If set to a string (e.g. http, https), then the specs_url and base_url will explicitly use this
        scheme regardless of how the application is deployed. This is necessary for some deployments behind a reverse
        proxy.
    :param str default_swagger_filename: The default swagger filename.
    """

    def __init__(
        self,
        app=None,
        version="1.0",
        title=None,
        description=None,
        terms_url=None,
        license=None,
        license_url=None,
        contact=None,
        contact_url=None,
        contact_email=None,
        authorizations=None,
        security=None,
        doc="/",
        default_id=default_id,
        default="default",
        default_label="Default namespace",
        validate=None,
        tags=None,
        prefix="",
        ordered=False,
        default_mediatype="application/json",
        decorators=None,
        catch_all_404s=False,
        serve_challenge_on_401=False,
        format_checker=None,
        url_scheme=None,
        default_swagger_filename="swagger.json",
        **kwargs
    ):
        self.version = version
        self.title = title or "API"
        self.description = description
        self.terms_url = terms_url
        self.contact = contact
        self.contact_email = contact_email
        self.contact_url = contact_url
        self.license = license
        self.license_url = license_url
        self.authorizations = authorizations
        self.security = security
        self.default_id = default_id
        self.ordered = ordered
        self._validate = validate
        self._doc = doc
        self._doc_view = None
        self._default_error_handler = None
        self.tags = tags or []

        self.error_handlers = OrderedDict(
            {
                ParseError: mask_parse_error_handler,
                MaskError: mask_error_handler,
            }
        )
        self._schema = None
        self.models = {}
        self._refresolver = None
        self.format_checker = format_checker
        self.namespaces = []
        self.default_swagger_filename = default_swagger_filename

        self.ns_paths = dict()

        self.representations = OrderedDict(DEFAULT_REPRESENTATIONS)
        self.urls = {}
        self.prefix = prefix
        self.default_mediatype = default_mediatype
        self.decorators = decorators if decorators else []
        self.catch_all_404s = catch_all_404s
        self.serve_challenge_on_401 = serve_challenge_on_401
        self.blueprint_setup = None
        self.endpoints = set()
        self.resources = []
        self.app = None
        self.blueprint = None
        # must come after self.app initialisation to prevent __getattr__ recursion
        # in self._configure_namespace_logger
        self.default_namespace = self.namespace(
            default,
            default_label,
            endpoint="{0}-declaration".format(default),
            validate=validate,
            api=self,
            path="/",
        )
        self.url_scheme = url_scheme
        if app is not None:
            self.app = app
            self.init_app(app)
        # super(Api, self).__init__(app, **kwargs)

    def init_app(self, app, **kwargs):
        """
        Allow to lazy register the API on a Flask application::

        >>> app = Flask(__name__)
        >>> api = Api()
        >>> api.init_app(app)

        :param flask.Flask app: the Flask application object
        :param str title: The API title (used in Swagger documentation)
        :param str description: The API description (used in Swagger documentation)
        :param str terms_url: The API terms page URL (used in Swagger documentation)
        :param str contact: A contact email for the API (used in Swagger documentation)
        :param str license: The license associated to the API (used in Swagger documentation)
        :param str license_url: The license page URL (used in Swagger documentation)
        :param url_scheme: If set to a string (e.g. http, https), then the specs_url and base_url will explicitly use
            this scheme regardless of how the application is deployed. This is necessary for some deployments behind a
            reverse proxy.
        """
        self.app = app
        self.title = kwargs.get("title", self.title)
        self.description = kwargs.get("description", self.description)
        self.terms_url = kwargs.get("terms_url", self.terms_url)
        self.contact = kwargs.get("contact", self.contact)
        self.contact_url = kwargs.get("contact_url", self.contact_url)
        self.contact_email = kwargs.get("contact_email", self.contact_email)
        self.license = kwargs.get("license", self.license)
        self.license_url = kwargs.get("license_url", self.license_url)
        self.url_scheme = kwargs.get("url_scheme", self.url_scheme)
        self._add_specs = kwargs.get("add_specs", True)
        self._register_specs(app)
        self._register_doc(app)

        # If app is a blueprint, defer the initialization
        try:
            app.record(self._deferred_blueprint_init)
        # Flask.Blueprint has a 'record' attribute, Flask.Api does not
        except AttributeError:
            self._init_app(app)
        else:
            self.blueprint = app

    def _init_app(self, app):
        """
        Perform initialization actions with the given :class:`flask.Flask` object.

        :param flask.Flask app: The flask application object
        """
        app.handle_exception = partial(self.error_router, app.handle_exception)
        app.handle_user_exception = partial(
            self.error_router, app.handle_user_exception
        )

        if len(self.resources) > 0:
            for resource, namespace, urls, kwargs in self.resources:
                self._register_view(app, resource, namespace, *urls, **kwargs)

        for ns in self.namespaces:
            self._configure_namespace_logger(app, ns)

        self._register_apidoc(app)
        self._validate = (
            self._validate
            if self._validate is not None
            else app.config.get("RESTX_VALIDATE", False)
        )
        app.config.setdefault("RESTX_MASK_HEADER", "X-Fields")
        app.config.setdefault("RESTX_MASK_SWAGGER", True)
        app.config.setdefault("RESTX_INCLUDE_ALL_MODELS", False)

        # check for deprecated config variable names
        if "ERROR_404_HELP" in app.config:
            app.config["RESTX_ERROR_404_HELP"] = app.config["ERROR_404_HELP"]
            warnings.warn(
                "'ERROR_404_HELP' config setting is deprecated and will be "
                "removed in the future. Use 'RESTX_ERROR_404_HELP' instead.",
                DeprecationWarning,
            )

    def __getattr__(self, name):
        try:
            return getattr(self.default_namespace, name)
        except AttributeError:
            raise AttributeError("Api does not have {0} attribute".format(name))

    def _complete_url(self, url_part, registration_prefix):
        """
        This method is used to defer the construction of the final url in
        the case that the Api is created with a Blueprint.

        :param url_part: The part of the url the endpoint is registered with
        :param registration_prefix: The part of the url contributed by the
            blueprint.  Generally speaking, BlueprintSetupState.url_prefix
        """
        parts = (registration_prefix, self.prefix, url_part)
        return "".join(part for part in parts if part)

    def _register_apidoc(self, app):
        conf = app.extensions.setdefault("restx", {})
        if not conf.get("apidoc_registered", False):
            app.register_blueprint(apidoc.apidoc)
        conf["apidoc_registered"] = True

    def _register_specs(self, app_or_blueprint):
        if self._add_specs:
            endpoint = str("specs")
            self._register_view(
                app_or_blueprint,
                SwaggerView,
                self.default_namespace,
                "/" + self.default_swagger_filename,
                endpoint=endpoint,
                resource_class_args=(self,),
            )
            self.endpoints.add(endpoint)

    def _register_doc(self, app_or_blueprint):
        if self._add_specs and self._doc:
            # Register documentation before root if enabled
            app_or_blueprint.add_url_rule(self._doc, "doc", self.render_doc)
        app_or_blueprint.add_url_rule(self.prefix or "/", "root", self.render_root)

    def register_resource(self, namespace, resource, *urls, **kwargs):
        endpoint = kwargs.pop("endpoint", None)
        endpoint = str(endpoint or self.default_endpoint(resource, namespace))

        kwargs["endpoint"] = endpoint
        self.endpoints.add(endpoint)

        if self.app is not None:
            self._register_view(self.app, resource, namespace, *urls, **kwargs)
        else:
            self.resources.append((resource, namespace, urls, kwargs))
        return endpoint

    def _configure_namespace_logger(self, app, namespace):
        for handler in app.logger.handlers:
            namespace.logger.addHandler(handler)
        namespace.logger.setLevel(app.logger.level)

    def _register_view(self, app, resource, namespace, *urls, **kwargs):
        endpoint = kwargs.pop("endpoint", None) or camel_to_dash(resource.__name__)
        resource_class_args = kwargs.pop("resource_class_args", ())
        resource_class_kwargs = kwargs.pop("resource_class_kwargs", {})

        # NOTE: 'view_functions' is cleaned up from Blueprint class in Flask 1.0
        if endpoint in getattr(app, "view_functions", {}):
            previous_view_class = app.view_functions[endpoint].__dict__["view_class"]

            # if you override the endpoint with a different class, avoid the
            # collision by raising an exception
            if previous_view_class != resource:
                msg = "This endpoint (%s) is already set to the class %s."
                raise ValueError(msg % (endpoint, previous_view_class.__name__))

        resource.mediatypes = self.mediatypes_method()  # Hacky
        resource.endpoint = endpoint

        resource_func = self.output(
            resource.as_view(
                endpoint, self, *resource_class_args, **resource_class_kwargs
            )
        )

        # Apply Namespace and Api decorators to a resource
        for decorator in chain(namespace.decorators, self.decorators):
            resource_func = decorator(resource_func)

        for url in urls:
            # If this Api has a blueprint
            if self.blueprint:
                # And this Api has been setup
                if self.blueprint_setup:
                    # Set the rule to a string directly, as the blueprint is already
                    # set up.
                    self.blueprint_setup.add_url_rule(
                        url, view_func=resource_func, **kwargs
                    )
                    continue
                else:
                    # Set the rule to a function that expects the blueprint prefix
                    # to construct the final url.  Allows deferment of url finalization
                    # in the case that the associated Blueprint has not yet been
                    # registered to an application, so we can wait for the registration
                    # prefix
                    rule = partial(self._complete_url, url)
            else:
                # If we've got no Blueprint, just build a url with no prefix
                rule = self._complete_url(url, "")
            # Add the url to the application or blueprint
            app.add_url_rule(rule, view_func=resource_func, **kwargs)

    def output(self, resource):
        """
        Wraps a resource (as a flask view function),
        for cases where the resource does not directly return a response object

        :param resource: The resource as a flask view function
        """

        @wraps(resource)
        def wrapper(*args, **kwargs):
            resp = resource(*args, **kwargs)
            if isinstance(resp, BaseResponse):
                return resp
            data, code, headers = unpack(resp)
            return self.make_response(data, code, headers=headers)

        return wrapper

    def make_response(self, data, *args, **kwargs):
        """
        Looks up the representation transformer for the requested media
        type, invoking the transformer to create a response object. This
        defaults to default_mediatype if no transformer is found for the
        requested mediatype. If default_mediatype is None, a 406 Not
        Acceptable response will be sent as per RFC 2616 section 14.1

        :param data: Python object containing response data to be transformed
        """
        default_mediatype = (
            kwargs.pop("fallback_mediatype", None) or self.default_mediatype
        )
        mediatype = request.accept_mimetypes.best_match(
            self.representations,
            default=default_mediatype,
        )
        if mediatype is None:
            raise NotAcceptable()
        if mediatype in self.representations:
            resp = self.representations[mediatype](data, *args, **kwargs)
            resp.headers["Content-Type"] = mediatype
            return resp
        elif mediatype == "text/plain":
            resp = original_flask_make_response(str(data), *args, **kwargs)
            resp.headers["Content-Type"] = "text/plain"
            return resp
        else:
            raise InternalServerError()

    def documentation(self, func):
        """A decorator to specify a view function for the documentation"""
        self._doc_view = func
        return func

    def render_root(self):
        self.abort(HTTPStatus.NOT_FOUND)

    def render_doc(self):
        """Override this method to customize the documentation page"""
        if self._doc_view:
            return self._doc_view()
        elif not self._doc:
            self.abort(HTTPStatus.NOT_FOUND)
        return apidoc.ui_for(self)

    def default_endpoint(self, resource, namespace):
        """
        Provide a default endpoint for a resource on a given namespace.

        Endpoints are ensured not to collide.

        Override this method specify a custom algorithm for default endpoint.

        :param Resource resource: the resource for which we want an endpoint
        :param Namespace namespace: the namespace holding the resource
        :returns str: An endpoint name
        """
        endpoint = camel_to_dash(resource.__name__)
        if namespace is not self.default_namespace:
            endpoint = "{ns.name}_{endpoint}".format(ns=namespace, endpoint=endpoint)
        if endpoint in self.endpoints:
            suffix = 2
            while True:
                new_endpoint = "{base}_{suffix}".format(base=endpoint, suffix=suffix)
                if new_endpoint not in self.endpoints:
                    endpoint = new_endpoint
                    break
                suffix += 1
        return endpoint

    def get_ns_path(self, ns):
        return self.ns_paths.get(ns)

    def ns_urls(self, ns, urls):
        path = self.get_ns_path(ns) or ns.path
        return [path + url for url in urls]

    def add_namespace(self, ns, path=None):
        """
        This method registers resources from namespace for current instance of api.
        You can use argument path for definition custom prefix url for namespace.

        :param Namespace ns: the namespace
        :param path: registration prefix of namespace
        """
        if ns not in self.namespaces:
            self.namespaces.append(ns)
            if self not in ns.apis:
                ns.apis.append(self)
            # Associate ns with prefix-path
            if path is not None:
                self.ns_paths[ns] = path
        # Register resources
        for r in ns.resources:
            urls = self.ns_urls(ns, r.urls)
            self.register_resource(ns, r.resource, *urls, **r.kwargs)
        # Register models
        for name, definition in ns.models.items():
            self.models[name] = definition
        if not self.blueprint and self.app is not None:
            self._configure_namespace_logger(self.app, ns)

    def namespace(self, *args, **kwargs):
        """
        A namespace factory.

        :returns Namespace: a new namespace instance
        """
        kwargs["ordered"] = kwargs.get("ordered", self.ordered)
        ns = Namespace(*args, **kwargs)
        self.add_namespace(ns)
        return ns

    def endpoint(self, name):
        if self.blueprint:
            return "{0}.{1}".format(self.blueprint.name, name)
        else:
            return name

    @property
    def specs_url(self):
        """
        The Swagger specifications relative url (ie. `swagger.json`). If
        the spec_url_scheme attribute is set, then the full url is provided instead
        (e.g. http://localhost/swaggger.json).

        :rtype: str
        """
        external = None if self.url_scheme is None else True
        return url_for(
            self.endpoint("specs"), _scheme=self.url_scheme, _external=external
        )

    @property
    def base_url(self):
        """
        The API base absolute url

        :rtype: str
        """
        return url_for(self.endpoint("root"), _scheme=self.url_scheme, _external=True)

    @property
    def base_path(self):
        """
        The API path

        :rtype: str
        """
        return url_for(self.endpoint("root"), _external=False)

    @cached_property
    def __schema__(self):
        """
        The Swagger specifications/schema for this API

        :returns dict: the schema as a serializable dict
        """
        if not self._schema:
            try:
                self._schema = Swagger(self).as_dict()
            except Exception:
                # Log the source exception for debugging purpose
                # and return an error message
                msg = "Unable to render schema"
                log.exception(msg)  # This will provide a full traceback
                return {"error": msg}
        return self._schema

    @property
    def _own_and_child_error_handlers(self):
        rv = OrderedDict()
        rv.update(self.error_handlers)
        for ns in self.namespaces:
            for exception, handler in ns.error_handlers.items():
                rv[exception] = handler
        return rv

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
            self._default_error_handler = exception
            return exception

    def owns_endpoint(self, endpoint):
        """
        Tests if an endpoint name (not path) belongs to this Api.
        Takes into account the Blueprint name part of the endpoint name.

        :param str endpoint: The name of the endpoint being checked
        :return: bool
        """

        if self.blueprint:
            if endpoint.startswith(self.blueprint.name):
                endpoint = endpoint.split(self.blueprint.name + ".", 1)[-1]
            else:
                return False
        return endpoint in self.endpoints

    def _should_use_fr_error_handler(self):
        """
        Determine if error should be handled with FR or default Flask

        The goal is to return Flask error handlers for non-FR-related routes,
        and FR errors (with the correct media type) for FR endpoints. This
        method currently handles 404 and 405 errors.

        :return: bool
        """
        adapter = current_app.create_url_adapter(request)

        try:
            adapter.match()
        except MethodNotAllowed as e:
            # Check if the other HTTP methods at this url would hit the Api
            valid_route_method = e.valid_methods[0]
            rule, _ = adapter.match(method=valid_route_method, return_rule=True)
            return self.owns_endpoint(rule.endpoint)
        except NotFound:
            return self.catch_all_404s
        except Exception:
            # Werkzeug throws other kinds of exceptions, such as Redirect
            pass

    def _has_fr_route(self):
        """Encapsulating the rules for whether the request was to a Flask endpoint"""
        # 404's, 405's, which might not have a url_rule
        if self._should_use_fr_error_handler():
            return True
        # for all other errors, just check if FR dispatched the route
        if not request.url_rule:
            return False
        return self.owns_endpoint(request.url_rule.endpoint)

    def error_router(self, original_handler, e):
        """
        This function decides whether the error occurred in a flask-restx
        endpoint or not. If it happened in a flask-restx endpoint, our
        handler will be dispatched. If it happened in an unrelated view, the
        app's original error handler will be dispatched.
        In the event that the error occurred in a flask-restx endpoint but
        the local handler can't resolve the situation, the router will fall
        back onto the original_handler as last resort.

        :param function original_handler: the original Flask error handler for the app
        :param Exception e: the exception raised while handling the request
        """
        if self._has_fr_route():
            try:
                return self.handle_error(e)
            except Exception as f:
                return original_handler(f)
        return original_handler(e)

    def _propagate_exceptions(self):
        """
        Returns the value of the ``PROPAGATE_EXCEPTIONS`` configuration
        value in case it's set, otherwise return true if app.debug or
        app.testing is set. This method was deprecated in Flask 2.3 but
        we still need it for our error handlers.
        """
        rv = current_app.config.get("PROPAGATE_EXCEPTIONS")
        if rv is not None:
            return rv
        return current_app.testing or current_app.debug

    def handle_error(self, e):
        """
        Error handler for the API transforms a raised exception into a Flask response,
        with the appropriate HTTP status code and body.

        :param Exception e: the raised Exception object

        """
        # When propagate_exceptions is set, do not return the exception to the
        # client if a handler is configured for the exception.
        if (
            not isinstance(e, HTTPException)
            and self._propagate_exceptions()
            and not isinstance(e, tuple(self._own_and_child_error_handlers.keys()))
        ):
            exc_type, exc_value, tb = sys.exc_info()
            if exc_value is e:
                raise
            else:
                raise e

        include_message_in_response = current_app.config.get(
            "ERROR_INCLUDE_MESSAGE", True
        )
        default_data = {}

        headers = Headers()

        for typecheck, handler in self._own_and_child_error_handlers.items():
            if isinstance(e, typecheck):
                result = handler(e)
                default_data, code, headers = unpack(
                    result, HTTPStatus.INTERNAL_SERVER_ERROR
                )
                break
        else:
            # Flask docs say: "This signal is not sent for HTTPException or other exceptions that have error handlers
            # registered, unless the exception was raised from an error handler."
            got_request_exception.send(current_app._get_current_object(), exception=e)

            if isinstance(e, HTTPException):
                code = None
                if e.code is not None:
                    code = HTTPStatus(e.code)
                elif e.response is not None:
                    code = HTTPStatus(e.response.status_code)
                if include_message_in_response:
                    default_data = {"message": e.description or code.phrase}
                headers = e.get_response().headers
            elif self._default_error_handler:
                result = self._default_error_handler(e)
                default_data, code, headers = unpack(
                    result, HTTPStatus.INTERNAL_SERVER_ERROR
                )
            else:
                code = HTTPStatus.INTERNAL_SERVER_ERROR
                if include_message_in_response:
                    default_data = {
                        "message": code.phrase,
                    }

        if include_message_in_response:
            default_data["message"] = default_data.get("message", str(e))

        data = getattr(e, "data", default_data)
        fallback_mediatype = None

        if code >= HTTPStatus.INTERNAL_SERVER_ERROR:
            exc_info = sys.exc_info()
            if exc_info[1] is None:
                exc_info = None
            current_app.log_exception(exc_info)

        elif (
            code == HTTPStatus.NOT_FOUND
            and current_app.config.get("RESTX_ERROR_404_HELP", True)
            and include_message_in_response
        ):
            data["message"] = self._help_on_404(data.get("message", None))

        elif code == HTTPStatus.NOT_ACCEPTABLE and self.default_mediatype is None:
            # if we are handling NotAcceptable (406), make sure that
            # make_response uses a representation we support as the
            # default mediatype (so that make_response doesn't throw
            # another NotAcceptable error).
            supported_mediatypes = list(self.representations.keys())
            fallback_mediatype = (
                supported_mediatypes[0] if supported_mediatypes else "text/plain"
            )

        # Remove blacklisted headers
        for header in HEADERS_BLACKLIST:
            headers.pop(header, None)

        resp = self.make_response(
            data, code, headers, fallback_mediatype=fallback_mediatype
        )

        if code == HTTPStatus.UNAUTHORIZED:
            resp = self.unauthorized(resp)
        return resp

    def _help_on_404(self, message=None):
        rules = dict(
            [
                (RE_RULES.sub("", rule.rule), rule.rule)
                for rule in current_app.url_map.iter_rules()
            ]
        )
        close_matches = difflib.get_close_matches(request.path, rules.keys())
        if close_matches:
            # If we already have a message, add punctuation and continue it.
            message = "".join(
                (
                    (message.rstrip(".") + ". ") if message else "",
                    "You have requested this URI [",
                    request.path,
                    "] but did you mean ",
                    " or ".join((rules[match] for match in close_matches)),
                    " ?",
                )
            )
        return message

    def as_postman(self, urlvars=False, swagger=False):
        """
        Serialize the API as Postman collection (v1)

        :param bool urlvars: whether to include or not placeholders for query strings
        :param bool swagger: whether to include or not the swagger.json specifications

        """
        return PostmanCollectionV1(self, swagger=swagger).as_dict(urlvars=urlvars)

    @property
    def payload(self):
        """Store the input payload in the current request context"""
        return request.get_json()

    @property
    def refresolver(self):
        if not self._refresolver:
            self._refresolver = RefResolver.from_schema(self.__schema__)
        return self._refresolver

    @staticmethod
    def _blueprint_setup_add_url_rule_patch(
        blueprint_setup, rule, endpoint=None, view_func=None, **options
    ):
        """
        Method used to patch BlueprintSetupState.add_url_rule for setup
        state instance corresponding to this Api instance.  Exists primarily
        to enable _complete_url's function.

        :param blueprint_setup: The BlueprintSetupState instance (self)
        :param rule: A string or callable that takes a string and returns a
            string(_complete_url) that is the url rule for the endpoint
            being registered
        :param endpoint: See BlueprintSetupState.add_url_rule
        :param view_func: See BlueprintSetupState.add_url_rule
        :param **options: See BlueprintSetupState.add_url_rule
        """

        if callable(rule):
            rule = rule(blueprint_setup.url_prefix)
        elif blueprint_setup.url_prefix:
            rule = blueprint_setup.url_prefix + rule
        options.setdefault("subdomain", blueprint_setup.subdomain)
        if endpoint is None:
            endpoint = endpoint_from_view_func(view_func)
        defaults = blueprint_setup.url_defaults
        if "defaults" in options:
            defaults = dict(defaults, **options.pop("defaults"))
        blueprint_setup.app.add_url_rule(
            rule,
            "%s.%s" % (blueprint_setup.blueprint.name, endpoint),
            view_func,
            defaults=defaults,
            **options
        )

    def _deferred_blueprint_init(self, setup_state):
        """
        Synchronize prefix between blueprint/api and registration options, then
        perform initialization with setup_state.app :class:`flask.Flask` object.
        When a :class:`flask_restx.Api` object is initialized with a blueprint,
        this method is recorded on the blueprint to be run when the blueprint is later
        registered to a :class:`flask.Flask` object.  This method also monkeypatches
        BlueprintSetupState.add_url_rule with _blueprint_setup_add_url_rule_patch.

        :param setup_state: The setup state object passed to deferred functions
            during blueprint registration
        :type setup_state: flask.blueprints.BlueprintSetupState

        """

        self.blueprint_setup = setup_state
        if setup_state.add_url_rule.__name__ != "_blueprint_setup_add_url_rule_patch":
            setup_state._original_add_url_rule = setup_state.add_url_rule
            setup_state.add_url_rule = MethodType(
                Api._blueprint_setup_add_url_rule_patch, setup_state
            )
        if not setup_state.first_registration:
            raise ValueError("flask-restx blueprints can only be registered once.")
        self._init_app(setup_state.app)

    def mediatypes_method(self):
        """Return a method that returns a list of mediatypes"""
        return lambda resource_cls: self.mediatypes() + [self.default_mediatype]

    def mediatypes(self):
        """Returns a list of requested mediatypes sent in the Accept header"""
        return [
            h
            for h, q in sorted(
                request.accept_mimetypes, key=operator.itemgetter(1), reverse=True
            )
        ]

    def representation(self, mediatype):
        """
        Allows additional representation transformers to be declared for the
        api. Transformers are functions that must be decorated with this
        method, passing the mediatype the transformer represents. Three
        arguments are passed to the transformer:

        * The data to be represented in the response body
        * The http status code
        * A dictionary of headers

        The transformer should convert the data appropriately for the mediatype
        and return a Flask response object.

        Ex::

            @api.representation('application/xml')
            def xml(data, code, headers):
                resp = make_response(convert_data_to_xml(data), code)
                resp.headers.extend(headers)
                return resp
        """

        def wrapper(func):
            self.representations[mediatype] = func
            return func

        return wrapper

    def unauthorized(self, response):
        """Given a response, change it to ask for credentials"""

        if self.serve_challenge_on_401:
            realm = current_app.config.get("HTTP_BASIC_AUTH_REALM", "flask-restx")
            challenge = '{0} realm="{1}"'.format("Basic", realm)

            response.headers["WWW-Authenticate"] = challenge
        return response

    def url_for(self, resource, **values):
        """
        Generates a URL to the given resource.

        Works like :func:`flask.url_for`.
        """
        endpoint = resource.endpoint
        if self.blueprint:
            endpoint = "{0}.{1}".format(self.blueprint.name, endpoint)
        return url_for(endpoint, **values)


class SwaggerView(Resource):
    """Render the Swagger specifications as JSON"""

    def get(self):
        schema = self.api.__schema__
        return (
            schema,
            HTTPStatus.INTERNAL_SERVER_ERROR if "error" in schema else HTTPStatus.OK,
        )

    def mediatypes(self):
        return ["application/json"]


def mask_parse_error_handler(error):
    """When a mask can't be parsed"""
    return {"message": "Mask parse error: {0}".format(error)}, HTTPStatus.BAD_REQUEST


def mask_error_handler(error):
    """When any error occurs on mask"""
    return {"message": "Mask error: {0}".format(error)}, HTTPStatus.BAD_REQUEST
