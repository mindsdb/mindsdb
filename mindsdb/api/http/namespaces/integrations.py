from http import HTTPStatus

from flask import request
from flask_restx import Resource

from mindsdb.api.http.utils import http_error
from mindsdb.api.http.namespaces.configs.integrations import ns_conf
from mindsdb.api.mysql.mysql_proxy.classes.fake_mysql_proxy import FakeMysqlProxy
from mindsdb.integrations.libs.bearer_passthrough import BearerPassthroughMixin
from mindsdb.integrations.libs.passthrough_types import (
    ALLOWED_METHODS,
    FORBIDDEN_REQUEST_HEADERS,
    HostNotAllowedError,
    PassthroughConfigError,
    PassthroughError,
    PassthroughNotSupportedError,
    PassthroughRequest,
    PassthroughResponse,
    PassthroughValidationError,
)
from mindsdb.interfaces.database.integrations import integration_controller
from mindsdb.metrics.metrics import api_endpoint_metrics
from mindsdb.utilities import log

logger = log.getLogger(__name__)


def _handler_supports_bearer_passthrough(handler_module) -> bool:
    handler_cls = getattr(handler_module, "Handler", None)
    if handler_cls is None:
        return False
    return issubclass(handler_cls, BearerPassthroughMixin)


def _get_bearer_passthrough_handler(name: str):
    """Look up the datasource's handler and verify it opts into the mixin."""
    proxy = FakeMysqlProxy()
    handler = proxy.session.integration_controller.get_data_handler(name)
    if not isinstance(handler, BearerPassthroughMixin):
        raise PassthroughNotSupportedError(
            f"datasource '{name}' does not support REST passthrough"
        )
    return handler


def _parse_passthrough_request(payload: dict) -> PassthroughRequest:
    if not isinstance(payload, dict):
        raise PassthroughValidationError("request body must be a JSON object")

    method = payload.get("method")
    path = payload.get("path")
    if not isinstance(method, str) or method.upper() not in ALLOWED_METHODS:
        raise PassthroughValidationError(
            f"'method' must be one of {sorted(ALLOWED_METHODS)}"
        )
    if not isinstance(path, str) or not path.startswith("/"):
        raise PassthroughValidationError("'path' must be a string starting with '/'")

    headers = payload.get("headers") or {}
    if not isinstance(headers, dict):
        raise PassthroughValidationError("'headers' must be an object")
    for name in headers:
        if not isinstance(name, str):
            raise PassthroughValidationError("header names must be strings")
        if name.lower() in FORBIDDEN_REQUEST_HEADERS or name.lower().startswith("proxy-"):
            raise PassthroughValidationError(
                f"header '{name}' is not allowed in passthrough requests"
            )

    query = payload.get("query") or {}
    if not isinstance(query, dict):
        raise PassthroughValidationError("'query' must be an object")

    return PassthroughRequest(
        method=method.upper(),
        path=path,
        query={str(k): str(v) for k, v in query.items()},
        headers={str(k): str(v) for k, v in headers.items()},
        body=payload.get("body"),
    )


def _serialize_response(resp: PassthroughResponse) -> dict:
    return {
        "status_code": resp.status_code,
        "headers": resp.headers,
        "body": resp.body,
        "content_type": resp.content_type,
    }


def _passthrough_error_response(err: PassthroughError):
    return {
        "error_code": err.error_code,
        "message": str(err),
    }, err.http_status


@ns_conf.route("/<name>/passthrough")
@ns_conf.param("name", "Datasource name")
class Passthrough(Resource):
    @ns_conf.doc("passthrough")
    @api_endpoint_metrics("POST", "/integrations/passthrough")
    def post(self, name: str):
        payload = request.json or {}
        try:
            req = _parse_passthrough_request(payload)
            handler = _get_bearer_passthrough_handler(name)
            response = handler.api_passthrough(req)
        except PassthroughError as e:
            return _passthrough_error_response(e)
        except Exception as e:  # noqa: BLE001
            logger.exception("passthrough failed for datasource %s", name)
            return http_error(HTTPStatus.INTERNAL_SERVER_ERROR, "PassthroughError", str(e))

        return _serialize_response(response), 200


@ns_conf.route("/<name>/passthrough/test")
@ns_conf.param("name", "Datasource name")
class PassthroughTest(Resource):
    @ns_conf.doc("passthrough_test")
    @api_endpoint_metrics("POST", "/integrations/passthrough/test")
    def post(self, name: str):
        try:
            handler = _get_bearer_passthrough_handler(name)
        except PassthroughError as e:
            return _passthrough_error_response(e)
        except Exception as e:  # noqa: BLE001
            logger.exception("passthrough test lookup failed for datasource %s", name)
            return http_error(HTTPStatus.INTERNAL_SERVER_ERROR, "PassthroughError", str(e))

        result = handler.test_passthrough()
        return result, 200


@ns_conf.route("/capabilities")
class Capabilities(Resource):
    """Return the list of engines (by name) that implement each passthrough flavor.

    Minds polls this at startup to build its engine allowlist and the
    ``supports_passthrough`` flag on datasource GETs.
    """

    @ns_conf.doc("integration_capabilities")
    @api_endpoint_metrics("GET", "/integrations/capabilities")
    def get(self):
        bearer_engines: list[str] = []
        handler_modules = getattr(integration_controller, "handler_modules", {}) or {}
        for engine, module in handler_modules.items():
            try:
                if _handler_supports_bearer_passthrough(module):
                    bearer_engines.append(engine)
            except Exception:
                # A broken handler module should not break the capabilities endpoint.
                logger.debug("skipping handler %s during capability probe", engine, exc_info=True)
        bearer_engines.sort()
        return {"bearer_passthrough": bearer_engines}, 200
