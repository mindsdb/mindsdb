# -*- coding: utf-8 -*-
import itertools
import re

from inspect import isclass, getdoc
from collections import OrderedDict

from collections.abc import Hashable

from flask import current_app

from . import fields
from .model import Model, ModelBase, OrderedModel
from .reqparse import RequestParser
from .utils import merge, not_none, not_none_sorted
from ._http import HTTPStatus

from urllib.parse import quote

#: Maps Flask/Werkzeug rooting types to Swagger ones
PATH_TYPES = {
    "int": "integer",
    "float": "number",
    "string": "string",
    "default": "string",
}

#: Maps Python primitives types to Swagger ones
PY_TYPES = {
    int: "integer",
    float: "number",
    str: "string",
    bool: "boolean",
    None: "void",
}

RE_URL = re.compile(r"<(?:[^:<>]+:)?([^<>]+)>")

DEFAULT_RESPONSE_DESCRIPTION = "Success"
DEFAULT_RESPONSE = {"description": DEFAULT_RESPONSE_DESCRIPTION}

RE_RAISES = re.compile(
    r"^:raises\s+(?P<name>[\w\d_]+)\s*:\s*(?P<description>.*)$", re.MULTILINE
)

RE_PARSE_RULE = re.compile(
    r"""
    (?P<static>[^<]*)                           # static rule data
    <
    (?:
        (?P<converter>[a-zA-Z_][a-zA-Z0-9_]*)   # converter name
        (?:\((?P<args>.*?)\))?                  # converter arguments
        \:                                      # variable delimiter
    )?
    (?P<variable>[a-zA-Z_][a-zA-Z0-9_]*)        # variable name
    >
    """,
    re.VERBOSE,
)


def ref(model):
    """Return a reference to model in definitions"""
    name = model.name if isinstance(model, ModelBase) else model
    return {"$ref": "#/definitions/{0}".format(quote(name, safe=""))}


def _v(value):
    """Dereference values (callable)"""
    return value() if callable(value) else value


def extract_path(path):
    """
    Transform a Flask/Werkzeug URL pattern in a Swagger one.
    """
    return RE_URL.sub(r"{\1}", path)


def parse_rule(rule):
    """
    Parse a rule and return it as generator. Each iteration yields tuples in the form
    ``(converter, arguments, variable)``. If the converter is `None` it's a static url part, otherwise it's a dynamic
    one.

    Note: This originally lived in werkzeug.routing.parse_rule until it was removed in werkzeug 2.2.0.
    """
    pos = 0
    end = len(rule)
    do_match = RE_PARSE_RULE.match
    used_names = set()
    while pos < end:
        m = do_match(rule, pos)
        if m is None:
            break
        data = m.groupdict()
        if data["static"]:
            yield None, None, data["static"]
        variable = data["variable"]
        converter = data["converter"] or "default"
        if variable in used_names:
            raise ValueError(f"variable name {variable!r} used twice.")
        used_names.add(variable)
        yield converter, data["args"] or None, variable
        pos = m.end()
    if pos < end:
        remaining = rule[pos:]
        if ">" in remaining or "<" in remaining:
            raise ValueError(f"malformed url rule: {rule!r}")
        yield None, None, remaining


def extract_path_params(path):
    """
    Extract Flask-style parameters from an URL pattern as Swagger ones.
    """
    params = OrderedDict()
    for converter, arguments, variable in parse_rule(path):
        if not converter:
            continue
        param = {"name": variable, "in": "path", "required": True}

        if converter in PATH_TYPES:
            param["type"] = PATH_TYPES[converter]
        elif converter in current_app.url_map.converters:
            param["type"] = "string"
        else:
            raise ValueError("Unsupported type converter: %s" % converter)
        params[variable] = param
    return params


def _param_to_header(param):
    param.pop("in", None)
    param.pop("name", None)
    return _clean_header(param)


def _clean_header(header):
    if isinstance(header, str):
        header = {"description": header}
    typedef = header.get("type", "string")
    if isinstance(typedef, Hashable) and typedef in PY_TYPES:
        header["type"] = PY_TYPES[typedef]
    elif (
        isinstance(typedef, (list, tuple))
        and len(typedef) == 1
        and typedef[0] in PY_TYPES
    ):
        header["type"] = "array"
        header["items"] = {"type": PY_TYPES[typedef[0]]}
    elif hasattr(typedef, "__schema__"):
        header.update(typedef.__schema__)
    else:
        header["type"] = typedef
    return not_none(header)


def parse_docstring(obj):
    raw = getdoc(obj)
    summary = raw.strip(" \n").split("\n")[0].split(".")[0] if raw else None
    raises = {}
    details = raw.replace(summary, "").lstrip(". \n").strip(" \n") if raw else None
    for match in RE_RAISES.finditer(raw or ""):
        raises[match.group("name")] = match.group("description")
        if details:
            details = details.replace(match.group(0), "")
    parsed = {
        "raw": raw,
        "summary": summary or None,
        "details": details or None,
        "returns": None,
        "params": [],
        "raises": raises,
    }
    return parsed


def is_hidden(resource, route_doc=None):
    """
    Determine whether a Resource has been hidden from Swagger documentation
    i.e. by using Api.doc(False) decorator
    """
    if route_doc is False:
        return True
    else:
        return hasattr(resource, "__apidoc__") and resource.__apidoc__ is False


def build_request_body_parameters_schema(body_params):
    """
    :param body_params: List of JSON schema of body parameters.
    :type body_params: list of dict, generated from the json body parameters of a request parser
    :return dict: The Swagger schema representation of the request body

    :Example:
        {
            'name': 'payload',
            'required': True,
            'in': 'body',
            'schema': {
                'type': 'object',
                'properties': [
                    'parameter1': {
                        'type': 'integer'
                    },
                    'parameter2': {
                        'type': 'string'
                    }
                ]
            }
        }
    """

    properties = {}
    for param in body_params:
        properties[param["name"]] = {"type": param.get("type", "string")}

    return {
        "name": "payload",
        "required": True,
        "in": "body",
        "schema": {"type": "object", "properties": properties},
    }


class Swagger(object):
    """
    A Swagger documentation wrapper for an API instance.
    """

    def __init__(self, api):
        self.api = api
        self._registered_models = {}

    def as_dict(self):
        """
        Output the specification as a serializable ``dict``.

        :returns: the full Swagger specification in a serializable format
        :rtype: dict
        """
        basepath = self.api.base_path
        if len(basepath) > 1 and basepath.endswith("/"):
            basepath = basepath[:-1]
        infos = {
            "title": _v(self.api.title),
            "version": _v(self.api.version),
        }
        if self.api.description:
            infos["description"] = _v(self.api.description)
        if self.api.terms_url:
            infos["termsOfService"] = _v(self.api.terms_url)
        if self.api.contact and (self.api.contact_email or self.api.contact_url):
            infos["contact"] = {
                "name": _v(self.api.contact),
                "email": _v(self.api.contact_email),
                "url": _v(self.api.contact_url),
            }
        if self.api.license:
            infos["license"] = {"name": _v(self.api.license)}
            if self.api.license_url:
                infos["license"]["url"] = _v(self.api.license_url)

        paths = {}
        tags = self.extract_tags(self.api)

        # register errors
        responses = self.register_errors()

        for ns in self.api.namespaces:
            for resource, urls, route_doc, kwargs in ns.resources:
                for url in self.api.ns_urls(ns, urls):
                    path = extract_path(url)
                    serialized = self.serialize_resource(
                        ns, resource, url, route_doc=route_doc, **kwargs
                    )
                    paths[path] = serialized

        # register all models if required
        if current_app.config["RESTX_INCLUDE_ALL_MODELS"]:
            for m in self.api.models:
                self.register_model(m)

        # merge in the top-level authorizations
        for ns in self.api.namespaces:
            if ns.authorizations:
                if self.api.authorizations is None:
                    self.api.authorizations = {}
                self.api.authorizations = merge(
                    self.api.authorizations, ns.authorizations
                )

        specs = {
            "swagger": "2.0",
            "basePath": basepath,
            "paths": not_none_sorted(paths),
            "info": infos,
            "produces": list(self.api.representations.keys()),
            "consumes": ["application/json"],
            "securityDefinitions": self.api.authorizations or None,
            "security": self.security_requirements(self.api.security) or None,
            "tags": tags,
            "definitions": self.serialize_definitions() or None,
            "responses": responses or None,
            "host": self.get_host(),
        }
        return not_none(specs)

    def get_host(self):
        hostname = current_app.config.get("SERVER_NAME", None) or None
        if hostname and self.api.blueprint and self.api.blueprint.subdomain:
            hostname = ".".join((self.api.blueprint.subdomain, hostname))
        return hostname

    def extract_tags(self, api):
        tags = []
        by_name = {}
        for tag in api.tags:
            if isinstance(tag, str):
                tag = {"name": tag}
            elif isinstance(tag, (list, tuple)):
                tag = {"name": tag[0], "description": tag[1]}
            elif isinstance(tag, dict) and "name" in tag:
                pass
            else:
                raise ValueError("Unsupported tag format for {0}".format(tag))
            tags.append(tag)
            by_name[tag["name"]] = tag
        for ns in api.namespaces:
            # hide namespaces without any Resources
            if not ns.resources:
                continue
            # hide namespaces with all Resources hidden from Swagger documentation
            if all(is_hidden(r.resource, route_doc=r.route_doc) for r in ns.resources):
                continue
            if ns.name not in by_name:
                tags.append(
                    {"name": ns.name, "description": ns.description}
                    if ns.description
                    else {"name": ns.name}
                )
            elif ns.description:
                by_name[ns.name]["description"] = ns.description
        return tags

    def extract_resource_doc(self, resource, url, route_doc=None):
        route_doc = {} if route_doc is None else route_doc
        if route_doc is False:
            return False
        doc = merge(getattr(resource, "__apidoc__", {}), route_doc)
        if doc is False:
            return False

        # ensure unique names for multiple routes to the same resource
        # provides different Swagger operationId's
        doc["name"] = (
            "{}_{}".format(resource.__name__, url) if route_doc else resource.__name__
        )

        params = merge(self.expected_params(doc), doc.get("params", OrderedDict()))
        params = merge(params, extract_path_params(url))
        # Track parameters for late deduplication
        up_params = {(n, p.get("in", "query")): p for n, p in params.items()}
        need_to_go_down = set()
        methods = [m.lower() for m in resource.methods or []]
        for method in methods:
            method_doc = doc.get(method, OrderedDict())
            method_impl = getattr(resource, method)
            if hasattr(method_impl, "im_func"):
                method_impl = method_impl.im_func
            elif hasattr(method_impl, "__func__"):
                method_impl = method_impl.__func__
            method_doc = merge(
                method_doc, getattr(method_impl, "__apidoc__", OrderedDict())
            )
            if method_doc is not False:
                method_doc["docstring"] = parse_docstring(method_impl)
                method_params = self.expected_params(method_doc)
                method_params = merge(method_params, method_doc.get("params", {}))
                inherited_params = OrderedDict(
                    (k, v) for k, v in params.items() if k in method_params
                )
                method_doc["params"] = merge(inherited_params, method_params)
                for name, param in method_doc["params"].items():
                    key = (name, param.get("in", "query"))
                    if key in up_params:
                        need_to_go_down.add(key)
            doc[method] = method_doc
        # Deduplicate parameters
        # For each couple (name, in), if a method overrides it,
        # we need to move the paramter down to each method
        if need_to_go_down:
            for method in methods:
                method_doc = doc.get(method)
                if not method_doc:
                    continue
                params = {
                    (n, p.get("in", "query")): p
                    for n, p in (method_doc["params"] or {}).items()
                }
                for key in need_to_go_down:
                    if key not in params:
                        method_doc["params"][key[0]] = up_params[key]
        doc["params"] = OrderedDict(
            (k[0], p) for k, p in up_params.items() if k not in need_to_go_down
        )
        return doc

    def expected_params(self, doc):
        params = OrderedDict()
        if "expect" not in doc:
            return params

        for expect in doc.get("expect", []):
            if isinstance(expect, RequestParser):
                parser_params = OrderedDict(
                    (p["name"], p) for p in expect.__schema__ if p["in"] != "body"
                )
                params.update(parser_params)

                body_params = [p for p in expect.__schema__ if p["in"] == "body"]
                if body_params:
                    params["payload"] = build_request_body_parameters_schema(
                        body_params
                    )
            elif isinstance(expect, ModelBase):
                params["payload"] = not_none(
                    {
                        "name": "payload",
                        "required": True,
                        "in": "body",
                        "schema": self.serialize_schema(expect),
                    }
                )
            elif isinstance(expect, (list, tuple)):
                if len(expect) == 2:
                    # this is (payload, description) shortcut
                    model, description = expect
                    params["payload"] = not_none(
                        {
                            "name": "payload",
                            "required": True,
                            "in": "body",
                            "schema": self.serialize_schema(model),
                            "description": description,
                        }
                    )
                else:
                    params["payload"] = not_none(
                        {
                            "name": "payload",
                            "required": True,
                            "in": "body",
                            "schema": self.serialize_schema(expect),
                        }
                    )
        return params

    def register_errors(self):
        responses = {}
        for exception, handler in self.api.error_handlers.items():
            doc = parse_docstring(handler)
            response = {"description": doc["summary"]}
            apidoc = getattr(handler, "__apidoc__", {})
            self.process_headers(response, apidoc)
            if "responses" in apidoc:
                _, model, _ = list(apidoc["responses"].values())[0]
                response["schema"] = self.serialize_schema(model)
            responses[exception.__name__] = not_none(response)
        return responses

    def serialize_resource(self, ns, resource, url, route_doc=None, **kwargs):
        doc = self.extract_resource_doc(resource, url, route_doc=route_doc)
        if doc is False:
            return
        path = {"parameters": self.parameters_for(doc) or None}
        for method in [m.lower() for m in resource.methods or []]:
            methods = [m.lower() for m in kwargs.get("methods", [])]
            if doc[method] is False or methods and method not in methods:
                continue
            path[method] = self.serialize_operation(doc, method)
            path[method]["tags"] = [ns.name]
        return not_none(path)

    def serialize_operation(self, doc, method):
        operation = {
            "responses": self.responses_for(doc, method) or None,
            "summary": doc[method]["docstring"]["summary"],
            "description": self.description_for(doc, method) or None,
            "operationId": self.operation_id_for(doc, method),
            "parameters": self.parameters_for(doc[method]) or None,
            "security": self.security_for(doc, method),
        }
        # Handle 'produces' mimetypes documentation
        if "produces" in doc[method]:
            operation["produces"] = doc[method]["produces"]
        # Handle deprecated annotation
        if doc.get("deprecated") or doc[method].get("deprecated"):
            operation["deprecated"] = True
        # Handle form exceptions:
        doc_params = list(doc.get("params", {}).values())
        all_params = doc_params + (operation["parameters"] or [])
        if all_params and any(p["in"] == "formData" for p in all_params):
            if any(p["type"] == "file" for p in all_params):
                operation["consumes"] = ["multipart/form-data"]
            else:
                operation["consumes"] = [
                    "application/x-www-form-urlencoded",
                    "multipart/form-data",
                ]
        operation.update(self.vendor_fields(doc, method))
        return not_none(operation)

    def vendor_fields(self, doc, method):
        """
        Extract custom 3rd party Vendor fields prefixed with ``x-``

        See: https://swagger.io/specification/#specification-extensions
        """
        return dict(
            (k if k.startswith("x-") else "x-{0}".format(k), v)
            for k, v in doc[method].get("vendor", {}).items()
        )

    def description_for(self, doc, method):
        """Extract the description metadata and fallback on the whole docstring"""
        parts = []
        if "description" in doc:
            parts.append(doc["description"] or "")
        if method in doc and "description" in doc[method]:
            parts.append(doc[method]["description"])
        if doc[method]["docstring"]["details"]:
            parts.append(doc[method]["docstring"]["details"])

        return "\n".join(parts).strip()

    def operation_id_for(self, doc, method):
        """Extract the operation id"""
        return (
            doc[method]["id"]
            if "id" in doc[method]
            else self.api.default_id(doc["name"], method)
        )

    def parameters_for(self, doc):
        params = []
        for name, param in doc["params"].items():
            param["name"] = name
            if "type" not in param and "schema" not in param:
                param["type"] = "string"
            if "in" not in param:
                param["in"] = "query"

            if "type" in param and "schema" not in param:
                ptype = param.get("type", None)
                if isinstance(ptype, (list, tuple)):
                    typ = ptype[0]
                    param["type"] = "array"
                    param["items"] = {"type": PY_TYPES.get(typ, typ)}

                elif isinstance(ptype, (type, type(None))) and ptype in PY_TYPES:
                    param["type"] = PY_TYPES[ptype]

            params.append(param)

        # Handle fields mask
        mask = doc.get("__mask__")
        if mask and current_app.config["RESTX_MASK_SWAGGER"]:
            param = {
                "name": current_app.config["RESTX_MASK_HEADER"],
                "in": "header",
                "type": "string",
                "format": "mask",
                "description": "An optional fields mask",
            }
            if isinstance(mask, str):
                param["default"] = mask
            params.append(param)

        return params

    def responses_for(self, doc, method):
        # TODO: simplify/refactor responses/model handling
        responses = {}

        for d in doc, doc[method]:
            if "responses" in d:
                for code, response in d["responses"].items():
                    code = str(code)
                    if isinstance(response, str):
                        description = response
                        model = None
                        kwargs = {}
                    elif len(response) == 3:
                        description, model, kwargs = response
                    elif len(response) == 2:
                        description, model = response
                        kwargs = {}
                    else:
                        raise ValueError("Unsupported response specification")
                    description = description or DEFAULT_RESPONSE_DESCRIPTION
                    if code in responses:
                        responses[code].update(description=description)
                    else:
                        responses[code] = {"description": description}
                    if model:
                        schema = self.serialize_schema(model)
                        envelope = kwargs.get("envelope")
                        if envelope:
                            schema = {"properties": {envelope: schema}}
                        responses[code]["schema"] = schema
                    self.process_headers(
                        responses[code], doc, method, kwargs.get("headers")
                    )
            if "model" in d:
                code = str(d.get("default_code", HTTPStatus.OK))
                if code not in responses:
                    responses[code] = self.process_headers(
                        DEFAULT_RESPONSE.copy(), doc, method
                    )
                responses[code]["schema"] = self.serialize_schema(d["model"])

            if "docstring" in d:
                for name, description in d["docstring"]["raises"].items():
                    for exception, handler in self.api.error_handlers.items():
                        error_responses = getattr(handler, "__apidoc__", {}).get(
                            "responses", {}
                        )
                        code = (
                            str(list(error_responses.keys())[0])
                            if error_responses
                            else None
                        )
                        if code and exception.__name__ == name:
                            responses[code] = {"$ref": "#/responses/{0}".format(name)}
                            break

        if not responses:
            responses[str(HTTPStatus.OK.value)] = self.process_headers(
                DEFAULT_RESPONSE.copy(), doc, method
            )
        return responses

    def process_headers(self, response, doc, method=None, headers=None):
        method_doc = doc.get(method, {})
        if "headers" in doc or "headers" in method_doc or headers:
            response["headers"] = dict(
                (k, _clean_header(v))
                for k, v in itertools.chain(
                    doc.get("headers", {}).items(),
                    method_doc.get("headers", {}).items(),
                    (headers or {}).items(),
                )
            )
        return response

    def serialize_definitions(self):
        return dict(
            (name, model.__schema__) for name, model in self._registered_models.items()
        )

    def serialize_schema(self, model):
        if isinstance(model, (list, tuple)):
            model = model[0]
            return {
                "type": "array",
                "items": self.serialize_schema(model),
            }

        elif isinstance(model, ModelBase):
            self.register_model(model)
            return ref(model)

        elif isinstance(model, str):
            self.register_model(model)
            return ref(model)

        elif isclass(model) and issubclass(model, fields.Raw):
            return self.serialize_schema(model())

        elif isinstance(model, fields.Raw):
            return model.__schema__

        elif isinstance(model, (type, type(None))) and model in PY_TYPES:
            return {"type": PY_TYPES[model]}

        raise ValueError("Model {0} not registered".format(model))

    def register_model(self, model):
        name = model.name if isinstance(model, ModelBase) else model
        if name not in self.api.models:
            raise ValueError("Model {0} not registered".format(name))
        specs = self.api.models[name]
        if name in self._registered_models:
            return ref(model)
        self._registered_models[name] = specs
        if isinstance(specs, ModelBase):
            for parent in specs.__parents__:
                self.register_model(parent)
        if isinstance(specs, (Model, OrderedModel)):
            for field in specs.values():
                self.register_field(field)
        return ref(model)

    def register_field(self, field):
        if isinstance(field, fields.Polymorph):
            for model in field.mapping.values():
                self.register_model(model)
        elif isinstance(field, fields.Nested):
            self.register_model(field.nested)
        elif isinstance(field, (fields.List, fields.Wildcard)):
            self.register_field(field.container)

    def security_for(self, doc, method):
        security = None
        if "security" in doc:
            auth = doc["security"]
            security = self.security_requirements(auth)

        if "security" in doc[method]:
            auth = doc[method]["security"]
            security = self.security_requirements(auth)

        return security

    def security_requirements(self, value):
        if isinstance(value, (list, tuple)):
            return [self.security_requirement(v) for v in value]
        elif value:
            requirement = self.security_requirement(value)
            return [requirement] if requirement else None
        else:
            return []

    def security_requirement(self, value):
        if isinstance(value, (str)):
            return {value: []}
        elif isinstance(value, dict):
            return dict(
                (k, v if isinstance(v, (list, tuple)) else [v])
                for k, v in value.items()
            )
        else:
            return None
