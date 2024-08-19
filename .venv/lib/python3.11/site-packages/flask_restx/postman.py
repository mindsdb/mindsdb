# -*- coding: utf-8 -*-
from time import time
from uuid import uuid5, NAMESPACE_URL

from urllib.parse import urlencode


def clean(data):
    """Remove all keys where value is None"""
    return dict((k, v) for k, v in data.items() if v is not None)


DEFAULT_VARS = {
    "string": "",
    "integer": 0,
    "number": 0,
}


class Request(object):
    """Wraps a Swagger operation into a Postman Request"""

    def __init__(self, collection, path, params, method, operation):
        self.collection = collection
        self.path = path
        self.params = params
        self.method = method.upper()
        self.operation = operation

    @property
    def id(self):
        seed = str(" ".join((self.method, self.url)))
        return str(uuid5(self.collection.uuid, seed))

    @property
    def url(self):
        return self.collection.api.base_url.rstrip("/") + self.path

    @property
    def headers(self):
        headers = {}
        # Handle content-type
        if self.method != "GET":
            consumes = self.collection.api.__schema__.get("consumes", [])
            consumes = self.operation.get("consumes", consumes)
            if len(consumes):
                headers["Content-Type"] = consumes[-1]

        # Add all parameters headers
        for param in self.operation.get("parameters", []):
            if param["in"] == "header":
                headers[param["name"]] = param.get("default", "")

        # Add security headers if needed (global then local)
        for security in self.collection.api.__schema__.get("security", []):
            for key, header in self.collection.apikeys.items():
                if key in security:
                    headers[header] = ""
        for security in self.operation.get("security", []):
            for key, header in self.collection.apikeys.items():
                if key in security:
                    headers[header] = ""

        lines = [":".join(line) for line in headers.items()]
        return "\n".join(lines)

    @property
    def folder(self):
        if "tags" not in self.operation or len(self.operation["tags"]) == 0:
            return
        tag = self.operation["tags"][0]
        for folder in self.collection.folders:
            if folder.tag == tag:
                return folder.id

    def as_dict(self, urlvars=False):
        url, variables = self.process_url(urlvars)
        return clean(
            {
                "id": self.id,
                "method": self.method,
                "name": self.operation["operationId"],
                "description": self.operation.get("summary"),
                "url": url,
                "headers": self.headers,
                "collectionId": self.collection.id,
                "folder": self.folder,
                "pathVariables": variables,
                "time": int(time()),
            }
        )

    def process_url(self, urlvars=False):
        url = self.url
        path_vars = {}
        url_vars = {}
        params = dict((p["name"], p) for p in self.params)
        params.update(
            dict((p["name"], p) for p in self.operation.get("parameters", []))
        )
        if not params:
            return url, None
        for name, param in params.items():
            if param["in"] == "path":
                url = url.replace("{%s}" % name, ":%s" % name)
                path_vars[name] = DEFAULT_VARS.get(param["type"], "")
            elif param["in"] == "query" and urlvars:
                default = DEFAULT_VARS.get(param["type"], "")
                url_vars[name] = param.get("default", default)
        if url_vars:
            url = "?".join((url, urlencode(url_vars)))
        return url, path_vars


class Folder(object):
    def __init__(self, collection, tag):
        self.collection = collection
        self.tag = tag["name"]
        self.description = tag["description"]

    @property
    def id(self):
        return str(uuid5(self.collection.uuid, str(self.tag)))

    @property
    def order(self):
        return [r.id for r in self.collection.requests if r.folder == self.id]

    def as_dict(self):
        return clean(
            {
                "id": self.id,
                "name": self.tag,
                "description": self.description,
                "order": self.order,
                "collectionId": self.collection.id,
            }
        )


class PostmanCollectionV1(object):
    """Postman Collection (V1 format) serializer"""

    def __init__(self, api, swagger=False):
        self.api = api
        self.swagger = swagger

    @property
    def uuid(self):
        return uuid5(NAMESPACE_URL, self.api.base_url)

    @property
    def id(self):
        return str(self.uuid)

    @property
    def requests(self):
        if self.swagger:
            # First request is Swagger specifications
            yield Request(
                self,
                "/swagger.json",
                {},
                "get",
                {
                    "operationId": "Swagger specifications",
                    "summary": "The API Swagger specifications as JSON",
                },
            )
        # Then iter over API paths and methods
        for path, operations in self.api.__schema__["paths"].items():
            path_params = operations.get("parameters", [])

            for method, operation in operations.items():
                if method != "parameters":
                    yield Request(self, path, path_params, method, operation)

    @property
    def folders(self):
        for tag in self.api.__schema__["tags"]:
            yield Folder(self, tag)

    @property
    def apikeys(self):
        return dict(
            (name, secdef["name"])
            for name, secdef in self.api.__schema__.get("securityDefinitions").items()
            if secdef.get("in") == "header" and secdef.get("type") == "apiKey"
        )

    def as_dict(self, urlvars=False):
        return clean(
            {
                "id": self.id,
                "name": " ".join((self.api.title, self.api.version)),
                "description": self.api.description,
                "order": [r.id for r in self.requests if not r.folder],
                "requests": [r.as_dict(urlvars=urlvars) for r in self.requests],
                "folders": [f.as_dict() for f in self.folders],
                "timestamp": int(time()),
            }
        )
