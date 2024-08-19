"""
This module give access to OpenAPI specifications schemas
and allows to validate specs against them.

.. versionadded:: 0.12.1
"""
import io
import json

import importlib_resources

from collections.abc import Mapping
from jsonschema import Draft4Validator

from flask_restx import errors


class SchemaValidationError(errors.ValidationError):
    """
    Raised when specification is not valid

    .. versionadded:: 0.12.1
    """

    def __init__(self, msg, errors=None):
        super(SchemaValidationError, self).__init__(msg)
        self.errors = errors

    def __str__(self):
        msg = [self.msg]
        for error in sorted(self.errors, key=lambda e: e.path):
            path = ".".join(error.path)
            msg.append("- {}: {}".format(path, error.message))
            for suberror in sorted(error.context, key=lambda e: e.schema_path):
                path = ".".join(suberror.schema_path)
                msg.append("  - {}: {}".format(path, suberror.message))
        return "\n".join(msg)

    __unicode__ = __str__


class LazySchema(Mapping):
    """
    A thin wrapper around schema file lazy loading the data on first access

    :param filename str: The package relative json schema filename
    :param validator: The jsonschema validator class version

    .. versionadded:: 0.12.1
    """

    def __init__(self, filename, validator=Draft4Validator):
        super(LazySchema, self).__init__()
        self.filename = filename
        self._schema = None
        self._validator = validator

    def _load(self):
        if not self._schema:
            ref = importlib_resources.files(__name__) / self.filename

            with io.open(ref) as infile:
                self._schema = json.load(infile)

    def __getitem__(self, key):
        self._load()
        return self._schema.__getitem__(key)

    def __iter__(self):
        self._load()
        return self._schema.__iter__()

    def __len__(self):
        self._load()
        return self._schema.__len__()

    @property
    def validator(self):
        """The jsonschema validator to validate against"""
        return self._validator(self)


#: OpenAPI 2.0 specification schema
OAS_20 = LazySchema("oas-2.0.json")

#: Map supported OpenAPI versions to their JSON schema
VERSIONS = {
    "2.0": OAS_20,
}


def validate(data):
    """
    Validate an OpenAPI specification.

    Supported OpenAPI versions: 2.0

    :param data dict: The specification to validate
    :returns boolean: True if the specification is valid
    :raises SchemaValidationError: when the specification is invalid
    :raises flask_restx.errors.SpecsError: when it's not possible to determinate
                                              the schema to validate against

    .. versionadded:: 0.12.1
    """
    if "swagger" not in data:
        raise errors.SpecsError("Unable to determinate OpenAPI schema version")

    version = data["swagger"]
    if version not in VERSIONS:
        raise errors.SpecsError('Unknown OpenAPI schema version "{}"'.format(version))

    validator = VERSIONS[version].validator

    validation_errors = list(validator.iter_errors(data))
    if validation_errors:
        raise SchemaValidationError(
            "OpenAPI {} validation failed".format(version), errors=validation_errors
        )
    return True
