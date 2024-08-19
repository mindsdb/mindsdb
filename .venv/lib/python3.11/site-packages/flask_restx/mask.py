import logging
import re

from collections import OrderedDict
from inspect import isclass

from .errors import RestError

log = logging.getLogger(__name__)

LEXER = re.compile(r"\{|\}|\,|[\w_:\-\*]+")


class MaskError(RestError):
    """Raised when an error occurs on mask"""

    pass


class ParseError(MaskError):
    """Raised when the mask parsing failed"""

    pass


class Mask(OrderedDict):
    """
    Hold a parsed mask.

    :param str|dict|Mask mask: A mask, parsed or not
    :param bool skip: If ``True``, missing fields won't appear in result
    """

    def __init__(self, mask=None, skip=False, **kwargs):
        self.skip = skip
        if isinstance(mask, str):
            super(Mask, self).__init__()
            self.parse(mask)
        elif isinstance(mask, (dict, OrderedDict)):
            super(Mask, self).__init__(mask, **kwargs)
        else:
            self.skip = skip
            super(Mask, self).__init__(**kwargs)

    def parse(self, mask):
        """
        Parse a fields mask.
        Expect something in the form::

            {field,nested{nested_field,another},last}

        External brackets are optionals so it can also be written::

            field,nested{nested_field,another},last

        All extras characters will be ignored.

        :param str mask: the mask string to parse
        :raises ParseError: when a mask is unparseable/invalid

        """
        if not mask:
            return

        mask = self.clean(mask)
        fields = self
        previous = None
        stack = []

        for token in LEXER.findall(mask):
            if token == "{":
                if previous not in fields:
                    raise ParseError("Unexpected opening bracket")
                fields[previous] = Mask(skip=self.skip)
                stack.append(fields)
                fields = fields[previous]
            elif token == "}":
                if not stack:
                    raise ParseError("Unexpected closing bracket")
                fields = stack.pop()
            elif token == ",":
                if previous in (",", "{", None):
                    raise ParseError("Unexpected comma")
            else:
                fields[token] = True

            previous = token

        if stack:
            raise ParseError("Missing closing bracket")

    def clean(self, mask):
        """Remove unnecessary characters"""
        mask = mask.replace("\n", "").strip()
        # External brackets are optional
        if mask[0] == "{":
            if mask[-1] != "}":
                raise ParseError("Missing closing bracket")
            mask = mask[1:-1]
        return mask

    def apply(self, data):
        """
        Apply a fields mask to the data.

        :param data: The data or model to apply mask on
        :raises MaskError: when unable to apply the mask

        """
        from . import fields

        # Should handle lists
        if isinstance(data, (list, tuple, set)):
            return [self.apply(d) for d in data]
        elif isinstance(data, (fields.Nested, fields.List, fields.Polymorph)):
            return data.clone(self)
        elif type(data) == fields.Raw:
            return fields.Raw(default=data.default, attribute=data.attribute, mask=self)
        elif data == fields.Raw:
            return fields.Raw(mask=self)
        elif (
            isinstance(data, fields.Raw)
            or isclass(data)
            and issubclass(data, fields.Raw)
        ):
            # Not possible to apply a mask on these remaining fields types
            raise MaskError("Mask is inconsistent with model")
        # Should handle objects
        elif not isinstance(data, (dict, OrderedDict)) and hasattr(data, "__dict__"):
            data = data.__dict__

        return self.filter_data(data)

    def filter_data(self, data):
        """
        Handle the data filtering given a parsed mask

        :param dict data: the raw data to filter
        :param list mask: a parsed mask to filter against
        :param bool skip: whether or not to skip missing fields

        """
        out = {}
        for field, content in self.items():
            if field == "*":
                continue
            elif isinstance(content, Mask):
                nested = data.get(field, None)
                if self.skip and nested is None:
                    continue
                elif nested is None:
                    out[field] = None
                else:
                    out[field] = content.apply(nested)
            elif self.skip and field not in data:
                continue
            else:
                out[field] = data.get(field, None)

        if "*" in self.keys():
            for key, value in data.items():
                if key not in out:
                    out[key] = value
        return out

    def __str__(self):
        return "{{{0}}}".format(
            ",".join(
                [
                    "".join((k, str(v))) if isinstance(v, Mask) else k
                    for k, v in self.items()
                ]
            )
        )


def apply(data, mask, skip=False):
    """
    Apply a fields mask to the data.

    :param data: The data or model to apply mask on
    :param str|Mask mask: the mask (parsed or not) to apply on data
    :param bool skip: If rue, missing field won't appear in result
    :raises MaskError: when unable to apply the mask

    """
    return Mask(mask, skip).apply(data)
