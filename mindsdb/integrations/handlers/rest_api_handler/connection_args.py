"""Aggregator for the rest_api handler's connection arguments.

REST/passthrough fields and authentication fields are defined in separate
modules (rest_connection_args, oauth_connection_args). This module merges
them into the single `connection_args` mapping that MindsDB expects each
handler package to export.
"""

from collections import OrderedDict

from .rest_connection_args import rest_connection_args
from .oauth_connection_args import oauth_connection_args


connection_args = OrderedDict()
connection_args.update(rest_connection_args)
connection_args.update(oauth_connection_args)


connection_args_example = OrderedDict(
    base_url="https://api.example.com",
    bearer_token="your_token_here",
    default_headers={"Accept": "application/json"},
    allowed_hosts=["api.example.com"],
)
