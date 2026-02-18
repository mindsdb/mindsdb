from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    access_token={
        "type": ARG_TYPE.STR,
        "description": " Dropbox Access Token",
        "required": True,
        "label": "Dropbox Access Token",
    },
)

connection_args_example = OrderedDict(
    access_token="ai.L-wqp3eP6r4cSWVklkKAdTNZ3VAuQjWuZMvIs1BzKvZNVW07rKbVNi5HbxvLc9q9D6qSfsf5VTsqYsNPGUkqSJBlpkr88gNboUNuhITmJG9mVw-Olniu4MO3BWVbEIphVxXxxxCd677Y",
)
