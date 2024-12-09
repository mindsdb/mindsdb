from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    base_id={"type": ARG_TYPE.STR, "description": "The Airtable base ID."},
    table_name={"type": ARG_TYPE.STR, "description": "The Airtable table name."},
    access_token={
        "type": ARG_TYPE.STR,
        "description": "The Access Token for the Airtable API.",
        "secret": True,
    },
)

connection_args_example = OrderedDict(
    base_id="dqweqweqrwwqq", table_name="iris", access_token="knlsndlknslk"
)
