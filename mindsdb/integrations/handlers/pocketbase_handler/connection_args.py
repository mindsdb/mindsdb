from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    url={
        "type": ARG_TYPE.URL,
        "description": "The base URL of the PocketBase instance (for example https://pb.example.com).",
        "required": True,
        "label": "URL",
    },
    email={
        "type": ARG_TYPE.STR,
        "description": "Admin or user email that MindsDB should use to authenticate against PocketBase.",
        "required": True,
        "label": "Email",
    },
    password={
        "type": ARG_TYPE.PWD,
        "description": "Password that pairs with the provided email.",
        "required": True,
        "label": "Password",
        "secret": True,
    },
    collections={
        "type": list,
        "description": "Optional list of collection names to expose. Leave empty to register every available collection.",
        "required": False,
        "label": "Collections",
    },
)

connection_args_example = OrderedDict(
    url="http://127.0.0.1:8090",
    email="admin@example.com",
    password="supersecret",
    collections=["posts", "users"],
)
