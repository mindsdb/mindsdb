from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    account={
        "type": ARG_TYPE.STR,
        "description": "The Snowflake account identifier.",
        "required": True,
        "label": "Account",
    },
    user={
        "type": ARG_TYPE.STR,
        "description": "The user name used to authenticate with the Snowflake account.",
        "required": True,
        "label": "User",
    },
    password={
        "type": ARG_TYPE.PWD,
        "description": "The password to authenticate the user with the Snowflake account. Required for password authentication.",
        "required": False,
        "label": "Password",
        "secret": True,
    },
    private_key_path={
        "type": ARG_TYPE.PATH,
        "description": "Path to the private key file for key pair authentication. Required for key pair authentication.",
        "required": False,
        "label": "Private Key Path",
    },
    private_key={
        "type": ARG_TYPE.PWD,
        "description": "PEM-formatted private key content for key pair authentication. Use when the key cannot be stored on disk.",
        "required": False,
        "label": "Private Key",
        "secret": True,
    },
    private_key_passphrase={
        "type": ARG_TYPE.PWD,
        "description": "Optional passphrase for the encrypted private key.",
        "required": False,
        "label": "Private Key Passphrase",
        "secret": True,
    },
    database={
        "type": ARG_TYPE.STR,
        "description": "The database to use when connecting to the Snowflake account.",
        "required": True,
        "label": "Database",
    },
    schema={
        "type": ARG_TYPE.STR,
        "description": "The schema to use when connecting to the Snowflake account.",
        "required": False,
        "label": "Schema",
    },
    warehouse={
        "type": ARG_TYPE.STR,
        "description": "The warehouse to use when executing queries on the Snowflake account.",
        "required": False,
        "label": "Warehouse",
    },
    role={
        "type": ARG_TYPE.STR,
        "description": "The role to use when executing queries on the Snowflake account.",
        "required": False,
        "label": "Role",
    },
    auth_type={
        "type": ARG_TYPE.STR,
        "description": 'Required authentication type. Options: "password" or "key_pair".',
        "required": True,
        "label": "Auth Type",
    },
)

connection_args_example = OrderedDict(
    account="abcxyz-1234567", user="user", password="password", database="test", auth_type="password"
)
