from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    address={
        'type': ARG_TYPE.STR,
        'description': 'The hostname, IP address, or URL of the SAP HANA database.',
        'required': True,
        'label': 'Address'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The port number for connecting to the SAP HANA database.',
        'required': True,
        'label': 'Port'
    },
    user={
        'type': ARG_TYPE.STR,
        'description': 'The username for the SAP HANA database.',
        'required': True,
        'label': 'User'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password for the SAP HANA database.',
        'secret': True,
        'required': True,
        'label': 'Password'
    },
    schema={
        'type': ARG_TYPE.STR,
        'description': "The database schema to use. Defaults to the user's default schema.",
        'required': False,
        'label': 'Schema'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The name of the database to connect to. This parameter is not used for SAP HANA Cloud.',
        'required': False,
        'label': 'Database'
    },
    encrypt={
        'type': ARG_TYPE.BOOL,
        'description': 'The setting to enable or disable encryption. Default is `True`.',
        'required': False,
        'label': 'Encrypt'
    }
)

connection_args_example = OrderedDict(
    host='123e4567-e89b-12d3-a456-426614174000.hana.trial-us10.hanacloud.ondemand.com',
    port=30013,
    user='DBADMIN',
    password='password'
)
