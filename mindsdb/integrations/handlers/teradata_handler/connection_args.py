from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    host={
        'type': ARG_TYPE.STR,
        'description': 'The IP address/host name of the Teradata instance host.'
    },
    user={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the user name.'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'Specifies the password for the user.',
        'secret': True
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the initial database to use after logon, instead of the default database.'
    },
    dbs_port={
        'type': ARG_TYPE.STR,
        'description': 'The port number of the Teradata instance.'
    },
    encryptdata={
        'type': ARG_TYPE.STR,
        'description': 'Controls encryption of data exchanged between the driver and the database.'
    },
    https_port={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the database port number for HTTPS/TLS connections.'
    },
    sslca={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the file name of a PEM file that contains Certificate Authority (CA) certificates.'
    },
    sslcapath={
        'type': ARG_TYPE.STR,
        'description': 'Specifies a directory of PEM files that contain Certificate Authority (CA) certificates.'
    },
    sslcipher={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the TLS cipher for HTTPS/TLS connections.'
    },
    sslmode={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the SSL mode for HTTPS/TLS connections.'
    },
    sslprotocol={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the TLS protocol for HTTPS/TLS connections.'
    },
    tmode={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the Teradata transaction mode.'
    },
    logmech={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the login authentication method.'
    },
    logdata={
        'type': ARG_TYPE.STR,
        'description': 'Specifies extra data for the chosen logon authentication method.'
    },
    browser={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the command to open the browser for Browser Authentication.'
    }
)

connection_args_example = OrderedDict(
    host='192.168.0.41',
    user='dbc',
    password='dbc',
    database='HR'
)
