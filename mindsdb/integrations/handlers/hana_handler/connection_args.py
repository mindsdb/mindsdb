from textwrap import dedent
from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    host={
        'type': ARG_TYPE.STR,
        'description': 'The IP address/host name of the SAP HANA instance host.'
    },
    port={
        'type': ARG_TYPE.STR,
        'description': 'The port number of the SAP HANA instance.'
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
    schema={
        'type': ARG_TYPE.STR,
        'description': 'Sets the current schema, which is used for identifiers without a schema.'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the name of the database to connect to. (Not used for SAP HANA Cloud)'
    },
    autocommit={
        'type': ARG_TYPE.BOOL,
        'description': 'Sets the autocommit mode for the connection.'
    },
    properties={
        'type': ARG_TYPE.STR,
        'description': 'Additional dictionary with special properties of the connection.'
    },
    encrypt={
        'type': ARG_TYPE.BOOL,
        'description': 'Enables or disables TLS encryption.'
    },
    sslHostNameInCertificate={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the host name used to verify server\'s identity.'
    },
    sslValidateCertificate={
        'type': ARG_TYPE.BOOL,
        'description': 'Specifies whether to validate the server\'s certificate.'
    },
    sslCryptoProvider={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the cryptographic library provider used for TLS communication.'
    },
    sslTrustStore={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the path to a trust store file that contains the server\'s public certificates.'
    },
    sslKeyStore={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the path to the keystore file that contains the client\'s identity.'
    },
    cseKeyStorePassword={
        'type': ARG_TYPE.STR,
        'description': 'Provides the password for the local key store.'
    },
    sslSNIHostname={
        'type': ARG_TYPE.STR,
        'description': dedent("""Specifies the name of the host that is attempting to connect at the start of
        the TLS handshaking process.""")
    },
    sslSNIRequest={
        'type': ARG_TYPE.BOOL,
        'description': 'Specifies whether SNI requests are enabled for TLS connections: TRUE/FALSE.'
    },
    siteType={
        'type': ARG_TYPE.STR,
        'description': dedent("""Specifies whether the connection is made to either the PRIMARY or SECONDARY
        site in an Active/Active (read enabled) system.""")
    },
    splitBatchCommands={
        'type': ARG_TYPE.BOOL,
        'description': 'Allows split and parallel execution of batch commands on partitioned tables.'
    },
    routeDirectExecute={
        'type': ARG_TYPE.BOOL,
        'description': dedent("""Converts direct execute into prepare and execute (routed execute) if the
        number of index servers is more than one and if statement routing is enabled.""")
    },
    secondarySessionFallback={
        'type': ARG_TYPE.BOOL,
        'description': dedent("""Forces the ongoing transaction on a non-anchor connection to fall back
        to the anchor/primary connection if this connection is dropped by the network or server.""")
    }
)

connection_args_example = OrderedDict(
    host='<uuid>.hana.trial-us10.hanacloud.ondemand.com',
    port=30013,
    user='DBADMIN',
    password='password',
    schema='MINDSDB',
)
