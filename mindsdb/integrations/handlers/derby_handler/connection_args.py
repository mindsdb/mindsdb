from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the Apache Derby server/database.'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'Specify port to connect to Apache Derby.'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': """
            The database name to use when connecting with the Apache Derby server.
        """
    },
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the Apache Derby server. If specified this is also treated as the schema.'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password to authenticate the user with the Apache Derby server.',
        'secret': True
    },
    jdbcClass={
        'type': ARG_TYPE.STR,
        'description': 'The jdbc class which should be used to establish the connection, the default value is:  org.apache.derby.jdbc.ClientDriver.'
    },
    jdbcJarLocation={
        'type': ARG_TYPE.STR,
        'description': 'The location of the jar files which contain the JDBC class. This need not be specified if the required classes are already added to the CLASSPATH variable.'
    }

)


connection_args_example = OrderedDict(
    host='localhost',
    port='1527',
    user='test',
    password='test',
    database="testdb",
    jdbcClass='org.apache.derby.jdbc.ClientDriver',
    jdbcJarLocation='/opt/homebrew/Cellar/derby/10.16.1.1/libexec/lib/derby.jar,/opt/homebrew/Cellar/derby/10.16.1.1/libexec/lib/derbyclient.jar,/opt/homebrew/Cellar/derby/10.16.1.1/libexec/lib/derbynet.jar,/opt/homebrew/Cellar/derby/10.16.1.1/libexec/lib/derbyoptionaltools.jar,/opt/homebrew/Cellar/derby/10.16.1.1/libexec/lib/derbyrun.jar,/opt/homebrew/Cellar/derby/10.16.1.1/libexec/lib/derbyshared.jar,/opt/homebrew/Cellar/derby/10.16.1.1/libexec/lib/derbytools.jar',
)
