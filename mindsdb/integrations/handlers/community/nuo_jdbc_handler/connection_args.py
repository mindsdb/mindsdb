from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the NuoDB AP or TE. If is_direct is set to true then provide the TE IP else provide the AP IP.'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'Specify port to connect to NuoDB. If is_direct is set to true then provide the TE port else provide the AP port.'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': """
            The database name to use when connecting with the NuoDB.
        """
    },
    schema={
        'type': ARG_TYPE.STR,
        'description': """
            The schema name to use when connecting with the NuoDB.
        """
    },
    user={
        'type': ARG_TYPE.STR,
        'description': 'The username to authenticate with the NuoDB server.'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password to authenticate the user with the NuoDB server.',
        'secret': True
    },
    is_direct={
        'type': ARG_TYPE.STR,
        'description': 'This argument indicates whether a direct connection to the TE is to be attempted.'
    },
    jar_location={
        'type': ARG_TYPE.STR,
        'description': 'The location of the jar files which contain the JDBC class. This need not be specified if the required classes are already added to the CLASSPATH variable.'
    },
    driver_args={
        'type': ARG_TYPE.STR,
        'description': """
            The extra arguments which can be specified to the driver.
            Specify this in the format: "arg1=value1,arg2=value2.
            More information on the supported paramters can be found at: https://doc.nuodb.com/nuodb/latest/deployment-models/physical-or-vmware-environments-with-nuodb-admin/reference-information/connection-properties/'
        """
    }
)


connection_args_example = OrderedDict(
    host="localhost",
    port="48006",
    database="test",
    schema="hockey",
    user="dba",
    password="goalie",
    jar_location="/Users/kavelbaruah/Desktop/nuodb-jdbc-24.0.0.jar",
    is_direct="true",
    driver_args="schema=hockey,clientInfo=info"
)
