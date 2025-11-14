from mindsdb.integrations.handlers.cassandra_handler import Handler as CassandraHandler

from mindsdb.utilities import log

logger = log.getLogger(__name__)


class DatastaxHandler(CassandraHandler):
    """
    This handler handles connection and execution of the Datastax Astra DB statements.
    """

    name = "astra"

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
        connection_data = kwargs.get("connection_data", {})
        if "secure_connect_bundle" in connection_data:
            logger.debug(
                f"Initializing Astra DB handler '{name}' with secure connect bundle"
            )
        else:
            logger.warning(
                f"Astra DB handler '{name}' initialized without secure_connect_bundle. "
                "This parameter is typically required for Astra DB connections."
            )

    def check_connection(self):
        """
        Check the connection to DataStax Astra DB.

        Returns:
            HandlerStatusResponse: Connection status
        """
        response = super().check_connection()

        # Add Astra-specific logging
        if response.success:
            logger.debug(f"Successfully connected to Astra DB: {self.name}")
        else:
            logger.error(
                f"Failed to connect to Astra DB: {self.name} - {response.error_message}"
            )

        return response
