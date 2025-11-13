import tempfile

import pandas as pd
import requests

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.util import Date


from mindsdb.integrations.libs.base import MetaDatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
)
from mindsdb_sql_parser import parse_sql


class CassandraHandler(MetaDatabaseHandler):
    """
    This handler handles connection and execution of the Cassandra statements.
    """

    name = "cassandra"

    def __init__(self, name, **kwargs):
        super().__init__(name)
        connection_data = kwargs["connection_data"]
        self.parser = parse_sql
        self.session = None
        self.is_connected = False
        self.connection_args = connection_data

    def get_tables(self) -> Response:
        """
        Get the list of tables in the connected Cassandra database.

        :return: List of table names.
        """
        sql = "DESCRIBE TABLES"
        result = self.native_query(sql)
        df = result.data_frame
        df = df.rename(columns={"name": "table_name"})
        result.data_frame = df
        return result

    def download_secure_bundle(self, url, max_size=10 * 1024 * 1024):
        """
        Downloads the secure bundle from a given URL and stores it in a temporary file.

        :param url: URL of the secure bundle to be downloaded.
        :param max_size: Maximum allowable size of the bundle in bytes. Defaults to 10MB.
        :return: Path to the downloaded secure bundle saved as a temporary file.
        :raises ValueError: If the secure bundle size exceeds the allowed `max_size`.
        """
        response = requests.get(url, stream=True, timeout=10)
        response.raise_for_status()

        content_length = int(response.headers.get("content-length", 0))
        if content_length > max_size:
            raise ValueError("Secure bundle is larger than the allowed size!")

        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            size_downloaded = 0
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    temp_file.write(chunk)
                    size_downloaded += len(chunk)
                    if size_downloaded > max_size:
                        raise ValueError(
                            "Secure bundle is larger than the allowed size!"
                        )
            return temp_file.name

    def connect(self):
        """
        Handles the connection to a Cassandra keystore.
        """
        if self.is_connected is True:
            return self.session
        auth_provider = None
        if any(key in self.connection_args for key in ("user", "password")):
            if all(key in self.connection_args for key in ("user", "password")):
                auth_provider = PlainTextAuthProvider(
                    username=self.connection_args["user"],
                    password=self.connection_args["password"],
                )
            else:
                raise ValueError(
                    "If authentication is required, both 'user' and 'password' must be provided!"
                )

        connection_props = {"auth_provider": auth_provider}
        connection_props["protocol_version"] = self.connection_args.get(
            "protocol_version", 4
        )
        secure_connect_bundle = self.connection_args.get("secure_connect_bundle")

        if secure_connect_bundle:
            if secure_connect_bundle.startswith(("http://", "https://")):
                secure_connect_bundle = self.download_secure_bundle(
                    secure_connect_bundle
                )
            connection_props["cloud"] = {"secure_connect_bundle": secure_connect_bundle}
        else:
            connection_props["contact_points"] = [self.connection_args["host"]]
            connection_props["port"] = int(self.connection_args["port"])

        cluster = Cluster(**connection_props)
        session = cluster.connect(self.connection_args.get("keyspace"))

        self.is_connected = True
        self.session = session
        return self.session

    def check_connection(self) -> StatusResponse:
        """
        Check the connection of the Cassandra database
        :return: success status and error message if error occurs
        """
        try:
            session = self.connect()
            session.execute("SELECT release_version FROM system.local")
            return StatusResponse(success=True)
        except Exception as e:
            return StatusResponse(
                success=False,
                error_message=str(e)
            )
