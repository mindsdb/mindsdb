from mindsdb_sql_parser import parse_sql

from mindsdb.integrations.handlers.npm_handler.api import NPM
from mindsdb.integrations.handlers.npm_handler.npm_tables import (
    NPMMetadataTable,
    NPMMaintainersTable,
    NPMKeywordsTable,
    NPMDependenciesTable,
    NPMDevDependenciesTable,
    NPMOptionalDependenciesTable,
    NPMGithubStatsTable,
)
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse


class NPMHandler(APIHandler):

    def __init__(self, name: str, **kwargs) -> None:
        super().__init__(name)
        self.connection = None
        self.is_connected = False
        _tables = [
            NPMMetadataTable,
            NPMMaintainersTable,
            NPMKeywordsTable,
            NPMDependenciesTable,
            NPMDevDependenciesTable,
            NPMOptionalDependenciesTable,
            NPMGithubStatsTable,
        ]
        for Table in _tables:
            self._register_table(Table.name, Table(self))

    def check_connection(self) -> StatusResponse:
        """Check if connected"""
        response = StatusResponse(False)
        if NPM.is_connected():
            response.success = True
        else:
            response.success = False
        self.is_connected = True
        return response

    def connect(self) -> NPM:
        """Make connection object"""
        self.connection = NPM
        return self.connection

    def native_query(self, query: str) -> StatusResponse:
        """Receive and process a raw query"""
        ast = parse_sql(query)
        return self.query(ast)
