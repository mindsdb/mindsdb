from mindsdb_sql import parse_sql

from mindsdb.integrations.handlers.lightdash_handler.api import Lightdash
from mindsdb.integrations.handlers.lightdash_handler.lightdash_tables import (
    UserTable,
    UserAbilityTable,
    OrgTable,
    OrgProjectsTable,
    OrgMembersTable,
    ProjectTable,
    WarehouseConnectionTable,
    DBTConnectionTable,
    DBTEnvironmentVarsTable,
    ChartsTable,
    SpacesTable,
    AccessTable,
    ValidationTable,
    DashboardsTable,
    QueriesTable,
    ChartHistoryTable,
    ChartConfigTable,
    ChartAdditionalMetricsTable,
    ChartTableCalculationsTable,
    SchedulerLogsTable,
    SchedulerTable,
    SchedulerJobsTable,
    SchedulerJobStatus,
)
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse


class LightdashHandler(APIHandler):

    def __init__(self, name: str, **kwargs) -> None:
        super().__init__(name)
        self.connection = None
        self.is_connected = False
        self.api_key = kwargs.get("connection_data", {}).get("api_key", "")
        self.base_url = kwargs.get("connection_data", {}).get("base_url", "")
        _tables = [
            UserTable,
            UserAbilityTable,
            OrgTable,
            OrgProjectsTable,
            OrgMembersTable,
            ProjectTable,
            WarehouseConnectionTable,
            DBTConnectionTable,
            DBTEnvironmentVarsTable,
            ChartsTable,
            SpacesTable,
            AccessTable,
            ValidationTable,
            DashboardsTable,
            QueriesTable,
            ChartHistoryTable,
            ChartConfigTable,
            ChartAdditionalMetricsTable,
            ChartTableCalculationsTable,
            SchedulerLogsTable,
            SchedulerTable,
            SchedulerJobsTable,
            SchedulerJobStatus,
        ]
        for Table in _tables:
            self._register_table(Table.name, Table(self))
        self.connect()

    def connect(self) -> Lightdash:
        self.connection = Lightdash(self.base_url, self.api_key)
        return self.connection

    def check_connection(self) -> StatusResponse:
        resp = StatusResponse(False)
        if self.connection and not self.connection.is_connected():
            resp.error = "Client not connected"
        else:
            resp.success = True
        return resp

    def native_query(self, query: str) -> StatusResponse:
        ast = parse_sql(query, dialect="mindsdb")
        return self.query(ast)
