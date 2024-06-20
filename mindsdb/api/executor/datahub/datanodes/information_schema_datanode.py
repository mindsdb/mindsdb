
import pandas as pd
from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.api.executor.datahub.datanodes.datanode import DataNode
from mindsdb.api.executor.datahub.datanodes.integration_datanode import (
    IntegrationDataNode,
)
from mindsdb.api.executor.datahub.datanodes.project_datanode import (
    ProjectDataNode,
)
from mindsdb.api.executor import exceptions as exc
from mindsdb.api.executor.utilities.sql import query_df
from mindsdb.api.executor.utilities.sql import get_query_tables
from mindsdb.interfaces.database.projects import ProjectController

from mindsdb.utilities import log

from .system_tables import (
    SchemataTable, TablesTable, ColumnsTable, EventsTable, RoutinesTable,
    PluginsTable, EnginesTable, KeyColumnUsageTable, StatisticsTable,
    CharacterSetsTable, CollationsTable)
from .mindsdb_tables import (
    ModelsTable, DatabasesTable, MLEnginesTable, HandlersTable, JobsTable,
    ChatbotsTable, KBTable, SkillsTable, AgentsTable, ViewsTable, TriggersTable)


logger = log.getLogger(__name__)


class InformationSchemaDataNode(DataNode):
    type = "INFORMATION_SCHEMA"

    tables_list = [
        SchemataTable, TablesTable, ColumnsTable, EventsTable, RoutinesTable,
        PluginsTable, EnginesTable, KeyColumnUsageTable, StatisticsTable,
        CharacterSetsTable, CollationsTable,
        ModelsTable, DatabasesTable, MLEnginesTable, HandlersTable, JobsTable,
        ChatbotsTable, KBTable, SkillsTable, AgentsTable, ViewsTable, TriggersTable
    ]

    def __init__(self, session):
        self.session = session
        self.integration_controller = session.integration_controller
        self.project_controller = ProjectController()
        self.database_controller = session.database_controller

        self.persis_datanodes = {
            'log': self.database_controller.logs_db_controller
        }

        databases = self.database_controller.get_dict()
        if "files" in databases:
            self.persis_datanodes["files"] = IntegrationDataNode(
                "files",
                ds_type="file",
                integration_controller=self.session.integration_controller,
            )

        self.tables = {t.name: t for t in self.tables_list}

    def __getitem__(self, key):
        return self.get(key)

    def get(self, name):
        name_lower = name.lower()

        if name_lower == "information_schema":
            return self

        if name_lower == 'log':
            return self.database_controller.get_system_db('log')

        if name_lower in self.persis_datanodes:
            return self.persis_datanodes[name_lower]

        existing_databases_meta = (
            self.database_controller.get_dict()
        )  # filter_type='project'
        database_name = None
        for key in existing_databases_meta:
            if key.lower() == name_lower:
                database_name = key
                break

        if database_name is None:
            return None

        database_meta = existing_databases_meta[database_name]
        if database_meta["type"] == "integration":
            integration = self.integration_controller.get(name=database_name)
            return IntegrationDataNode(
                database_name,
                ds_type=integration["engine"],
                integration_controller=self.session.integration_controller,
            )
        if database_meta["type"] == "project":
            project = self.database_controller.get_project(name=database_name)
            return ProjectDataNode(
                project=project,
                integration_controller=self.session.integration_controller,
                information_schema=self,
            )

        integration_names = self.integration_controller.get_all().keys()
        for integration_name in integration_names:
            if integration_name.lower() == name_lower:
                datasource = self.integration_controller.get(name=integration_name)
                return IntegrationDataNode(
                    integration_name,
                    ds_type=datasource["engine"],
                    integration_controller=self.session.integration_controller,
                )

        return None

    def has_table(self, tableName):
        tn = tableName.upper()
        if tn in self.tables:
            return True
        return False

    def get_table_columns(self, tableName):
        tn = tableName.upper()
        if tn in self.tables:
            return self.tables[tn].columns
        raise exc.TableNotExistError(
            f"Table information_schema.{tableName} does not exists"
        )

    def get_integrations_names(self):
        integration_names = self.integration_controller.get_all().keys()
        # remove files from list to prevent doubling in 'select from INFORMATION_SCHEMA.TABLES'
        return [x.lower() for x in integration_names if x not in ("files",)]

    def get_projects_names(self):
        projects = self.database_controller.get_dict(filter_type="project")
        return [x.lower() for x in projects]

    def get_tables(self):
        return {
            name: table
            for name, table in self.tables.items()
            if table.visible
        }

    def query(self, query: ASTNode, session=None):
        query_tables = [x[1] for x in get_query_tables(query)]

        if len(query_tables) != 1:
            raise exc.BadTableError(
                f"Only one table can be used in query to information_schema: {query}"
            )

        table_name = query_tables[0].upper()

        if table_name not in self.tables:
            raise exc.NotSupportedYet("Information schema: Not implemented.")

        tbl = self.tables[table_name]

        if hasattr(tbl, 'get_data'):
            dataframe = tbl.get_data(query=query, inf_schema=self, session=self.session)
        else:
            dataframe = self._get_empty_table(tbl)
        data = query_df(dataframe, query, session=self.session)

        columns_info = [{"name": k, "type": v} for k, v in data.dtypes.items()]

        return data, columns_info

    def _get_empty_table(self, table):
        columns = table.columns
        data = []

        df = pd.DataFrame(data, columns=columns)
        return df
