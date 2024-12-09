from typing import List

import pandas as pd
from mindsdb_sql_parser import ast

from mindsdb.integrations.utilities.handlers.query_utilities import (
    SELECTQueryExecutor,
    SELECTQueryParser,
)
from mindsdb.integrations.libs.api_handler import APIHandler, APITable
from mindsdb.integrations.utilities.sql_utils import conditions_to_filter


def val_to_string(d, k):
    if k in d:
        d[k] = str(d[k])


def move_under(d, key_contents_to_move, key_to_move_under=None):
    if key_contents_to_move not in d:
        return
    for k, v in d[key_contents_to_move].items():
        if key_to_move_under:
            d[key_to_move_under][k] = v
        else:
            d[k] = v
    del d[key_contents_to_move]


def select_keys(d, keys):
    new_d = {}
    for key in keys:
        new_d[key] = d.get(key, "")
    return new_d


class CustomAPITable(APITable):

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.handler.connect()

    def get_columns(self, ignore: List[str] = []) -> List[str]:
        return [item for item in self.columns if item not in ignore]

    def select(self, query: ast.Select) -> pd.DataFrame:
        raise NotImplementedError()

    def parse_select(self, query: ast.Select, table_name: str):
        select_statement_parser = SELECTQueryParser(query, table_name, self.get_columns())
        self.selected_columns, self.where_conditions, self.order_by_conditions, self.result_limit = select_statement_parser.parse_query()

    def get_where_param(self, query: ast.Select, param: str):
        params = conditions_to_filter(query.where)
        if param not in params:
            raise Exception(f"WHERE condition does not have '{param}' selector")
        return params[param]

    def apply_query_params(self, df, query):
        select_statement_parser = SELECTQueryParser(query, self.name, self.get_columns())
        selected_columns, _, order_by_conditions, result_limit = select_statement_parser.parse_query()
        select_statement_executor = SELECTQueryExecutor(df, selected_columns, [], order_by_conditions, result_limit)
        return select_statement_executor.execute_query()


class UserTable(CustomAPITable):
    name: str = "user"
    columns: List[str] = [
        'userUuid',
        'email',
        'firstName',
        'lastName',
        'organizationUuid',
        'organizationName',
        'organizationCreatedAt',
        'isTrackingAnonymized',
        'isMarketingOptedIn',
        'isSetupComplete',
        'role',
        'isActive',
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = select_keys(self.connection.get_user(), self.columns)
        df = pd.DataFrame.from_records([data])
        return self.apply_query_params(df, query)


class UserAbilityTable(CustomAPITable):
    name: str = "user_ability"
    columns: List[str] = [
        'action',
        'subject',
        'conditions'
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = select_keys(self.connection.get_user(), ["abilityRules"])
        for d in data:
            val_to_string(d, "condition")
        df = pd.DataFrame.from_records(data, columns=self.columns)
        return self.apply_query_params(df, query)


class OrgTable(CustomAPITable):
    name: str = "org"
    columns: List[str] = [
        'organizationUuid',
        'defaultProjectUuid'
        'name',
        'chartColors',
        'needsProject',
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = select_keys(self.connection.get_org(), self.columns)
        val_to_string(data, "chartColors")
        df = pd.DataFrame.from_records([data])
        return self.apply_query_params(df, query)


class OrgProjectsTable(CustomAPITable):
    name: str = "org_projects"
    columns: List[str] = [
        'name',
        'projectUuid',
        'type',
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get_projects()
        df = pd.DataFrame.from_records(data, columns=self.columns)
        return self.apply_query_params(df, query)


class OrgMembersTable(CustomAPITable):
    name: str = "org_members"
    columns: List[str] = [
        'userUuid',
        'firstName',
        'lastName',
        'email',
        'organizationUuid',
        'role',
        'isActive',
        'isInviteExpired',
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get_org_members()
        df = pd.DataFrame.from_records(data, columns=self.columns)
        return self.apply_query_params(df, query)


class ProjectTable(CustomAPITable):
    name: str = "project_table"
    columns: List[str] = [
        'organizationUuid',
        'projectUuid',
        'name',
        'type',
        'pinnedListUuid',
        'copiedFromProjectUuid',
        'dbtVersion',
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        project_uuid = self.get_where_param(query, 'project_uuid')
        data = select_keys(self.connection.get_project(project_uuid), self.columns)
        df = pd.DataFrame.from_records([data])
        return self.apply_query_params(df, query)


class WarehouseConnectionTable(CustomAPITable):
    name: str = "warehouse_connection"
    columns: List[str] = [
        "role",
        "type",
        "account",
        "database",
        "warehouse",
        "schema",
        "threads",
        "clientSessionKeepAlive",
        "queryTag",
        "accessUrl",
        "startOfWeek",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        project_uuid = self.get_where_param(query, 'project_uuid')
        data = select_keys(self.connection.get_project(project_uuid).get("warehouseConnection", {}), self.columns)
        df = pd.DataFrame.from_records([data])
        return self.apply_query_params(df, query)


class DBTConnectionTable(CustomAPITable):
    name: str = "dbt_connection"
    columns: List[str] = [
        "type",
        "target",
        "profiles_dir",
        "project_dir",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        project_uuid = self.get_where_param(query, "project_uuid")
        data = select_keys(self.connection.get_project(project_uuid).get("dbtConnection", {}), self.columns)
        df = pd.DataFrame.from_records([data])
        return self.apply_query_params(df, query)


class DBTEnvironmentVarsTable(CustomAPITable):
    name: str = "dbt_env_vars"
    columns: List[str] = [
        "value",
        "key",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        project_uuid = self.get_where_param(query, "project_uuid")
        data = self.connection.get_project(project_uuid).get("dbtConnection", {}).get("environment", [])
        df = pd.DataFrame.from_records(data, columns=self.columns)
        return self.apply_query_params(df, query)


class ChartsTable(CustomAPITable):
    name: str = "charts"
    columns: List[str] = [
        "name",
        "organizationUuid",
        "uuid",
        "description",
        "projectUuid",
        "spaceUuid",
        "pinnedListUuid",
        "spaceName",
        "dashboardUuid",
        "dashboardName",
        "chartType",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        project_uuid = self.get_where_param(query, "project_uuid")
        data = self.connection.get_charts_in_project(project_uuid)
        df = pd.DataFrame.from_records(data, columns=self.columns)
        return self.apply_query_params(df, query)


class SpacesTable(CustomAPITable):
    name: str = "spaces"
    columns: List[str] = [
        "name",
        "organizationUuid",
        "uuid",
        "projectUuid",
        "pinnedListUuid",
        "pinnedListOrder",
        "isPrivate",
        "dashboardCount",
        "chartCount",
        "access",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        project_uuid = self.get_where_param(query, "project_uuid")
        data = self.connection.get_spaces_in_project(project_uuid)
        for d in data:
            val_to_string(d, "access")
        df = pd.DataFrame.from_records(data, columns=self.columns)
        return self.apply_query_params(df, query)


class AccessTable(CustomAPITable):
    name: str = "access"
    columns: List[str] = [
        "lastName",
        "firstName",
        "email",
        "role",
        "projectUuid",
        "userUuid",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        project_uuid = self.get_where_param(query, "project_uuid")
        data = self.connection.get_project_access_list(project_uuid)
        df = pd.DataFrame.from_records(data, columns=self.columns)
        return self.apply_query_params(df, query)


class ValidationTable(CustomAPITable):
    name: str = "validation"
    columns: List[str] = [
        "source",
        "spaceUuid",
        "projectUuid",
        "errorType",
        "error",
        "name",
        "createdAt",
        "validationId",
        "chartName",
        "chartViews",
        "lastUpdatedAt",
        "lastUpdatedBy",
        "fieldName",
        "chartType",
        "chartUuid",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        project_uuid = self.get_where_param(query, "project_uuid")
        data = self.connection.get_validation_results(project_uuid)
        df = pd.DataFrame.from_records(data, columns=self.columns)
        return self.apply_query_params(df, query)


class DashboardsTable(CustomAPITable):
    name: str = "dashboards"
    columns: List[str] = [
        "name",
        "organizationUuid",
        "uuid",
        "description",
        "updatedAt",
        "projectUuid",
        "spaceUuid",
        "views",
        "firstViewedAt",
        "pinnedListUuid",
        "pinnedListOrder",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        project_uuid = self.get_where_param(query, "project_uuid")
        space_uuid = self.get_where_param(query, "space_uuid")
        data = []
        for row in self.connection.get_space(project_uuid, space_uuid).get("dashboards", []):
            data.append(select_keys(row, self.columns))
        df = pd.DataFrame.from_records(data, columns=self.columns)
        return self.apply_query_params(df, query)


class QueriesTable(CustomAPITable):
    name: str = "queries"
    columns: List[str] = [
        "name",
        "uuid",
        "description",
        "updatedAt",
        "spaceUuid",
        "pinnedListUuid",
        "pinnedListOrder",
        "firstViewedAt",
        "views",
        "chartType",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        project_uuid = self.get_where_param(query, "project_uuid")
        space_uuid = self.get_where_param(query, "space_uuid")
        data = []
        for row in self.connection.get_space(project_uuid, space_uuid).get("queries", []):
            data.append(select_keys(row, self.columns))
        df = pd.DataFrame.from_records(data, columns=self.columns)
        return self.apply_query_params(df, query)


class ChartHistoryTable(CustomAPITable):
    name: str = "chart_history"
    columns: List[str] = [
        "createdAt",
        "chartUuid",
        "versionUuid",
        "createdBy",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        chart_uuid = self.get_where_param(query, "chart_uuid")
        data = []
        for row in self.connection.get_chart_version_history(chart_uuid):
            d = select_keys(row, self.columns)
            val_to_string(d, "createdBy")
            data.append(d)
        df = pd.DataFrame.from_records(data, columns=self.columns)
        return self.apply_query_params(df, query)


class ChartConfigTable(CustomAPITable):
    name: str = "chart_config"
    columns: List[str] = [
        "legendPosition",
        "showLegend",
        "groupSortOverrides",
        "groupValueOptionOverrides",
        "groupColorOverrides",
        "groupLabelOverrides",
        "showPercentage",
        "showValue",
        "valueLabel",
        "isDonut",
        "metricId",
        "groupFieldIds"
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        chart_uuid = self.get_where_param(query, "chart_uuid")
        version_uuid = self.get_where_param(query, "version_uuid")
        raw_data = self.connection.get_chart(chart_uuid, version_uuid).get("chart", {}).get("chart_config", {})
        config_data = raw_data.get("config", {})
        val_to_string(config_data, "groupSortOverrides")
        val_to_string(config_data, "groupValueOptionOverrides")
        val_to_string(config_data, "groupColorOverrides")
        val_to_string(config_data, "groupLabelOverrides")
        val_to_string(config_data, "groupFieldIds")
        config_data = select_keys(config_data, self.columns)
        df = pd.DataFrame.from_records([{**config_data, "type": raw_data.get("type", "")}], columns=self.columns)
        return self.apply_query_params(df, query)


class ChartAdditionalMetricsTable(CustomAPITable):
    name: str = "chart_additional_metrics"
    columns: List[str] = [
        "label",
        "type",
        "description",
        "sql",
        "hidden",
        "round",
        "compact",
        "format",
        "table",
        "name",
        "index",
        "filters",
        "baseDimensionName",
        "uuid",
        "percentile",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        chart_uuid = self.get_where_param(query, "chart_uuid")
        version_uuid = self.get_where_param(query, "version_uuid")
        data = self.connection.get_chart(chart_uuid, version_uuid).get("metricQuery", {}).get("additionalMetrics", [])
        for d in data:
            val_to_string(data, "filters")
            d = select_keys(d, self.columns)
        df = pd.DataFrame.from_records(data, columns=self.columns)
        return self.apply_query_params(df, query)


class ChartTableCalculationsTable(CustomAPITable):
    name: str = "chart_table_calculations"
    columns: List[str] = [
        "suffix",
        "prefix",
        "compact",
        "currency",
        "separator",
        "round",
        "type",
        "sql",
        "displayName",
        "name",
        "index",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        chart_uuid = self.get_where_param(query, "chart_uuid")
        version_uuid = self.get_where_param(query, "version_uuid")
        data = self.connection.get_chart(chart_uuid, version_uuid).get("metricQuery", {}).get("tableCalculations", [])
        for d in data:
            move_under(d, "format")
            d = select_keys(d, self.columns)
        df = pd.DataFrame.from_records(data, columns=self.columns)
        return self.apply_query_params(df, query)


class SchedulerLogsTable(CustomAPITable):
    name: str = "scheduler_logs"
    columns: List[str] = [
        "details",
        "targetType",
        "target",
        "status",
        "createdAt",
        "scheduledTime",
        "jobGroup",
        "jobId",
        "schedulerUuid",
        "task",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        project_uuid = self.get_where_param(query, "project_uuid")
        data = self.connection.get_scheduler_logs(project_uuid).get("logs", [])
        for d in data:
            val_to_string(d, "details")
            d = select_keys(d, self.columns)
        df = pd.DataFrame.from_records(data, columns=self.columns)
        return self.apply_query_params(df, query)


class SchedulerTable(CustomAPITable):
    name: str = "scheduler"
    columns: List[str] = [
        "options",
        "dashboardUuid",
        "savedChartUuid",
        "cron",
        "format",
        "createdBy",
        "updatedAt",
        "createdAt",
        "message",
        "name",
        "schedulerUuid",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        scheduler_uuid = self.get_where_param(query, "scheduler_uuid")
        data = select_keys(self.connection.get_scheduler(scheduler_uuid), self.columns)
        val_to_string(data, "options")
        df = pd.DataFrame.from_records([data], columns=self.columns)
        return self.apply_query_params(df, query)


class SchedulerJobsTable(CustomAPITable):
    name: str = "scheduler_jobs"
    columns: List[str] = [
        "id",
        "date",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        scheduler_uuid = self.get_where_param(query, "scheduler_uuid")
        data = self.connection.get_scheduler_jobs(scheduler_uuid)
        for d in data:
            d = select_keys(d, self.columns)
        df = pd.DataFrame.from_records(data, columns=self.columns)
        return self.apply_query_params(df, query)


class SchedulerJobStatus(CustomAPITable):
    name: str = "scheduler_job_status"
    columns: List[str] = [
        "status",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        job_id = self.get_where_param(query, "job_id")
        data = self.connection.get_scheduler_jobs(job_id)
        data = select_keys(data, self.columns)
        df = pd.DataFrame.from_records([data], columns=self.columns)
        return self.apply_query_params(df, query)
