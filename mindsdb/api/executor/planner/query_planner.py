import copy

import pandas as pd

from mindsdb_sql_parser import ast
from mindsdb_sql_parser.ast import (
    Select,
    Identifier,
    Join,
    Star,
    BinaryOperation,
    Constant,
    Union,
    CreateTable,
    Function,
    Insert,
    Except,
    Intersect,
    Update,
    NativeQuery,
    Parameter,
    Delete,
)

from mindsdb.api.executor.planner.exceptions import PlanningException
from mindsdb.api.executor.planner import utils
from mindsdb.api.executor.planner.query_plan import QueryPlan
from mindsdb.api.executor.planner.steps import (
    PlanStep,
    FetchDataframeStep,
    ProjectStep,
    ApplyPredictorStep,
    ApplyPredictorRowStep,
    UnionStep,
    GetPredictorColumns,
    SaveToTable,
    InsertToTable,
    UpdateToTable,
    SubSelectStep,
    QueryStep,
    JoinStep,
    DeleteStep,
    DataStep,
    CreateTableStep,
    FetchDataframeStepPartition,
)
from mindsdb.api.executor.planner.utils import (
    disambiguate_predictor_column_identifier,
    recursively_extract_column_values,
    query_traversal,
    filters_to_bin_op,
)
from mindsdb.api.executor.planner.plan_join import PlanJoin
from mindsdb.api.executor.planner.query_prepare import PreparedStatementPlanner
from mindsdb.utilities.config import config


default_project = config.get("default_project")

# This includes built-in MindsDB SQL functions and functions to be executed via DuckDB consistently.
MINDSDB_SQL_FUNCTIONS = {"llm", "to_markdown", "hash"}


class QueryPlanner:
    def __init__(
        self,
        query=None,
        integrations: list = None,
        predictor_namespace=None,
        predictor_metadata: list = None,
        default_namespace: str = None,
    ):
        self.query = query
        self.plan = QueryPlan()

        _projects = set()
        self.integrations = {}
        if integrations is not None:
            for integration in integrations:
                if isinstance(integration, dict):
                    integration_name = integration["name"].lower()
                    # it is project of system database
                    if integration["type"] != "data":
                        _projects.add(integration_name)
                        continue
                else:
                    integration_name = integration.lower()
                    integration = {"name": integration}
                self.integrations[integration_name] = integration

        # allow to select from mindsdb namespace
        _projects.add(default_project)

        self.default_namespace = default_namespace

        # legacy parameter
        self.predictor_namespace = predictor_namespace.lower() if predictor_namespace else default_project

        # map for lower names of predictors

        self.predictor_info = {}
        if isinstance(predictor_metadata, list):
            # convert to dict
            for predictor in predictor_metadata:
                if "integration_name" in predictor:
                    integration_name = predictor["integration_name"]
                else:
                    integration_name = self.predictor_namespace
                    predictor["integration_name"] = integration_name
                idx = f"{integration_name}.{predictor['name']}".lower()
                self.predictor_info[idx] = predictor
                _projects.add(integration_name.lower())
        elif isinstance(predictor_metadata, dict):
            # legacy behaviour
            for name, predictor in predictor_metadata.items():
                if "." not in name:
                    if "integration_name" in predictor:
                        integration_name = predictor["integration_name"]
                    else:
                        integration_name = self.predictor_namespace
                        predictor["integration_name"] = integration_name
                    name = f"{integration_name}.{name}".lower()
                    _projects.add(integration_name.lower())

                self.predictor_info[name] = predictor

        self.projects = list(_projects)
        self.databases = list(self.integrations.keys()) + self.projects

        self.statement = None

        self.cte_results = {}

    def is_predictor(self, identifier):
        if not isinstance(identifier, Identifier):
            return False
        return self.get_predictor(identifier) is not None

    def get_predictor(self, identifier):
        name_parts = list(identifier.parts)

        version = None
        if len(name_parts) > 1 and name_parts[-1].isdigit():
            # last part is version
            version = name_parts[-1]
            name_parts = name_parts[:-1]

        name = name_parts[-1]

        namespace = None
        if len(name_parts) > 1:
            namespace = name_parts[-2]
        else:
            if self.default_namespace is not None:
                namespace = self.default_namespace

        idx_ar = [name]
        if namespace is not None:
            idx_ar.insert(0, namespace)

        idx = ".".join(idx_ar).lower()
        info = self.predictor_info.get(idx)
        if info is not None:
            info["version"] = version
            info["name"] = name
        return info

    def prepare_integration_select(self, database, query):
        # replacement for 'utils.recursively_disambiguate_*' functions from utils
        #   main purpose: make tests working (don't change planner outputs)
        # can be removed in future (with adapting the tests) except 'cut integration part' block

        def _prepare_integration_select(node, is_table, is_target, parent_query, **kwargs):
            if not isinstance(node, Identifier):
                return

            # cut integration part
            if len(node.parts) > 1 and node.parts[0].lower() == database:
                node.parts.pop(0)
                node.is_quoted.pop(0)

            if not hasattr(parent_query, "from_table"):
                return

            table = parent_query.from_table
            if not is_table:
                # add table name or alias for identifiers
                if isinstance(table, Join):
                    # skip for join
                    return

                # keep column name for target
                if is_target:
                    if node.alias is None:
                        last_part = node.parts[-1]
                        if isinstance(last_part, str):
                            node.alias = Identifier(parts=[node.parts[-1]])

        query_traversal(query, _prepare_integration_select)

    def get_integration_select_step(self, select: Select, params: dict = None) -> PlanStep:
        """
        Generate planner step to execute query over integration or over results of previous step (if it is CTE)
        """

        if isinstance(select.from_table, NativeQuery):
            integration_name = select.from_table.integration.parts[-1]
        else:
            integration_name, table = self.resolve_database_table(select.from_table)

            # is it CTE?
            table_name = table_alias = table.parts[-1]
            if table.alias is not None:
                table_alias = table.alias.parts[-1]

            if integration_name == self.default_namespace and table_name in self.cte_results:
                select.from_table = None
                return SubSelectStep(select, self.cte_results[table_name], table_name=table_alias)

        fetch_df_select = copy.deepcopy(select)
        self.prepare_integration_select(integration_name, fetch_df_select)

        # remove predictor params
        if fetch_df_select.using is not None:
            fetch_df_select.using = None
        fetch_params = self.get_fetch_params(params)
        return FetchDataframeStep(integration=integration_name, query=fetch_df_select, params=fetch_params)

    def get_fetch_params(self, params):
        # extracts parameters for fetching

        if params:
            fetch_params = params.copy()
            # remove partition parameters
            for key in ("batch_size", "track_column"):
                if key in params:
                    del params[key]
            if "track_column" in fetch_params and isinstance(fetch_params["track_column"], Identifier):
                fetch_params["track_column"] = fetch_params["track_column"].parts[-1]
        else:
            fetch_params = None
        return fetch_params

    def plan_integration_select(self, select):
        """Plan for a select query that can be fully executed in an integration"""

        return self.plan.add_step(self.get_integration_select_step(select, params=select.using))

    def resolve_database_table(self, node: Identifier):
        # resolves integration name and table name

        parts = node.parts.copy()
        alias = None
        if node.alias is not None:
            alias = node.alias.copy()

        database = self.default_namespace

        err_msg_suffix = ""
        if len(parts) > 1:
            if parts[0].lower() in self.databases:
                database = parts.pop(0).lower()
            else:
                err_msg_suffix = f"'{parts[0].lower()}' is not valid database name."

        if database is None:
            raise PlanningException(
                f"Invalid or missing database name for identifier '{node}'. {err_msg_suffix}\n"
                "Query must include a valid database name prefix in format: 'database_name.table_name' or 'database_name.schema_name.table_name'"
            )

        return database, Identifier(parts=parts, alias=alias)

    def get_query_info(self, query):
        # get all predictors
        mdb_entities = []
        predictors = []
        user_functions = []
        # projects = set()
        integrations = set()

        def find_objects(node, is_table, **kwargs):
            if isinstance(node, Function):
                if node.namespace is not None or node.op.lower() in MINDSDB_SQL_FUNCTIONS:
                    user_functions.append(node)

            if is_table:
                if isinstance(node, ast.Identifier):
                    integration, _ = self.resolve_database_table(node)

                    if self.is_predictor(node):
                        predictors.append(node)

                    if integration in self.projects:
                        # it is project
                        mdb_entities.append(node)

                    elif integration is not None:
                        integrations.add(integration)
                if isinstance(node, ast.NativeQuery) or isinstance(node, ast.Data):
                    mdb_entities.append(node)

        query_traversal(query, find_objects)

        # cte names are not mdb objects
        if isinstance(query, Select) and query.cte:
            cte_names = [cte.name.parts[-1] for cte in query.cte]
            mdb_entities = [item for item in mdb_entities if ".".join(item.parts) not in cte_names]

        return {
            "mdb_entities": mdb_entities,
            "integrations": integrations,
            "predictors": predictors,
            "user_functions": user_functions,
        }

    def get_nested_selects_plan_fnc(self, main_integration, force=False):
        # returns function for traversal over query and inject fetch data query instead of subselects
        def find_selects(node, **kwargs):
            if isinstance(node, Select):
                query_info2 = self.get_query_info(node)
                if force or (
                    len(query_info2["integrations"]) > 1
                    or main_integration not in query_info2["integrations"]
                    or len(query_info2["mdb_entities"]) > 0
                ):
                    # need to execute in planner

                    node.parentheses = False
                    last_step = self.plan_select(node)

                    node2 = Parameter(last_step.result)

                    return node2

        return find_selects

    def plan_select_identifier(self, query):
        # query_info = self.get_query_info(query)
        #
        # if len(query_info['integrations']) == 0 and len(query_info['predictors']) >= 1:
        #     # select from predictor
        #     return self.plan_select_from_predictor(query)
        # elif (
        #     len(query_info['integrations']) == 1
        #     and len(query_info['mdb_entities']) == 0
        #     and len(query_info['user_functions']) == 0
        # ):
        #
        #     int_name = list(query_info['integrations'])[0]
        #     if self.integrations.get(int_name, {}).get('class_type') != 'api':
        #         # one integration without predictors, send all query to integration
        #         return self.plan_integration_select(query)

        # find subselects
        main_integration, _ = self.resolve_database_table(query.from_table)
        is_api_db = self.integrations.get(main_integration, {}).get("class_type") == "api"

        find_selects = self.get_nested_selects_plan_fnc(main_integration, force=is_api_db)
        query.targets = query_traversal(query.targets, find_selects)
        query_traversal(query.where, find_selects)

        # get info of updated query
        query_info = self.get_query_info(query)

        if len(query_info["predictors"]) >= 1:
            # select from predictor
            return self.plan_select_from_predictor(query)
        elif is_api_db:
            return self.plan_api_db_select(query)
        elif len(query_info["user_functions"]) > 0:
            return self.plan_integration_select_with_functions(query)
        else:
            # fallback to integration
            return self.plan_integration_select(query)

    def plan_integration_select_with_functions(self, query):
        # UDF can't be aggregate function: it means we have to do aggregation after function execution
        # - remove targets from query
        # - add subselect with targets

        # replace functions in conditions

        query2 = query.copy()

        skipped_conditions = []

        def replace_functions(node, **kwargs):
            if not isinstance(node, BinaryOperation):
                return

            arg1, arg2 = node.args
            if not isinstance(arg1, Function):
                arg1 = arg2
            if not isinstance(arg1, Function):
                return

            # user defined
            if arg1.namespace is not None:
                # clear
                skipped_conditions.append(node)
                node.args = [Constant(0), Constant(0)]
                node.op = "="

        query_traversal(query2.where, replace_functions)

        query2.targets = [Star()]

        # don't do aggregate
        query2.having = None

        if query.group_by is not None:
            # if aggregation exists, do order and limit in subquery
            query2.group_by = None
            query2.order_by = None
            query2.limit = None
        else:
            query.order_by = None
            query.limit = None

        # if all conditions were executed - clear it
        if len(skipped_conditions) == 0:
            query.where = None

        prev_step = self.plan_integration_select(query2)

        return self.plan_sub_select(query, prev_step)

    def plan_api_db_select(self, query):
        # split to select from api database
        #     keep only limit and where
        #     the rest goes to outer select
        query2 = Select(
            targets=query.targets,
            from_table=query.from_table,
            where=query.where,
            order_by=query.order_by,
            limit=query.limit,
        )
        prev_step = self.plan_integration_select(query2)

        # clear limit and where
        query.limit = None
        query.where = None
        return self.plan_sub_select(query, prev_step)

    def plan_nested_select(self, select):
        # query_info = self.get_query_info(select)
        # # get all predictors
        #
        # if (
        #     len(query_info['mdb_entities']) == 0
        #     and len(query_info['integrations']) == 1
        #     and len(query_info['user_functions']) == 0
        #     and 'files' not in query_info['integrations']
        #     and 'views' not in query_info['integrations']
        # ):
        #     int_name = list(query_info['integrations'])[0]
        #     if self.integrations.get(int_name, {}).get('class_type') != 'api':
        #
        #         # if no predictor inside = run as is
        #         return self.plan_integration_nested_select(select, int_name)

        return self.plan_mdb_nested_select(select)

    def plan_mdb_nested_select(self, select):
        # plan nested select

        select2 = copy.deepcopy(select.from_table)
        select2.parentheses = False
        select2.alias = None
        self.plan_select(select2)
        last_step = self.plan.steps[-1]

        return self.plan_sub_select(select, last_step)

    def get_predictor_namespace_and_name_from_identifier(self, identifier):
        new_identifier = copy.deepcopy(identifier)

        info = self.get_predictor(identifier)
        namespace = info["integration_name"]

        parts = [namespace, info["name"]]
        if info["version"] is not None:
            parts.append(info["version"])
        new_identifier.parts = parts

        return namespace, new_identifier

    def plan_select_from_predictor(self, select):
        predictor_namespace, predictor = self.get_predictor_namespace_and_name_from_identifier(select.from_table)

        if select.where == BinaryOperation("=", args=[Constant(1), Constant(0)]):
            # Hardcoded mysql way of getting predictor columns
            predictor_identifier = utils.get_predictor_name_identifier(predictor)
            predictor_step = self.plan.add_step(
                GetPredictorColumns(namespace=predictor_namespace, predictor=predictor_identifier)
            )
        else:
            new_query_targets = []
            for target in select.targets:
                if isinstance(target, Identifier):
                    new_query_targets.append(disambiguate_predictor_column_identifier(target, predictor))
                elif type(target) in (Star, Constant, Function):
                    new_query_targets.append(target)
                else:
                    raise PlanningException(f"Unknown select target {type(target)}")

            if select.group_by or select.having:
                raise PlanningException(
                    "Unsupported operation when querying predictor. Only WHERE is allowed and required."
                )

            row_dict = {}
            where_clause = select.where
            if not where_clause:
                raise PlanningException("WHERE clause required when selecting from predictor")

            predictor_identifier = utils.get_predictor_name_identifier(predictor)
            recursively_extract_column_values(where_clause, row_dict, predictor_identifier)

            params = None
            if select.using is not None:
                params = select.using
            predictor_step = self.plan.add_step(
                ApplyPredictorRowStep(
                    namespace=predictor_namespace, predictor=predictor_identifier, row_dict=row_dict, params=params
                )
            )
        project_step = self.plan_project(select, predictor_step.result)
        return project_step

    def plan_predictor(self, query, table, predictor_namespace, predictor):
        int_select = copy.deepcopy(query)
        int_select.targets = [Star()]  # TODO why not query.targets?
        int_select.from_table = table

        predictor_alias = None
        if predictor.alias is not None:
            predictor_alias = predictor.alias.parts[0]

        params = {}
        if query.using is not None:
            params = query.using

        binary_ops = []
        table_filters = []
        model_filters = []

        def split_filters(node, **kwargs):
            # split conditions between model and table

            if isinstance(node, BinaryOperation):
                op = node.op.lower()

                binary_ops.append(op)

                if op in ["and", "or"]:
                    return

                arg1, arg2 = node.args
                if not isinstance(arg1, Identifier):
                    arg1, arg2 = arg2, arg1

                if isinstance(arg1, Identifier) and isinstance(arg2, (Constant, Parameter)) and len(arg1.parts) > 1:
                    model = Identifier(parts=arg1.parts[:-1])

                    if self.is_predictor(model) or (len(model.parts) == 1 and model.parts[0] == predictor_alias):
                        model_filters.append(node)
                        return
                table_filters.append(node)

        # find subselects
        main_integration, _ = self.resolve_database_table(table)
        find_selects = self.get_nested_selects_plan_fnc(main_integration, force=True)
        query_traversal(int_select.where, find_selects)

        # split conditions
        query_traversal(int_select.where, split_filters)

        if len(model_filters) > 0 and "or" not in binary_ops:
            int_select.where = filters_to_bin_op(table_filters)

        integration_select_step = self.plan_integration_select(int_select)

        predictor_identifier = utils.get_predictor_name_identifier(predictor)

        if len(params) == 0:
            params = None

        row_dict = None
        if model_filters:
            row_dict = {}
            for el in model_filters:
                if isinstance(el.args[0], Identifier) and el.op == "=":
                    if isinstance(el.args[1], (Constant, Parameter)):
                        row_dict[el.args[0].parts[-1]] = el.args[1].value

        last_step = self.plan.add_step(
            ApplyPredictorStep(
                namespace=predictor_namespace,
                dataframe=integration_select_step.result,
                predictor=predictor_identifier,
                params=params,
                row_dict=row_dict,
            )
        )

        return {
            "predictor": last_step,
            "data": integration_select_step,
        }

    # def plan_group(self, query, last_step):
    #     # ! is not using yet
    #
    #     # check group
    #     funcs = []
    #     for t in query.targets:
    #         if isinstance(t, Function):
    #             funcs.append(t.op.lower())
    #     agg_funcs = ['sum', 'min', 'max', 'avg', 'count', 'std']
    #
    #     if (
    #             query.having is not None
    #             or query.group_by is not None
    #             or set(agg_funcs) & set(funcs)
    #     ):
    #         # is aggregate
    #         group_by_targets = []
    #         for t in query.targets:
    #             target_copy = copy.deepcopy(t)
    #             group_by_targets.append(target_copy)
    #         # last_step = self.plan.steps[-1]
    #         return GroupByStep(dataframe=last_step.result, columns=query.group_by, targets=group_by_targets)

    def plan_project(self, query, dataframe, ignore_doubles=False):
        out_identifiers = []

        if len(query.targets) == 1 and isinstance(query.targets[0], Star):
            last_step = self.plan.steps[-1]
            return last_step

        for target in query.targets:
            if (
                isinstance(target, Identifier)
                or isinstance(target, Star)
                or isinstance(target, Function)
                or isinstance(target, Constant)
                or isinstance(target, BinaryOperation)
            ):
                out_identifiers.append(target)
            else:
                new_identifier = Identifier(str(target.to_string(alias=False)), alias=target.alias)
                out_identifiers.append(new_identifier)
        return self.plan.add_step(
            ProjectStep(dataframe=dataframe, columns=out_identifiers, ignore_doubles=ignore_doubles)
        )

    def plan_create_table(self, query: CreateTable):
        if query.from_select is None:
            if query.columns is not None:
                self.plan.add_step(
                    CreateTableStep(
                        table=query.name,
                        columns=query.columns,
                        is_replace=query.is_replace,
                    )
                )
                return

            raise PlanningException(f'Not implemented "create table": {query.to_string()}')

        integration_name = query.name.parts[0]

        last_step = self.plan_select(query.from_select, integration=integration_name)

        # create table step
        self.plan.add_step(
            SaveToTable(
                table=query.name,
                dataframe=last_step.result,
                is_replace=query.is_replace,
            )
        )

    def plan_insert(self, query):
        table = query.table
        if query.from_select is not None:
            integration_name = query.table.parts[0]

            # plan sub-select first
            last_step = self.plan_select(query.from_select, integration=integration_name)

            # possible knowledge base parameters
            select = query.from_select
            params = {}
            if isinstance(select, Select) and select.using is not None:
                for k, v in select.using.items():
                    if k.startswith("kb_"):
                        params[k] = v

            self.plan.add_step(
                InsertToTable(
                    table=table,
                    dataframe=last_step.result,
                    params=params,
                )
            )
        else:
            self.plan.add_step(
                InsertToTable(
                    table=table,
                    query=query,
                )
            )

    def plan_update(self, query):
        last_step = None
        if query.from_select is not None:
            integration_name = query.table.parts[0]
            last_step = self.plan_select(query.from_select, integration=integration_name)

        # plan sub-select first
        update_command = copy.deepcopy(query)
        # clear subselect
        update_command.from_select = None

        table = query.table
        self.plan.add_step(UpdateToTable(table=table, dataframe=last_step, update_command=update_command))

    def plan_delete(self, query: Delete):
        # find subselects
        main_integration, _ = self.resolve_database_table(query.table)

        is_api_db = self.integrations.get(main_integration, {}).get("class_type") == "api"

        find_selects = self.get_nested_selects_plan_fnc(main_integration, force=is_api_db)
        query_traversal(query.where, find_selects)

        self.prepare_integration_select(main_integration, query.where)

        return self.plan.add_step(DeleteStep(table=query.table, where=query.where))

    def plan_cte(self, query):
        for cte in query.cte:
            step = self.plan_select(cte.query)
            name = cte.name.parts[-1]
            self.cte_results[name] = step.result

    def check_single_integration(self, query):
        query_info = self.get_query_info(query)

        # can we send all query to integration?

        # one integration and not mindsdb objects in query
        if (
            len(query_info["mdb_entities"]) == 0
            and len(query_info["integrations"]) == 1
            and "files" not in query_info["integrations"]
            and "views" not in query_info["integrations"]
            and len(query_info["user_functions"]) == 0
        ):
            int_name = list(query_info["integrations"])[0]
            # if is sql database
            if self.integrations.get(int_name, {}).get("class_type") != "api":
                # send to this integration
                self.prepare_integration_select(int_name, query)

                last_step = self.plan.add_step(FetchDataframeStep(integration=int_name, query=query))
                return last_step

    def plan_select(self, query, integration=None):
        if isinstance(query, (Union, Except, Intersect)):
            return self.plan_union(query, integration=integration)

        if query.cte is not None:
            self.plan_cte(query)

        from_table = query.from_table

        if isinstance(from_table, Identifier):
            return self.plan_select_identifier(query)
        elif isinstance(from_table, Select):
            return self.plan_nested_select(query)
        elif isinstance(from_table, Join):
            plan_join = PlanJoin(self)
            return plan_join.plan(query, integration)
        elif isinstance(from_table, NativeQuery):
            integration = from_table.integration.parts[0].lower()
            step = FetchDataframeStep(integration=integration, raw_query=from_table.query)
            last_step = self.plan.add_step(step)
            return self.plan_sub_select(query, last_step)

        elif isinstance(from_table, ast.Data):
            step = DataStep(from_table.data)
            last_step = self.plan.add_step(step)
            return self.plan_sub_select(query, last_step, add_absent_cols=True)
        elif from_table is None:
            # one line select
            step = QueryStep(query, from_table=pd.DataFrame([None]))
            return self.plan.add_step(step)
        else:
            raise PlanningException(f"Unsupported from_table {type(from_table)}")

    def plan_sub_select(self, query, prev_step, add_absent_cols=False):
        if (
            query.group_by is not None
            or query.order_by is not None
            or query.having is not None
            or query.distinct is True
            or query.where is not None
            or query.limit is not None
            or query.offset is not None
            or len(query.targets) != 1
            or not isinstance(query.targets[0], Star)
        ):
            if query.from_table.alias is not None:
                table_name = query.from_table.alias.parts[-1]
            elif isinstance(query.from_table, Identifier):
                table_name = query.from_table.parts[-1]
            else:
                table_name = None

            query2 = copy.deepcopy(query)
            query2.from_table = None
            sup_select = SubSelectStep(query2, prev_step.result, table_name=table_name, add_absent_cols=add_absent_cols)
            self.plan.add_step(sup_select)
            return sup_select
        return prev_step

    def plan_union(self, query, integration=None):
        step1 = self.plan_select(query.left, integration=integration)
        step2 = self.plan_select(query.right, integration=integration)
        operation = "union"
        if isinstance(query, Except):
            operation = "except"
        elif isinstance(query, Intersect):
            operation = "intersect"

        return self.plan.add_step(
            UnionStep(left=step1.result, right=step2.result, unique=query.unique, operation=operation)
        )

    # method for compatibility
    def from_query(self, query=None):
        self.plan = QueryPlan()

        if query is None:
            query = self.query

        if isinstance(query, (Select, Union, Except, Intersect)):
            if self.check_single_integration(query):
                return self.plan
            self.plan_select(query)
        elif isinstance(query, CreateTable):
            self.plan_create_table(query)
        elif isinstance(query, Insert):
            self.plan_insert(query)
        elif isinstance(query, Update):
            self.plan_update(query)
        elif isinstance(query, Delete):
            self.plan_delete(query)
        else:
            raise PlanningException(f"Unsupported query type {type(query)}")

        plan = self.handle_partitioning(self.plan)

        return plan

    def handle_partitioning(self, plan: QueryPlan) -> QueryPlan:
        """
        If plan has fetching in partitions:
          try to rebuild plan to send fetched chunk of data through the following steps, if it is possible
        """

        # handle fetchdataframe partitioning
        steps_in = plan.steps
        steps_out = []

        step = None
        partition_step = None
        for step in steps_in:
            if isinstance(step, FetchDataframeStep) and step.params is not None:
                batch_size = step.params.get("batch_size")
                if batch_size is not None:
                    # found batched fetch
                    partition_step = FetchDataframeStepPartition(
                        step_num=step.step_num,
                        integration=step.integration,
                        query=step.query,
                        raw_query=step.raw_query,
                        params=step.params,
                    )
                    steps_out.append(partition_step)
                    # mark plan
                    plan.is_resumable = True
                    continue
                else:
                    step.params = None

            if partition_step is not None:
                # check and add step into partition

                can_be_partitioned = False
                if isinstance(step, (JoinStep, ApplyPredictorStep, InsertToTable)):
                    can_be_partitioned = True
                elif isinstance(step, QueryStep):
                    query = step.query
                    if (
                        query.group_by is None
                        and query.order_by is None
                        and query.distinct is False
                        and query.limit is None
                        and query.offset is None
                    ):
                        no_identifiers = [
                            target for target in step.query.targets if not isinstance(target, (Star, Identifier))
                        ]
                        if len(no_identifiers) == 0:
                            can_be_partitioned = True

                if not can_be_partitioned:
                    if len(partition_step.steps) == 0:
                        # Nothing can be partitioned, failback to old plan
                        plan.is_resumable = False
                        return plan
                    partition_step = None
                else:
                    partition_step.steps.append(step)
                    continue

            steps_out.append(step)

        if plan.is_resumable and isinstance(step, InsertToTable):
            plan.is_async = True
        else:
            # special case: register insert from select (it is the same as mark resumable)
            if (
                len(steps_in) == 2
                and isinstance(steps_in[0], FetchDataframeStep)
                and isinstance(steps_in[1], InsertToTable)
            ):
                plan.is_resumable = True

        plan.steps = steps_out
        return plan

    def prepare_steps(self, query):
        statement_planner = PreparedStatementPlanner(self)

        # return generator
        return statement_planner.prepare_steps(query)

    def execute_steps(self, params=None):
        statement_planner = PreparedStatementPlanner(self)

        # return generator
        return statement_planner.execute_steps(params)

    # def fetch(self, row_count):
    #     statement_planner = PreparedStatementPlanner(self)
    #     return statement_planner.fetch(row_count)
    #
    # def close(self):
    #     statement_planner = PreparedStatementPlanner(self)
    #     return statement_planner.close()

    def get_statement_info(self):
        statement_planner = PreparedStatementPlanner(self)

        return statement_planner.get_statement_info()
