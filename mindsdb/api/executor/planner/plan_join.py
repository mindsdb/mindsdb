import copy
from dataclasses import dataclass, field

from mindsdb_sql_parser import ast
from mindsdb_sql_parser.ast import (
    Select,
    Identifier,
    BetweenOperation,
    Join,
    Star,
    BinaryOperation,
    Constant,
    NativeQuery,
    Parameter,
    Function,
    Last,
)

from mindsdb.integrations.utilities.query_traversal import query_traversal

from mindsdb.api.executor.planner.exceptions import PlanningException
from mindsdb.api.executor.planner.steps import (
    FetchDataframeStep,
    JoinStep,
    ApplyPredictorStep,
    SubSelectStep,
    QueryStep,
    MapReduceStep,
)
from mindsdb.api.executor.planner.utils import filters_to_bin_op
from mindsdb.api.executor.planner.plan_join_ts import PlanJoinTSPredictorQuery


@dataclass
class TableInfo:
    integration: str
    table: Identifier
    aliases: list[tuple[str, ...]] = field(default_factory=list)
    conditions: list = None
    sub_select: ast.ASTNode = None
    predictor_info: dict = None
    join_condition = None
    index: int = None


class PlanJoin:
    def __init__(self, planner):
        self.planner = planner

    def is_timeseries(self, query):
        join = query.from_table
        l_predictor = self.planner.get_predictor(join.left) if isinstance(join.left, Identifier) else None
        r_predictor = self.planner.get_predictor(join.right) if isinstance(join.right, Identifier) else None
        if l_predictor and l_predictor.get("timeseries"):
            return True
        if r_predictor and r_predictor.get("timeseries"):
            return True

    def check_single_integration(self, query):
        query_info = self.planner.get_query_info(query)
        # can we send all query to integration?
        # one integration and not mindsdb objects in query
        if (
            len(query_info["mdb_entities"]) == 0
            and len(query_info["integrations"]) == 1
            and "files" not in query_info["integrations"]
            and "views" not in query_info["integrations"]
        ):
            int_name = list(query_info["integrations"])[0]
            # if is sql database
            class_type = self.planner.integrations.get(int_name, {}).get("class_type")
            if class_type != "api":
                # send to this integration
                return int_name
        return None

    def plan(self, query, integration=None):
        # FIXME: Tableau workaround, INFORMATION_SCHEMA with Where
        # if isinstance(join.right, Identifier) \
        #         and self.resolve_database_table(join.right)[0] == 'INFORMATION_SCHEMA':
        #     pass

        # send join to integration as is?
        integration_to_send = self.check_single_integration(query)
        if integration_to_send:
            self.planner.prepare_integration_select(integration_to_send, query)

            fetch_params = self.planner.get_fetch_params(query.using)
            last_step = self.planner.plan.add_step(
                FetchDataframeStep(integration=integration_to_send, query=query, params=fetch_params)
            )
            return last_step
        elif self.is_timeseries(query):
            return PlanJoinTSPredictorQuery(self.planner).plan(query, integration)
        else:
            return PlanJoinTablesQuery(self.planner).plan(query)


class PlanJoinTablesQuery:
    def __init__(self, planner):
        self.planner = planner

        # index to lookup tables
        self.tables_idx = None
        self.tables = []
        self.tables_fetch_step = {}

        self.step_stack = None
        self.query_context = {}

        self.partition = None

    def plan(self, query):
        self.tables_idx = {}
        join_step = self.plan_join_tables(query)

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
            query2 = copy.deepcopy(query)
            query2.from_table = None
            query2.using = None
            query2.cte = None
            sup_select = QueryStep(query2, from_table=join_step.result, strict_where=False)
            self.planner.plan.add_step(sup_select)
            return sup_select
        return join_step

    def resolve_table(self, table):
        # gets integration for table and name to access to it
        table = copy.deepcopy(table)
        # get possible table aliases
        aliases = []
        if table.alias is not None:
            # to lowercase
            parts = tuple(map(str.lower, table.alias.parts))
            aliases.append(parts)
        else:
            for i in range(0, len(table.parts)):
                parts = table.parts[i:]
                parts = tuple(map(str.lower, parts))
                aliases.append(parts)

        # try to use default namespace
        integration = self.planner.default_namespace
        if len(table.parts) > 0:
            # if not quoted  check in lower case
            part = table.parts[0]
            if part not in self.planner.databases and not table.is_quoted[0]:
                part = part.lower()

            if part in self.planner.databases:
                integration = part
                table.parts.pop(0)
                table.is_quoted.pop(0)
            else:
                integration = self.planner.default_namespace

        if integration is None and not hasattr(table, "sub_select"):
            raise PlanningException(f"Database not found for: {table}")

        sub_select = getattr(table, "sub_select", None)

        return TableInfo(integration, table, aliases, conditions=[], sub_select=sub_select)

    def get_table_for_column(self, column: Identifier):
        if not isinstance(column, Identifier):
            return
        # to lowercase
        parts = tuple(map(str.lower, column.parts[:-1]))
        if parts in self.tables_idx:
            return self.tables_idx[parts]

    def get_join_sequence(self, node, condition=None):
        sequence = []
        if isinstance(node, Identifier):
            # resolve identifier

            table_info = self.resolve_table(node)
            for alias in table_info.aliases:
                self.tables_idx[alias] = table_info

            table_info.index = len(self.tables)
            self.tables.append(table_info)

            table_info.predictor_info = self.planner.get_predictor(node)

            if condition is not None:
                table_info.join_condition = condition
            sequence.append(table_info)

        elif isinstance(node, Join):
            # create sequence: 1)table1, 2)table2, 3)join 1 2, 4)table 3, 5)join 3 4

            # put all tables before
            sequence2 = self.get_join_sequence(node.left)
            for item in sequence2:
                sequence.append(item)

            sequence2 = self.get_join_sequence(node.right, condition=node.condition)
            if len(sequence2) != 1:
                raise PlanningException("Unexpected join nesting behavior")

            # put next table
            sequence.append(sequence2[0])

            # put join
            sequence.append(node)

        else:
            raise NotImplementedError()
        return sequence

    def check_node_condition(self, node):
        col_idx = 0
        if len(node.args) == 2:
            if not isinstance(node.args[col_idx], Identifier):
                # try to use second arg, could be: 'x'=col
                col_idx = 1

        # check the case col <condition> value, col between value and value
        for i, arg in enumerate(node.args):
            if i == col_idx:
                if not isinstance(arg, Identifier):
                    return
            else:
                if not self.can_be_table_filter(arg):
                    return

        # checked, find table and store condition
        node2 = copy.deepcopy(node)

        arg1 = node2.args[col_idx]

        if len(arg1.parts) < 2:
            return

        table_info = self.get_table_for_column(arg1)
        if table_info is None:
            raise PlanningException(f"Table not found for identifier: {arg1.to_string()}")

        # keep only column name
        arg1.parts = [arg1.parts[-1]]

        node2._orig_node = node
        table_info.conditions.append(node2)

    def can_be_table_filter(self, node):
        """
        Check if node can be used as a filter.
        It can contain only: Constant, Parameter, Function (with Last)
        """
        if isinstance(node, (Constant, Parameter)):
            return True
        if isinstance(node, Function):
            # `Last` must be in args
            if not any(isinstance(arg, Last) for arg in node.args):
                return False
            return all([self.can_be_table_filter(arg) for arg in node.args])

    def check_query_conditions(self, query):
        # get conditions for tables
        binary_ops = []

        def _check_node_condition(node, **kwargs):
            if isinstance(node, BetweenOperation):
                self.check_node_condition(node)

            if isinstance(node, BinaryOperation):
                binary_ops.append(node.op)

                self.check_node_condition(node)

        query_traversal(query.where, _check_node_condition)

        self.query_context["binary_ops"] = binary_ops

    def check_use_limit(self, query_in, join_sequence):
        # if only models (predictors), not for regular table joins
        use_limit = False
        if query_in.having is None and query_in.group_by is None and query_in.limit is not None:
            use_limit = True
            has_join = False
            regular_table_count = 0

            for item in join_sequence:
                if isinstance(item, TableInfo):
                    # Check if it's a regular table (not a predictor, not a subselect)
                    if item.predictor_info is None and item.sub_select is None:
                        regular_table_count += 1
                elif isinstance(item, Join):
                    # LEFT JOIN preserves left table row count - LIMIT pushdown is safe
                    join_type = str(item.join_type).upper() if item.join_type else ""
                    if join_type not in ("LEFT JOIN", "LEFT OUTER JOIN"):
                        has_join = True

            # Disable limit pushdown only if joining MULTIPLE regular database tables
            # Allow it for: single table, or table + predictor (predictor generates on-demand)
            if has_join and regular_table_count > 1:
                use_limit = False

        self.query_context["use_limit"] = use_limit

    def plan_join_tables(self, query_in):
        # plan all nested selects in 'where'
        find_selects = self.planner.get_nested_selects_plan_fnc(self.planner.default_namespace, force=True)
        query_in.targets = query_traversal(query_in.targets, find_selects)
        query_traversal(query_in.where, find_selects)

        query = copy.deepcopy(query_in)

        # replace sub selects, with identifiers with links to original selects
        def replace_subselects(node, **args):
            if isinstance(node, Select) or isinstance(node, NativeQuery) or isinstance(node, ast.Data):
                name = f"t_{id(node)}"
                node2 = Identifier(name, alias=node.alias)

                # save in attribute
                if isinstance(node, NativeQuery) or isinstance(node, ast.Data):
                    # wrap to select
                    node = Select(targets=[Star()], from_table=node)
                node2.sub_select = node
                return node2

        query_traversal(query.from_table, replace_subselects)

        # get all join tables, form join sequence
        join_sequence = self.get_join_sequence(query.from_table)
        self.join_sequence = join_sequence

        # find tables for identifiers used in query
        def _check_identifiers(node, is_table, **kwargs):
            if not is_table and isinstance(node, Identifier):
                if len(node.parts) > 1:
                    table_info = self.get_table_for_column(node)
                    if table_info is None:
                        raise PlanningException(f"Table not found for identifier: {node.to_string()}")

                    # # replace identifies name
                    col_parts = list(table_info.aliases[-1])
                    col_parts.append(node.parts[-1])
                    node.parts = col_parts

        query_traversal(query, _check_identifiers)

        self.check_query_conditions(query)

        # workaround for 'model join table': swap tables:
        if len(join_sequence) == 3 and join_sequence[0].predictor_info is not None:
            join_sequence = [join_sequence[1], join_sequence[0], join_sequence[2]]

        self.check_use_limit(query_in, join_sequence)

        # create plan
        # TODO add optimization: one integration without predictor

        self.step_stack = []
        for item in join_sequence:
            if isinstance(item, TableInfo):
                if item.sub_select is not None:
                    self.process_subselect(item, query_in)
                elif item.predictor_info is not None:
                    self.process_predictor(item, query_in)
                else:
                    # is table
                    self.process_table(item, query_in)

            elif isinstance(item, Join):
                step_right = self.step_stack.pop()
                step_left = self.step_stack.pop()

                new_join = copy.deepcopy(item)

                # TODO
                new_join.left = Identifier("tab1")
                new_join.right = Identifier("tab2")
                new_join.implicit = False

                step = self.add_plan_step(JoinStep(left=step_left.result, right=step_right.result, query=new_join))

                self.step_stack.append(step)

        query_in.where = query.where

        self.close_partition()
        return self.step_stack.pop()

    def process_subselect(self, item, query_in):
        # is sub select
        item.sub_select.alias = None
        item.sub_select.parentheses = False
        step = self.planner.plan_select(item.sub_select)

        where = filters_to_bin_op(item.conditions)

        # Column pruning for subselects:
        # - If subselect has pure SELECT *, we can prune to only needed columns
        # - If subselect has explicit columns (SELECT a, b, c), pass through all (don't prune)
        # This preserves column aliases and prevents breaking explicit projections
        targets = [Star()]
        if self._can_prune_columns(item):
            needed_columns = self.get_columns_for_table(item, query_in, self.join_sequence)
            if needed_columns:
                targets = needed_columns

        # apply table alias
        query2 = Select(targets=targets, where=where)
        if item.table.alias is None:
            raise PlanningException(f"Subselect in join have to be aliased: {item.sub_select.to_string()}")
        table_name = item.table.alias.parts[-1]

        add_absent_cols = False
        if hasattr(item.sub_select, "from_table") and isinstance(item.sub_select.from_table, ast.Data):
            add_absent_cols = True

        step2 = SubSelectStep(query2, step.result, table_name=table_name, add_absent_cols=add_absent_cols)
        step2 = self.add_plan_step(step2)
        self.step_stack.append(step2)

    def _collect_from_order_by(self, query_in, alias_map, add_column_callback):
        """Helper to collect columns from ORDER BY clause, resolving aliases and ordinals."""
        for order_col in query_in.order_by:
            field = order_col.field

            # Handle ORDER BY ordinal (e.g., ORDER BY 1)
            if isinstance(field, Constant) and isinstance(field.value, int):
                ordinal = field.value
                if 1 <= ordinal <= len(query_in.targets):
                    target_expr = query_in.targets[ordinal - 1]
                    query_traversal(target_expr, add_column_callback)
                continue

            # Handle ORDER BY alias (e.g., ORDER BY alias_name)
            if isinstance(field, Identifier) and len(field.parts) == 1:
                alias_name = field.parts[0].lower()
                if alias_name in alias_map:
                    query_traversal(alias_map[alias_name], add_column_callback)
                    continue

            # Regular column reference
            query_traversal(field, add_column_callback)

    def _join_has_predictor(self, join_sequence) -> bool:
        """Check if the join sequence contains any predictor."""
        for item in join_sequence:
            if isinstance(item, TableInfo) and item.predictor_info is not None:
                return True
        return False

    def _can_prune_columns(self, table_info) -> bool:
        """
        Determine if column pruning can be applied to this table.

        Returns:
            True if column pruning can be applied
            False if we should skip pruning (use SELECT *)
        """
        # Predictors/models: cannot prune (need all input features)
        if table_info.predictor_info is not None:
            return False

        # If this table is part of a join with a predictor: cannot prune
        # Predictors may need all columns from joined tables as input features
        if hasattr(self, "join_sequence") and self._join_has_predictor(self.join_sequence):
            return False

        # For subselects: can only prune if they have pure SELECT * (no other columns)
        sub = table_info.sub_select
        if sub is not None and isinstance(sub, Select):
            targets = getattr(sub, "targets", None) or []
            # Can prune only if subselect has PURE SELECT * (one target that is Star)
            # Cannot prune if:
            #   - Mixed: SELECT *, col1 (has Star but also other columns)
            if len(targets) == 1 and isinstance(targets[0], Star):
                return True  # Pure SELECT * - can prune
            return False

        # For project tables (KB tables, views, etc.): cannot prune
        # Project tables need SELECT * for proper column mapping
        if table_info.integration and table_info.integration in self.planner.projects:
            return False

        # Regular integration tables: can prune
        return True

    def get_columns_for_table(self, table_info, query_in, join_sequence):
        """
        Collect all columns needed from a specific table for column pruning optimization.

        Note: Caller should check _can_prune_columns() before calling this method.

        Returns a list of column Identifiers or None if we should fetch all columns.
        """
        columns = {}
        has_qualified_star_for_table = False

        alias_map = {}
        if query_in.targets:
            for target in query_in.targets:
                if isinstance(target, Identifier) and target.alias:
                    alias_map[target.alias.parts[-1].lower()] = target

        def add_column(node, **kwargs):
            if isinstance(node, Identifier):
                col_table = self.get_table_for_column(node)
                if not col_table or col_table.index != table_info.index:
                    return

                # Check for qualified star: t1.* or alias.*
                if node.parts and (node.parts[-1] == "*" or isinstance(node.parts[-1], Star)):
                    nonlocal has_qualified_star_for_table
                    has_qualified_star_for_table = True
                    return

                col_name = node.parts[-1]
                # Make sure col_name is a string, not a Star object
                if not isinstance(col_name, str):
                    return

                is_quoted = node.is_quoted[-1] if node.is_quoted and len(node.is_quoted) == len(node.parts) else False
                # Store - if already exists, keep it quoted if either reference was quoted
                if col_name in columns:
                    columns[col_name] = columns[col_name] or is_quoted
                else:
                    columns[col_name] = is_quoted
            elif isinstance(node, Function):
                # Traverse window function clauses (PARTITION BY, ORDER BY)
                if hasattr(node, "partition_by") and node.partition_by:
                    query_traversal(node.partition_by, add_column)
                if hasattr(node, "order_by") and node.order_by:
                    for order_item in node.order_by:
                        query_traversal(order_item.field if hasattr(order_item, "field") else order_item, add_column)

        # Check for bare Star() in targets
        if query_in.targets:
            for target in query_in.targets:
                if isinstance(target, Star):
                    return None

        # Collect columns from SELECT targets
        if query_in.targets:
            query_traversal(query_in.targets, add_column)

        # Collect columns from WHERE clause
        if query_in.where:
            query_traversal(query_in.where, add_column)

        # Collect columns from ORDER BY (resolve aliases and ordinals)
        if query_in.order_by:
            self._collect_from_order_by(query_in, alias_map, add_column)

        # Collect columns from GROUP BY
        if query_in.group_by:
            query_traversal(query_in.group_by, add_column)

        # Collect columns from HAVING
        if query_in.having:
            query_traversal(query_in.having, add_column)

        # Collect columns from JOIN conditions
        for seq_item in join_sequence:
            if isinstance(seq_item, TableInfo) and seq_item.join_condition:
                query_traversal(seq_item.join_condition, add_column)

        # If qualified star found for this table, fetch all columns
        if has_qualified_star_for_table:
            return None

        # If we found no columns, fetch all
        if not columns:
            return None

        # Convert column names to Identifier objects, we need to preserve quoting
        result = []
        for col, is_quoted in sorted(columns.items()):
            ident = Identifier(parts=[col])
            ident.is_quoted = [is_quoted]
            result.append(ident)
        return result

    def process_table(self, item, query_in):
        table = copy.deepcopy(item.table)
        table.parts.insert(0, item.integration)
        table.is_quoted.insert(0, False)

        if self._can_prune_columns(item):
            needed_columns = self.get_columns_for_table(item, query_in, self.join_sequence)
            targets = needed_columns if needed_columns else [Star()]
        else:
            targets = [Star()]

        query2 = Select(from_table=table, targets=targets)
        conditions = item.conditions
        if "or" in self.query_context["binary_ops"]:
            conditions = []

        # For cross-database joins, skip the IN clause optimization
        # Reason: We can't predict row counts, and building large IN clauses causes errors
        # Let the join happen in memory without filter pushdown
        # conditions += self.get_filters_from_join_conditions(item, query_in.using)

        if self.query_context["use_limit"]:
            order_by = None
            if query_in.order_by is not None:
                order_by = []
                # all order column be from this table
                for col in query_in.order_by:
                    table_info = self.get_table_for_column(col.field)
                    if table_info is None or table_info.table != item.table:
                        order_by = False
                        break
                    col = copy.deepcopy(col)
                    col.field.parts = [col.field.parts[-1]]
                    col.field.is_quoted = [col.field.is_quoted[-1]]
                    order_by.append(col)

            if order_by is not False:
                # copy limit from upper query
                query2.limit = query_in.limit
                # move offset from upper query
                query2.offset = query_in.offset
                query_in.offset = None
                # copy order
                query2.order_by = order_by

            self.query_context["use_limit"] = False
        for cond in conditions:
            if query2.where is not None:
                query2.where = BinaryOperation("and", args=[query2.where, cond])
            else:
                query2.where = cond

        step = self.planner.get_integration_select_step(query2, params=query_in.using)
        self.tables_fetch_step[item.index] = step

        self.add_plan_step(step)
        self.step_stack.append(step)

    def join_condition_to_columns_map(self, model_table):
        columns_map = {}

        def _check_conditions(node, **kwargs):
            if not isinstance(node, BinaryOperation):
                return

            arg1, arg2 = node.args
            if not (isinstance(arg1, Identifier) and isinstance(arg2, Identifier)):
                return

            table1 = self.get_table_for_column(arg1)
            table2 = self.get_table_for_column(arg2)

            if table1 is model_table:
                # model is on the left
                columns_map[arg1.parts[-1]] = arg2
            elif table2 is model_table:
                # model is on the right
                columns_map[arg2.parts[-1]] = arg1
            else:
                # not found, skip
                return

            # exclude condition
            node.args = [Constant(0), Constant(0)]

        query_traversal(model_table.join_condition, _check_conditions)
        return columns_map

    def get_filters_from_join_conditions(self, fetch_table):
        """
        Extract filters from join conditions for filter pushdown optimization.

        Note: This function is currently disabled (not called) to avoid:
        - Creating massive IN clauses that exceed database query size limits
        - Making arbitrary assumptions about data distribution

        For cross-database joins with large tables, users should:
        - Add explicit WHERE clauses to filter data at the source
        - Use indexed/partitioned tables in their databases
        - Consider materializing intermediate results
        """
        binary_ops = set()
        conditions = []
        data_conditions = []

        def _check_conditions(node, **kwargs):
            if not isinstance(node, BinaryOperation):
                return

            if node.op != "=":
                binary_ops.add(node.op.lower())
                return

            arg1, arg2 = node.args
            table1 = self.get_table_for_column(arg1) if isinstance(arg1, Identifier) else None
            table2 = self.get_table_for_column(arg2) if isinstance(arg2, Identifier) else None

            if table1 is not fetch_table:
                if table2 is not fetch_table:
                    return
                # set our table first
                table1, table2 = table2, table1
                arg1, arg2 = arg2, arg1

            if isinstance(arg2, Constant):
                conditions.append(node)
            elif table2 is not None:
                data_conditions.append([arg1, arg2])

        query_traversal(fetch_table.join_condition, _check_conditions)

        binary_ops.discard("and")
        if len(binary_ops) > 0:
            # other operations exists, skip
            return []

        for arg1, arg2 in data_conditions:
            # is fetched?
            table2 = self.get_table_for_column(arg2)
            fetch_step = self.tables_fetch_step.get(table2.index)

            if fetch_step is None:
                continue

            # extract distinct values
            # remove aliases
            arg1 = Identifier(parts=[arg1.parts[-1]])
            arg2 = Identifier(parts=[arg2.parts[-1]])

            query2 = Select(targets=[arg2], distinct=True)
            subselect_step = SubSelectStep(query2, fetch_step.result)
            subselect_step = self.add_plan_step(subselect_step)

            conditions.append(BinaryOperation(op="in", args=[arg1, Parameter(subselect_step.result)]))

        return conditions

    def process_predictor(self, item, query_in):
        if len(self.step_stack) == 0:
            raise NotImplementedError("Predictor can't be first element of join syntax")
        if item.predictor_info.get("timeseries"):
            raise NotImplementedError("TS predictor is not supported here yet")
        data_step = self.step_stack[-1]
        row_dict = None

        predict_target = item.predictor_info.get("to_predict")
        if isinstance(predict_target, list) and len(predict_target) > 0:
            predict_target = predict_target[0]
        if predict_target is not None:
            predict_target = predict_target.lower()

        columns_map = None
        if item.join_condition:
            columns_map = self.join_condition_to_columns_map(item)

        if item.conditions:
            row_dict = {}
            for i, el in enumerate(item.conditions):
                if isinstance(el.args[0], Identifier) and el.op == "=":
                    col_name = el.args[0].parts[-1]
                    if col_name.lower() == predict_target:
                        # don't add predict target to parameters
                        continue

                    if isinstance(el.args[1], (Constant, Parameter)):
                        row_dict[el.args[0].parts[-1]] = el.args[1].value

                    # exclude condition
                    el._orig_node.args = [Constant(0), Constant(0)]

        # params for model
        model_params = None
        partition_size = None
        if query_in.using is not None:
            model_params = {}
            for param, value in query_in.using.items():
                if "." in param:
                    alias = param.split(".")[0]
                    if (alias,) in item.aliases:
                        new_param = ".".join(param.split(".")[1:])
                        model_params[new_param.lower()] = value
                else:
                    model_params[param.lower()] = value

            partition_size = model_params.pop("partition_size", None)

        predictor_step = ApplyPredictorStep(
            namespace=item.integration,
            dataframe=data_step.result,
            predictor=item.table,
            params=model_params,
            row_dict=row_dict,
            columns_map=columns_map,
        )

        self.step_stack.append(self.add_plan_step(predictor_step, partition_size=partition_size))

    def add_plan_step(self, step, partition_size=None):
        """
        Adds step to plan

        If partition_size is defined: create partition
        If partition is active
            If step can be partitioned:
                Add step to partition not in plan
            Otherwise:
                Add partition to plan
                Add step to plan
        """
        if self.partition:
            if isinstance(step, (JoinStep, ApplyPredictorStep)):
                # add to partition

                self.add_step_to_partition(step)
                return step

        elif partition_size is not None:
            # create partition

            self.partition = MapReduceStep(values=step.dataframe, reduce="union", step=[], partition=partition_size)
            self.planner.plan.add_step(self.partition)

            self.add_step_to_partition(step)
            return step

        else:
            # next step can't be partitioned.
            self.close_partition()

        return self.planner.plan.add_step(step)

    def add_step_to_partition(self, step):
        step.step_num = f"{self.partition.step_num}_{len(self.partition.step)}"
        self.partition.step.append(step)

    def close_partition(self):
        # return
        # if partitions is exist - clear it and replace last stack item with it

        if self.partition:
            if len(self.step_stack) > 0:
                self.step_stack[-1] = self.partition

            self.partition = None
