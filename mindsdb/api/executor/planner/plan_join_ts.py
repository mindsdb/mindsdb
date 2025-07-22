import copy

from mindsdb_sql_parser.ast.mindsdb import Latest
from mindsdb_sql_parser.ast import (
    Select,
    Identifier,
    BetweenOperation,
    Join,
    Star,
    BinaryOperation,
    Constant,
    OrderBy,
    NullConstant,
)

from mindsdb.integrations.utilities.query_traversal import query_traversal

from mindsdb.api.executor.planner.exceptions import PlanningException
from mindsdb.api.executor.planner import utils
from mindsdb.api.executor.planner.steps import (
    JoinStep,
    LimitOffsetStep,
    MultipleSteps,
    MapReduceStep,
    ApplyTimeseriesPredictorStep,
)
from mindsdb.api.executor.planner.ts_utils import (
    validate_ts_where_condition,
    find_time_filter,
    replace_time_filter,
    find_and_remove_time_filter,
    recursively_check_join_identifiers_for_ambiguity,
)


class PlanJoinTSPredictorQuery:
    def __init__(self, planner):
        self.planner = planner

    def adapt_dbt_query(self, query, integration):
        orig_query = query

        join = query.from_table
        join_left = join.left

        # dbt query.

        # move latest into subquery
        moved_conditions = []

        def move_latest(node, **kwargs):
            if isinstance(node, BinaryOperation):
                if Latest() in node.args:
                    for arg in node.args:
                        if isinstance(arg, Identifier):
                            # remove table alias
                            arg.parts = [arg.parts[-1]]
                    moved_conditions.append(node)

        query_traversal(query.where, move_latest)

        # TODO make project step from query.target

        # TODO support complex query. Only one table is supported at the moment.
        # if not isinstance(join_left.from_table, Identifier):
        #     raise PlanningException(f'Statement not supported: {query.to_string()}')

        # move properties to upper query
        query = join_left

        if query.from_table.alias is not None:
            table_alias = [query.from_table.alias.parts[0]]
        else:
            table_alias = query.from_table.parts

        # add latest to query.where
        for cond in moved_conditions:
            if query.where is not None:
                query.where = BinaryOperation("and", args=[query.where, cond])
            else:
                query.where = cond

        def add_aliases(node, is_table, **kwargs):
            if not is_table and isinstance(node, Identifier):
                if len(node.parts) == 1:
                    # add table alias to field
                    node.parts = table_alias + node.parts
                    node.is_quoted = [False] + node.is_quoted

        query_traversal(query.where, add_aliases)

        if isinstance(query.from_table, Identifier):
            # DBT workaround: allow use tables without integration.
            #   if table.part[0] not in integration - take integration name from create table command
            if integration is not None and query.from_table.parts[0] not in self.planner.databases:
                # add integration name to table
                query.from_table.parts.insert(0, integration)
                query.from_table.is_quoted.insert(0, False)

        join_left = join_left.from_table

        if orig_query.limit is not None:
            if query.limit is None or query.limit.value > orig_query.limit.value:
                query.limit = orig_query.limit
        query.parentheses = False
        query.alias = None

        return query, join_left

    def get_aliased_fields(self, targets):
        # get aliases from select target
        aliased_fields = {}
        for target in targets:
            if target.alias is not None:
                aliased_fields[target.alias.to_string()] = target
        return aliased_fields

    def plan_fetch_timeseries_partitions(self, query, table, predictor_group_by_names):
        targets = [Identifier(column) for column in predictor_group_by_names]

        query = Select(
            distinct=True,
            targets=targets,
            from_table=table,
            where=query.where,
            modifiers=query.modifiers,
        )
        select_step = self.planner.plan_integration_select(query)
        return select_step

    def plan(self, query, integration=None):
        # integration is for dbt only

        join = query.from_table
        join_left = join.left
        join_right = join.right

        predictor_is_left = False
        if self.planner.is_predictor(join_left):
            # predictor is in the left, put it in the right
            join_left, join_right = join_right, join_left
            predictor_is_left = True

        if self.planner.is_predictor(join_left):
            # in the left is also predictor
            raise PlanningException(f"Can't join two predictors {join_left} and {join_left}")

        orig_query = query
        # dbt query?
        if isinstance(join_left, Select) and isinstance(join_left.from_table, Identifier):
            query, join_left = self.adapt_dbt_query(query, integration)

        predictor_namespace, predictor = self.planner.get_predictor_namespace_and_name_from_identifier(join_right)
        table = join_left

        aliased_fields = self.get_aliased_fields(query.targets)

        recursively_check_join_identifiers_for_ambiguity(query.where)
        recursively_check_join_identifiers_for_ambiguity(query.group_by, aliased_fields=aliased_fields)
        recursively_check_join_identifiers_for_ambiguity(query.having)
        recursively_check_join_identifiers_for_ambiguity(query.order_by, aliased_fields=aliased_fields)

        predictor_steps = self.plan_timeseries_predictor(query, table, predictor_namespace, predictor)

        # add join
        # Update reference

        left = Identifier(predictor_steps["predictor"].result.ref_name)
        right = Identifier(predictor_steps["data"].result.ref_name)

        if not predictor_is_left:
            # swap join
            left, right = right, left
        new_join = Join(left=left, right=right, join_type=join.join_type)

        left = predictor_steps["predictor"].result
        right = predictor_steps["data"].result
        if not predictor_is_left:
            # swap join
            left, right = right, left

        last_step = self.planner.plan.add_step(JoinStep(left=left, right=right, query=new_join))

        # limit from timeseries
        if predictor_steps.get("saved_limit"):
            last_step = self.planner.plan.add_step(
                LimitOffsetStep(dataframe=last_step.result, limit=predictor_steps["saved_limit"])
            )

        return self.planner.plan_project(orig_query, last_step.result)

    def plan_timeseries_predictor(self, query, table, predictor_namespace, predictor):
        predictor_metadata = self.planner.get_predictor(predictor)

        predictor_time_column_name = predictor_metadata["order_by_column"]
        predictor_group_by_names = predictor_metadata["group_by_columns"]
        if predictor_group_by_names is None:
            predictor_group_by_names = []
        predictor_window = predictor_metadata["window"]

        if query.order_by:
            raise PlanningException(
                f"Can't provide ORDER BY to time series predictor, it will be taken from predictor settings. Found: {query.order_by}"
            )

        saved_limit = None
        if query.limit is not None:
            saved_limit = query.limit.value

        if query.group_by or query.having or query.offset:
            raise PlanningException(f"Unsupported query to timeseries predictor: {str(query)}")

        allowed_columns = [predictor_time_column_name.lower()]
        if len(predictor_group_by_names) > 0:
            allowed_columns += [i.lower() for i in predictor_group_by_names]

        no_time_filter_query = copy.deepcopy(query)

        preparation_where = no_time_filter_query.where

        validate_ts_where_condition(preparation_where, allowed_columns=allowed_columns)

        time_filter = find_time_filter(preparation_where, time_column_name=predictor_time_column_name)

        order_by = [OrderBy(Identifier(parts=[predictor_time_column_name]), direction="DESC")]

        query_modifiers = query.modifiers

        # add {order_by_field} is not null
        def add_order_not_null(condition):
            order_field_not_null = BinaryOperation(
                op="is not", args=[Identifier(parts=[predictor_time_column_name]), NullConstant()]
            )
            if condition is not None:
                condition = BinaryOperation(op="and", args=[condition, order_field_not_null])
            else:
                condition = order_field_not_null
            return condition

        preparation_where2 = copy.deepcopy(preparation_where)
        preparation_where = add_order_not_null(preparation_where)

        # Obtain integration selects
        if isinstance(time_filter, BetweenOperation):
            between_from = time_filter.args[1]
            preparation_time_filter = BinaryOperation("<", args=[Identifier(predictor_time_column_name), between_from])
            preparation_where2 = replace_time_filter(preparation_where2, time_filter, preparation_time_filter)
            integration_select_1 = Select(
                targets=[Star()],
                from_table=table,
                where=add_order_not_null(preparation_where2),
                modifiers=query_modifiers,
                order_by=order_by,
                limit=Constant(predictor_window),
            )

            integration_select_2 = Select(
                targets=[Star()],
                from_table=table,
                where=preparation_where,
                modifiers=query_modifiers,
                order_by=order_by,
            )

            integration_selects = [integration_select_1, integration_select_2]
        elif isinstance(time_filter, BinaryOperation) and time_filter.op == ">" and time_filter.args[1] == Latest():
            integration_select = Select(
                targets=[Star()],
                from_table=table,
                where=preparation_where,
                modifiers=query_modifiers,
                order_by=order_by,
                limit=Constant(predictor_window),
            )
            integration_select.where = find_and_remove_time_filter(integration_select.where, time_filter)
            integration_selects = [integration_select]
        elif isinstance(time_filter, BinaryOperation) and time_filter.op == "=":
            integration_select = Select(
                targets=[Star()],
                from_table=table,
                where=preparation_where,
                modifiers=query_modifiers,
                order_by=order_by,
                limit=Constant(predictor_window),
            )

            if type(time_filter.args[1]) is Latest:
                integration_select.where = find_and_remove_time_filter(integration_select.where, time_filter)
            else:
                time_filter_date = time_filter.args[1]
                preparation_time_filter = BinaryOperation(
                    "<=", args=[Identifier(predictor_time_column_name), time_filter_date]
                )
                integration_select.where = add_order_not_null(
                    replace_time_filter(preparation_where2, time_filter, preparation_time_filter)
                )
                time_filter.op = ">"

            integration_selects = [integration_select]
        elif isinstance(time_filter, BinaryOperation) and time_filter.op in (">", ">="):
            time_filter_date = time_filter.args[1]
            preparation_time_filter_op = {">": "<=", ">=": "<"}[time_filter.op]

            preparation_time_filter = BinaryOperation(
                preparation_time_filter_op, args=[Identifier(predictor_time_column_name), time_filter_date]
            )
            preparation_where2 = replace_time_filter(preparation_where2, time_filter, preparation_time_filter)
            integration_select_1 = Select(
                targets=[Star()],
                from_table=table,
                where=add_order_not_null(preparation_where2),
                modifiers=query_modifiers,
                order_by=order_by,
                limit=Constant(predictor_window),
            )

            integration_select_2 = Select(
                targets=[Star()],
                from_table=table,
                where=preparation_where,
                modifiers=query_modifiers,
                order_by=order_by,
            )

            integration_selects = [integration_select_1, integration_select_2]
        else:
            integration_select = Select(
                targets=[Star()],
                from_table=table,
                where=preparation_where,
                modifiers=query_modifiers,
                order_by=order_by,
            )
            integration_selects = [integration_select]

        if len(predictor_group_by_names) == 0:
            # ts query without grouping
            # one or multistep
            if len(integration_selects) == 1:
                select_partition_step = self.planner.get_integration_select_step(integration_selects[0])
            else:
                select_partition_step = MultipleSteps(
                    steps=[self.planner.get_integration_select_step(s) for s in integration_selects], reduce="union"
                )

            # fetch data step
            data_step = self.planner.plan.add_step(select_partition_step)
        else:
            # inject $var to queries
            for integration_select in integration_selects:
                condition = integration_select.where
                for num, column in enumerate(predictor_group_by_names):
                    cond = BinaryOperation("=", args=[Identifier(column), Constant(f"$var[{column}]")])

                    # join to main condition
                    if condition is None:
                        condition = cond
                    else:
                        condition = BinaryOperation("and", args=[condition, cond])

                integration_select.where = condition
            # one or multistep
            if len(integration_selects) == 1:
                select_partition_step = self.planner.get_integration_select_step(integration_selects[0])
            else:
                select_partition_step = MultipleSteps(
                    steps=[self.planner.get_integration_select_step(s) for s in integration_selects], reduce="union"
                )

            # get groping values
            no_time_filter_query.where = find_and_remove_time_filter(no_time_filter_query.where, time_filter)
            select_partitions_step = self.plan_fetch_timeseries_partitions(
                no_time_filter_query, table, predictor_group_by_names
            )

            # sub-query by every grouping value
            map_reduce_step = self.planner.plan.add_step(
                MapReduceStep(values=select_partitions_step.result, reduce="union", step=select_partition_step)
            )
            data_step = map_reduce_step

        predictor_identifier = utils.get_predictor_name_identifier(predictor)

        params = None
        if query.using is not None:
            params = query.using
        predictor_step = self.planner.plan.add_step(
            ApplyTimeseriesPredictorStep(
                output_time_filter=time_filter,
                namespace=predictor_namespace,
                dataframe=data_step.result,
                predictor=predictor_identifier,
                params=params,
            )
        )

        return {
            "predictor": predictor_step,
            "data": data_step,
            "saved_limit": saved_limit,
        }
