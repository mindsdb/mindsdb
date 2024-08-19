import copy
from mindsdb_sql.parser import ast
from mindsdb_sql.exceptions import PlanningException
from mindsdb_sql.planner import steps
from mindsdb_sql.planner import utils


def to_string(identifier):
    # alternative to AST.to_string() but without quoting
    return '.'.join(identifier.parts)

class Table:
    def __init__(self,  node=None, ds=None, is_predictor=None):
        self.node = node
        self.is_predictor = is_predictor
        self.ds = ds
        self.name = to_string(node)
        self.columns = None
        # None is unknown
        self.columns_map = None
        self.keys = None
        if node.alias:
            self.alias = to_string(node.alias)
        else:
            self.alias = None
            # self.alias = self.name


class Column:
    def __init__(self, node=None, table=None, name=None, type=None):
        alias = None
        if node is not None:

            if isinstance(node, ast.Identifier):
                # set name
                name = node.parts[-1]  # ???

        else:
            if table is not None and name is not None:
                node = ast.Identifier(parts=[table.name, name])

        self.node = node # link to AST
        self.alias = alias # for ProjectStep

        self.is_star = False

        self.table = table # link to AST table
        self.name = name # column name
        self.type = type


class Statement:
    def __init__(self):
        self.columns = []
        # self.query = None
        self.params = None
        self.result = None

        # Tables on first level of select
        self.tables_lvl1 = None

        # mapping tables by name {'ta.ble': Table()}
        self.tables_map = None

        self.offset = 0



class PreparedStatementPlanner():

    def __init__(self, planner):
        self.planner = planner

    def get_type_of_var(self, v):
        if isinstance(v, str):
            return 'str'
        elif isinstance(v, float):
            return 'float'
        elif isinstance(v, int):
            return 'integer'

        return 'str'


    def get_statement_info(self):
        stmt = self.planner.statement

        if stmt is None:
            raise PlanningException('Statement is not prepared')

        columns_result = []

        for column in stmt.columns:
            table, ds = None, None
            if column.table is not None:
                table = column.table.name
                ds = column.table.ds
            columns_result.append(dict(
                alias=column.alias,
                type=column.type,
                name=column.name,
                table_name=table,
                table_alias=table,
                ds=ds,
            ))

        parameters = []
        for param in stmt.params:
            name = '?'
            parameters.append(dict(
                alias=name,
                type='str',
                name=name,
            ))

        return {
            'parameters': parameters,
            'columns': columns_result
        }

    def get_table_of_column(self, t):

        tables_map = self.planner.statement.tables_map

        # get tables to check
        if len(t.parts) > 1:
            # try to find table
            table_parts = t.parts[:-1]
            table_name = '.'.join(table_parts)
            if table_name in tables_map:
                return tables_map[table_name]

            elif len(table_parts) > 1:
                # maybe datasource is 1st part
                table_parts = table_parts[1:]
                table_name = '.'.join(table_parts)
                if table_name in tables_map:
                    return tables_map[table_name]

    def table_from_identifier(self, table):
        # disambiguate
        if self.planner.is_predictor(table):
            ds, table = self.planner.get_predictor_namespace_and_name_from_identifier(table)
            is_predictor = True

        else:
            ds, table = self.planner.resolve_database_table(table)
            is_predictor = False

        if table.alias is not None:
            # access by alias if table is having alias
            keys = [to_string(table.alias)]

        else:
            # access by table name, in all variants
            keys = []
            parts = []
            # in reverse order
            for p in table.parts[::-1]:
                parts.insert(0, p)
                keys.append('.'.join(parts))

        # remember table
        tbl = Table(
            ds=ds,
            node=table,
            is_predictor=is_predictor
        )
        tbl.keys = keys

        return tbl

    def prepare_select(self, query):
        # prepare select with or without predictor

        stmt = self.planner.statement

        # get all predictors
        query_predictors = []

        def find_predictors(node, is_table, **kwargs):
            if is_table and isinstance(node, ast.Identifier):
                if self.planner.is_predictor(node):
                    query_predictors.append(node)

        utils.query_traversal(query, find_predictors)

        # only 1 predictor is allowed
        # if len(query_predictors) > 1:
        #     raise PlanningException(f'To many predictors in query: {len(query_predictors)}')

        # === get all tables from 1st level of query ===
        stmt.tables_map = {}
        stmt.tables_lvl1 = []
        if query.from_table is not None:

            if isinstance(query.from_table, ast.Join):
                # get all tables
                join_tables = utils.convert_join_to_list(query.from_table)
            else:
                join_tables = [dict(table=query.from_table)]

            if isinstance(query.from_table, ast.Select):
                # nested select, get only last select
                join_tables = [
                    dict(
                        table=utils.get_deepest_select(query.from_table).from_table
                    )
                ]

            for i, join_table in enumerate(join_tables):
                table = join_table['table']
                if isinstance(table, ast.Identifier):
                    tbl = self.table_from_identifier(table)

                    if tbl.is_predictor:
                        # Is the last table?
                        if i + 1 < len(join_tables):
                            raise PlanningException(f'Predictor must be last table in query')

                    stmt.tables_lvl1.append(tbl)
                    for key in tbl.keys:
                        stmt.tables_map[key] = tbl

                else:
                    # don't add unknown table to looking list
                    continue

        # is there any predictors at other levels?
        lvl1_predictors = [i for i in stmt.tables_lvl1 if i.is_predictor]
        if len(query_predictors) != len(lvl1_predictors):
            raise PlanningException('Predictor is not at first level')

        # === get targets ===
        columns = []
        get_all_tables = False
        for t in query.targets:

            column = Column(t)

            # column alias
            alias = None
            if t.alias is not None:
                alias = to_string(t.alias)

            if isinstance(t, ast.Star):
                if len(stmt.tables_lvl1) == 0:
                    # if "from" is emtpy we can't make plan
                    raise PlanningException("Can't find table")

                column.is_star = True
                get_all_tables = True

            elif isinstance(t, ast.Identifier):
                if alias is None:
                    alias = t.parts[-1]

                table = self.get_table_of_column(t)
                if table is None:
                    # table is not known
                    get_all_tables = True
                else:
                    column.table = table

            elif isinstance(t, ast.Constant):
                if alias is None:
                    alias = str(t.value)
                column.type = self.get_type_of_var(t.value)
            elif isinstance(t, ast.Function):
                # mysql function
                if t.op == 'connection_id':
                    column.type = 'integer'
                else:
                    column.type = 'str'
            else:
                # TODO go down into lower level.
                #  It can be function, operation, select.
                #  But now show it as string

                # TODO add several known types for function, i.e ABS-int

                # TODO TypeCast - as casted type
                column.type = 'str'

            if alias is not None:
                column.alias = alias
            columns.append(column)

        # === get columns from tables ===
        request_tables = set()
        for column in columns:
            if column.table is not None:
                request_tables.add(column.table.name)

        for table in stmt.tables_lvl1:
            if get_all_tables or table.name in request_tables:
                if table.is_predictor:
                    step = steps.GetPredictorColumns(namespace=table.ds, predictor=table.node)
                else:
                    step = steps.GetTableColumns(namespace=table.ds, table=table.name)
                yield step

                if step.result_data is not None:
                    # save results

                    if len(step.result_data['tables']) > 0:
                        table_info = step.result_data['tables'][0]
                        columns_info = step.result_data['columns'][table_info]

                        table.columns = []
                        table.ds = table_info[0]
                        for col in columns_info:
                            if isinstance(col, tuple):
                                # is predictor
                                col = dict(name=col[0], type='str')
                            table.columns.append(
                                Column(
                                    name=col['name'],
                                    type=col['type'],
                                )
                            )

                    # map by names
                    table.columns_map = {
                        i.name.upper(): i
                        for i in table.columns
                    }

        # === create columns list ===
        columns_result = []
        for i, column in enumerate(columns):
            if column.is_star:
                # add data from all tables
                for table in stmt.tables_lvl1:
                    if table.columns is None:
                        raise PlanningException(f'Table is not found {table.name}')

                    for col in table.columns:
                        # col = {name: 'col', type: 'str'}
                        column2 = Column(table=table, name=col.name)
                        column2.alias = col.name
                        column2.type = col.type

                        columns_result.append(column2)

                # to next column
                continue

            elif column.name is not None:
                # is Identifier
                col_name = column.name.upper()
                if column.table is not None:
                    table = column.table
                    if table.columns_map is not None:
                        if col_name in table.columns_map:
                            column.type = table.columns_map[col_name].type
                        else:
                            # print(col_name, table.name, query.to_string())
                            # continue
                            raise PlanningException(f'Column not found {col_name}')


                else:
                    # table is not found, looking for in all tables
                    for table in stmt.tables_lvl1:
                        if table.columns_map is not None:
                            col = table.columns_map.get(col_name)
                            if col is not None:
                                column.type = col.type
                                column.table = table
                                break



            # forcing alias
            if column.alias is None:
                column.alias = f'column_{i}'

            # forcing type
            if column.type is None:
                column.type = 'str'

            columns_result.append(column)

        # save columns
        stmt.columns = columns_result

    def prepare_insert(self, query):
        stmt = self.planner.statement

        # get table columns
        table = self.table_from_identifier(query.table)
        if table.is_predictor:
            step = steps.GetPredictorColumns(namespace=table.ds, predictor=table.node)
        else:
            step = steps.GetTableColumns(namespace=table.ds, table=table.name)
        yield step

        if step.result_data is not None:
            # save results

            if len(step.result_data['tables']) > 0:
                table_info = step.result_data['tables'][0]
                columns_info = step.result_data['columns'][table_info]

                table.columns = []
                table.ds = table_info[0]
                for col in columns_info:
                    if isinstance(col, tuple):
                        # is predictor
                        col = dict(name=col[0], type='str')
                    table.columns.append(
                        Column(
                            name=col['name'],
                            type=col['type'],
                        )
                    )

                # map by names
                table.columns_map = {
                    i.name.upper(): i
                    for i in table.columns
                }

        # save results
        columns_result = []
        for col in query.columns:
            col_name = col.parts[-1]

            column = Column(table=table, name=col_name)

            if table.columns_map is not None:
                col = table.columns_map.get(col_name)
                if col is not None:
                    column.type = col.type

            if column.type is None:
                # forcing type
                column.type = 'str'

            columns_result.append(column)

        stmt.columns = columns_result

    def prepare_show(self, query):
        stmt = self.planner.statement

        stmt.columns = [
            Column(name='Variable_name', type='str'),
            Column(name='Value', type='str'),
        ]
        return []

    def prepare_steps(self, query):

        stmt = Statement()
        self.planner.statement = stmt

        self.planner.query = query

        query = copy.deepcopy(query)

        params = utils.get_query_params(query)

        stmt.params = params

        # get columns
        if isinstance(query, ast.Select):
            # prepare select
            return self.prepare_select(query)
        if isinstance(query, ast.Union):
            # get column definition only from select
            return self.prepare_select(query.left)
        if isinstance(query, ast.Insert):
            # return self.prepare_insert(query)
            # TODO do we need columns?
            return []
        if isinstance(query, ast.Delete):
            ...
            # TODO do we need columns?
            return []
        if isinstance(query, ast.Show):
            return self.prepare_show(query)
        else:

            # do nothing
            return []
            # raise NotImplementedError(query.__name__)

    def execute_steps(self, params=None):
        # find all parameters
        stmt = self.planner.statement

        # is already executed
        if stmt is None:
            if params is not None:
                raise PlanningException("Can't execute statement")
            stmt = Statement()

        # === form query with new target ===

        query = self.planner.query

        if params is not None:

            if len(params) != len(stmt.params):
                raise PlanningException("Count of execution parameters don't match prepared statement")

            query = utils.fill_query_params(query, params)

            self.planner.query = query

        # prevent from second execution
        stmt.params = None

        if (
                isinstance(query, ast.Select)
                or isinstance(query, ast.Union)
                or isinstance(query, ast.CreateTable)
                or isinstance(query, ast.Insert)
                or isinstance(query, ast.Update)
                or isinstance(query, ast.Delete)
        ):
            return self.plan_query(query)
        else:
            return []

    def plan_query(self, query):
        # use v1 planner
        self.planner.from_query(query)
        step = None
        for step in self.planner.plan.steps:
            # print(step)
            yield step

        # # save results from last_step
        # stmt = self.planner.statement
        # stmt.result = step.result_data

    # not used yet
    # def fetch(self, row_count):
    #     # split the query to predictor and rest of the query
    #
    #     stmt = self.planner.statement
    #     offset = stmt.offset
    #     offset2 = offset + row_count
    #
    #     stmt.offset = offset2
    #
    #     return stmt.result[offset: offset2]
    #
    # def close(self):
    #     # clear
    #     self.planner.statement = None


    # def plan_query_v2(self, query):
    #     columns_result = stmt.columns
    #
    #     targets = []
    #     for col in columns_result:
    #         if col.node is None:
    #             raise Exception('something wrong')
    #         targets.append(col.node)

    #     # Not used yet
    #     raise NotImplementedError()
    #
    #     stmt = self.planner.statement
    #     columns_result = stmt.columns
    #
    #     # === plan ===
    #     plan = query_plan.QueryPlan()
    #     # query_properties = {}
    #
    #     # is predictor in query
    #     lvl1_predictors = [i for i in stmt.tables_lvl1 if i.is_predictor]
    #     if len(lvl1_predictors) == 0:
    #         # no predictor: run query as is
    #         plan.add_step(self.planner.get_integration_select_step(query))
    #
    #         # query_properties['no_predictors'] = True
    #     else:
    #         # predictor is the only table?
    #         if isinstance(query.from_table, ast.Identifier) and self.planner.is_predictor(query.from_table):
    #             # Only predictor
    #             # TODO self.plan_select_from_predictor(query)
    #
    #             plan.add_step(self.planner.plan_select_from_predictor(query))
    #
    #             # query_properties['no_tables'] = True
    #         else:
    #             # this is predictor with table
    #
    #             # remove predictor from query
    #             prediction_props = {}
    #
    #             def remove_predictor(node, is_table, **kwargs):
    #
    #                 if isinstance(node, ast.Join):
    #                     if self.planner.is_predictor(node.right):
    #                         # remember conditions
    #                         prediction_props['conditions'] = node.condition
    #
    #                         # left only left join
    #                         return node.left
    #
    #             utils.query_traversal(query, remove_predictor)
    #
    #             # remove predictor fields from targets
    #             columns_result2 = []
    #             for col in columns_result:
    #                 if not col.table.is_predictor:
    #                     columns_result2.append(col)
    #
    #             # add predictor fields from join condition to targets
    #             def find_predictor_columns(node, **kwargs):
    #                 if isinstance(node, ast.Identifier):
    #                     tbl = self.get_table_of_column(node)
    #                     if tbl.is_predictor:
    #                         col = Column(node)
    #                         col.table = tbl
    #                         columns_result2.append(col)
    #
    #             utils.query_traversal(prediction_props['conditions'], find_predictor_columns)
    #
    #             predictor = lvl1_predictors[0].name
    #
    #             # TODO move predictor planning from old planner
    #             # if self.planner.predictor_metadata[predictor].get('timeseries'):
    #             #     predictor_steps = self.planner.plan_timeseries_predictor(query, table, predictor_namespace, predictor)
    #             # else:
    #             #     predictor_steps = self.planner.plan_predictor(query, table, predictor_namespace, predictor)
    #             #
    #             # ...
    #
    #         # returns no steps
    #         return self.plan