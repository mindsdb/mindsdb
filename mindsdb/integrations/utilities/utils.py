import pandas as pd
from mindsdb_sql.parser.ast import Identifier, Constant, Star, Select, Join, BinaryOperation
from mindsdb.api.mysql.mysql_proxy.controllers.session_controller import SessionController
from mindsdb.interfaces.database.integrations import IntegrationController
from mindsdb.interfaces.model.model_controller import ModelController
from mindsdb.interfaces.database.views import ViewController


def make_sql_session(company_id):

    server_obj = type('', (), {})()
    server_obj.original_integration_controller = IntegrationController()
    server_obj.original_model_controller = ModelController()
    server_obj.original_view_controller = ViewController()

    sql_session = SessionController(
        server=server_obj,
        company_id=company_id
    )
    sql_session.database = 'mindsdb'
    return sql_session


def get_where_data(where):
    result = {}
    if type(where) != BinaryOperation:
        raise Exception("Wrong 'where' statement")
    if where.op == '=':
        if type(where.args[0]) != Identifier or type(where.args[1]) != Constant:
            raise Exception("Wrong 'where' statement")
        result[where.args[0].parts[-1]] = where.args[1].value
    elif where.op == 'and':
        result.update(get_where_data(where.args[0]))
        result.update(get_where_data(where.args[1]))
    else:
        raise Exception("Wrong 'where' statement")
    return result


def recur_get_conditionals(args: list, values):
    """ Gets all the specified data from an arbitrary amount of AND clauses inside the WHERE statement """  # noqa
    if isinstance(args[0], Identifier) and isinstance(args[1], Constant):
        values[args[0].parts[0]] = [args[1].value]
    else:
        for op in args:
            values = {**values, **recur_get_conditionals([*op.args], {})}
    return values


def get_aliased_columns(aliased_columns, model_alias, targets, mode=None):
    """ This method assumes mdb_sql will alert if there are two columns with the same alias """  # noqa
    for col in targets:
        if mode == 'input':
            if str(col.parts[0]) != model_alias and col.alias is not None:
                aliased_columns[aliased_columns.index(col.parts[-1])] = str(col.alias)

        if mode == 'output':
            if str(col.parts[0]) == model_alias and col.alias is not None:
                aliased_columns[aliased_columns.index('prediction')] = str(col.alias)

    return aliased_columns


def default_data_gather(handler, query):
    """ Default method to gather data from another handler into a pandas dataframe """  # noqa
    records = handler.query(query).data_frame
    df = pd.DataFrame.from_records(records)
    return df


def get_join_input(query, model, model_aliases, data_handler, data_side):
    target_cols = set()
    for t in query.targets:
        if t.parts[0] not in model_aliases:
            if t.parts[-1] == Star():
                target_cols = [Star()]
                break
            else:
                target_cols.add(t.parts[-1])

    if target_cols != [Star()]:
        target_cols = [Identifier(col) for col in target_cols]

    data_handler_table = getattr(query.from_table, data_side).parts[-1]
    data_query = Select(
        targets=target_cols,
        from_table=Identifier(data_handler_table),
        where=query.where,
        group_by=query.group_by,
        having=query.having,
        order_by=query.order_by,
        offset=query.offset,
        limit=query.limit
    )

    model_input = pd.DataFrame.from_records(
        data_handler.query(data_query).data_frame
    )

    return model_input


def get_model_name(handler, stmt):
    """ Discern between joined entities to retrieve model name, alias and the clause side it is on. """
    side = None
    models = handler.get_tables() # .data_frame['model_name'].values
    if type(stmt.from_table) == Join:
        model_name = stmt.from_table.right.parts[-1]
        side = 'right'
        if model_name not in models:
            model_name = stmt.from_table.left.parts[-1]
            side = 'left'
        alias = str(getattr(stmt.from_table, side).alias)
    else:
        model_name = stmt.from_table.parts[-1]
        alias = None  # todo: fix this

    if model_name not in models:
        raise Exception("Error, not found. Please create this predictor first.")

    return model_name, alias, side