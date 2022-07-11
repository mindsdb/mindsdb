import json
import datetime
from typing import Optional

import pandas as pd

from mindsdb_sql.parser.dialects.mindsdb import (
    CreateDatasource,
    RetrainPredictor,
    CreatePredictor,
    DropDatasource,
    DropPredictor,
    CreateView
)
from mindsdb_sql import parse_sql
from mindsdb.api.mysql.mysql_proxy.utilities.sql import query_df
from mindsdb.api.mysql.mysql_proxy.utilities import log
from mindsdb_sql.parser.dialects.mysql import Variable

from mindsdb_sql.parser.ast import (
    RollbackTransaction,
    CommitTransaction,
    StartTransaction,
    BinaryOperation,
    DropDatabase,
    NullConstant,
    Describe,
    Constant,
    Function,
    Explain,
    Delete,
    Insert,
    Select,
    Star,
    Show,
    Set,
    Use,
    Alter,
    Update,
    CreateTable,
    TableColumn,
    Identifier,
    DropTables,
    Operation,
    ASTNode,
)

from mindsdb.api.mysql.mysql_proxy.utilities import (
    SqlApiException,
    ErBadDbError,
    ErBadTableError,
    ErKeyColumnDoesNotExist,
    ErTableExistError,
    ErDubFieldName,
    ErDbDropDelete,
    ErNonInsertableTable,
    ErNotSupportedYet,
    ErSqlSyntaxError,
    ErSqlWrongArguments,
)
from mindsdb.api.mysql.mysql_proxy.utilities.functions import get_column_in_case

from mindsdb.api.mysql.mysql_proxy.classes.sql_query import (
    SQLQuery, Column
)

from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import (
    CHARSET_NUMBERS,
    ERR,
    TYPES,
    SERVER_VARIABLES,
)

from mindsdb.api.mysql.mysql_proxy.executor.data_types import ExecuteAnswer, ANSWER_TYPE
from mindsdb.integrations.libs.response import HandlerStatusResponse


class ExecuteCommands:

    def __init__(self, session, executor):
        self.session = session
        self.executor = executor

        self.charset_text_type = CHARSET_NUMBERS['utf8_general_ci']
        self.datahub = session.datahub

    def execute_command(self, statement):
        sql = None
        if self.executor is None:
            if isinstance(statement, ASTNode):
                sql = statement.to_string()
            sql_lower = sql.lower()
        else:
            sql = self.executor.sql
            sql_lower = self.executor.sql_lower

        if type(statement) == CreateDatasource:
            struct = {
                'datasource_name': statement.name,
                'database_type': statement.engine.lower(),
                'connection_args': statement.parameters
            }
            return self.answer_create_datasource(struct)
        if type(statement) == DropPredictor:
            predictor_name = statement.name.parts[-1]
            try:
                self.session.datahub['mindsdb'].delete_predictor(predictor_name)
            except Exception as e:
                if not statement.if_exists:
                    raise e
            return ExecuteAnswer(ANSWER_TYPE.OK)
        elif type(statement) == DropTables:
            return self.answer_drop_tables(statement)
        elif type(statement) == DropDatasource or type(statement) == DropDatabase:
            ds_name = statement.name.parts[-1]
            return self.answer_drop_datasource(ds_name)
        elif type(statement) == Describe:
            # NOTE in sql 'describe table' is same as 'show columns'
            predictor_attrs = ("model", "features", "ensemble")
            if statement.value.parts[-1] in predictor_attrs:
                return self.answer_describe_predictor(statement.value.parts[-2:])
            else:
                return self.answer_describe_predictor(statement.value.parts[-1])
        elif type(statement) == RetrainPredictor:
            return self.answer_retrain_predictor(statement.name.parts[-1])
        elif type(statement) == Show:
            sql_category = statement.category.lower()
            if sql_category == 'predictors':
                where = statement.where
                if statement.like is not None:
                    like = BinaryOperation('like', args=[Identifier('name'), Constant(statement.like)])
                    if where is not None:
                        where = BinaryOperation('and', args=[where, like])
                    else:
                        where = like
                new_statement = Select(
                    targets=[Star()],
                    from_table=Identifier(parts=[self.session.database or 'mindsdb', 'predictors']),
                    where=where
                )
                query = SQLQuery(
                    new_statement,
                    session=self.session
                )
                return self.answer_select(query)
            elif sql_category == 'views':
                where = BinaryOperation('and', args=[
                    BinaryOperation('=', args=[Identifier('table_schema'), Constant('views')]),
                    BinaryOperation('like', args=[Identifier('table_type'), Constant('BASE TABLE')])
                ])
                if statement.where is not None:
                    where = BinaryOperation('and', args=[where, statement.where])
                if statement.like is not None:
                    like = BinaryOperation('like', args=[Identifier('View'), Constant(statement.like)])
                    where = BinaryOperation('and', args=[where, like])

                new_statement = Select(
                    targets=[Identifier(parts=['table_name'], alias=Identifier('View'))],
                    from_table=Identifier(parts=['information_schema', 'TABLES']),
                    where=where
                )

                query = SQLQuery(
                    new_statement,
                    session=self.session
                )
                return self.answer_select(query)
            elif sql_category == 'plugins':
                if statement.where is not None or statement.like:
                    raise SqlApiException("'SHOW PLUGINS' query should be used without filters")
                new_statement = Select(
                    targets=[Star()],
                    from_table=Identifier(parts=['information_schema', 'PLUGINS'])
                )
                query = SQLQuery(
                    new_statement,
                    session=self.session
                )
                return self.answer_select(query)
            elif sql_category in ('databases', 'schemas'):
                where = statement.where
                if statement.like is not None:
                    like = BinaryOperation('like', args=[Identifier('Database'), Constant(statement.like)])
                    if where is not None:
                        where = BinaryOperation('and', args=[where, like])
                    else:
                        where = like

                new_statement = Select(
                    targets=[Identifier(parts=["schema_name"], alias=Identifier('Database'))],
                    from_table=Identifier(parts=['information_schema', 'SCHEMATA']),
                    where=where
                )
                if statement.where is not None:
                    new_statement.where = statement.where

                query = SQLQuery(
                    new_statement,
                    session=self.session
                )
                return self.answer_select(query)
            elif sql_category == 'datasources':
                where = statement.where
                if statement.like is not None:
                    like = BinaryOperation('like', args=[Identifier('name'), Constant(statement.like)])
                    if where is not None:
                        where = BinaryOperation('and', args=[where, like])
                    else:
                        where = like
                new_statement = Select(
                    targets=[Star()],
                    from_table=Identifier(parts=['mindsdb', 'datasources']),
                    where=where
                )
                query = SQLQuery(
                    new_statement,
                    session=self.session
                )
                return self.answer_select(query)
            elif sql_category in ('tables', 'full tables'):
                schema = self.session.database or 'mindsdb'
                if statement.from_table is not None:
                    schema = statement.from_table.parts[-1]
                where = BinaryOperation('and', args=[
                    BinaryOperation('=', args=[Identifier('table_schema'), Constant(schema)]),
                    BinaryOperation('or', args=[
                        BinaryOperation('=', args=[Identifier('table_type'), Constant('BASE TABLE')]),
                        BinaryOperation('or', args=[
                            BinaryOperation('=', args=[Identifier('table_type'), Constant('SYSTEM VIEW')]),
                            BinaryOperation('=', args=[Identifier('table_type'), Constant('VIEW')])
                        ])
                    ])
                ])
                if statement.where is not None:
                    where = BinaryOperation('and', args=[statement.where, where])
                if statement.like is not None:
                    like = BinaryOperation('like', args=[Identifier(f'Tables_in_{schema}'), Constant(statement.like)])
                    if where is not None:
                        where = BinaryOperation('and', args=[where, like])
                    else:
                        where = like

                new_statement = Select(
                    targets=[Identifier(parts=['table_name'], alias=Identifier(f'Tables_in_{schema}'))],
                    from_table=Identifier(parts=['information_schema', 'TABLES']),
                    where=where
                )

                if statement.modes is not None:
                    modes = [m.upper() for m in statement.modes]
                    # show full tables. show always 'BASE TABLE'
                    if 'FULL' in modes:
                        new_statement.targets.append(
                            Constant(value='BASE TABLE', alias=Identifier('Table_type'))
                        )

                query = SQLQuery(
                    new_statement,
                    session=self.session
                )
                return self.answer_select(query)
            elif sql_category in ('variables', 'session variables', 'session status', 'global variables'):
                where = statement.where
                if statement.like is not None:
                    like = BinaryOperation('like', args=[Identifier('Variable_name'), Constant(statement.like)])
                    if where is not None:
                        where = BinaryOperation('and', args=[where, like])
                    else:
                        where = like

                new_statement = Select(
                    targets=[Identifier(parts=['Variable_name']), Identifier(parts=['Value'])],
                    from_table=Identifier(parts=['dataframe']),
                    where=where
                )

                data = {}
                is_session = 'session' in sql_category
                for var_name, var_data in SERVER_VARIABLES.items():
                    var_name = var_name.replace('@@', '')
                    if is_session and var_name.startswith('session.') is False:
                        continue
                    if var_name.startswith('session.') or var_name.startswith('GLOBAL.'):
                        name = var_name.replace('session.', '').replace('GLOBAL.', '')
                        data[name] = var_data[0]
                    elif var_name not in data:
                        data[var_name] = var_data[0]

                df = pd.DataFrame(data.items(), columns=['Variable_name', 'Value'])
                data = query_df(df, new_statement)
                data = data.values.tolist()

                columns = [
                    Column(name='Variable_name', table_name='session_variables', type='str'),
                    Column(name='Value', table_name='session_variables', type='str'),
                ]

                return ExecuteAnswer(
                    answer_type=ANSWER_TYPE.TABLE,
                    columns=columns,
                    data=data
                )
            elif "show status like 'ssl_version'" in sql_lower:
                return ExecuteAnswer(
                    answer_type=ANSWER_TYPE.TABLE,
                    columns=[
                        Column(name='Value', table_name='session_variables', type='str'),
                        Column(name='Value', table_name='session_variables', type='str'),
                    ],
                    data=[['Ssl_version', 'TLSv1.1']]
                )
            elif sql_category in ('function status', 'procedure status'):
                # SHOW FUNCTION STATUS WHERE Db = 'MINDSDB';
                # SHOW PROCEDURE STATUS WHERE Db = 'MINDSDB'
                # SHOW FUNCTION STATUS WHERE Db = 'MINDSDB' AND Name LIKE '%';
                return self.answer_function_status()
            elif sql_category in ('index', 'keys', 'indexes'):
                # INDEX | INDEXES | KEYS are synonyms
                # https://dev.mysql.com/doc/refman/8.0/en/show-index.html
                new_statement = Select(
                    targets=[
                        Identifier('TABLE_NAME', alias=Identifier('Table')),
                        Identifier('NON_UNIQUE', alias=Identifier('Non_unique')),
                        Identifier('INDEX_NAME', alias=Identifier('Key_name')),
                        Identifier('SEQ_IN_INDEX', alias=Identifier('Seq_in_index')),
                        Identifier('COLUMN_NAME', alias=Identifier('Column_name')),
                        Identifier('COLLATION', alias=Identifier('Collation')),
                        Identifier('CARDINALITY', alias=Identifier('Cardinality')),
                        Identifier('SUB_PART', alias=Identifier('Sub_part')),
                        Identifier('PACKED', alias=Identifier('Packed')),
                        Identifier('NULLABLE', alias=Identifier('Null')),
                        Identifier('INDEX_TYPE', alias=Identifier('Index_type')),
                        Identifier('COMMENT', alias=Identifier('Comment')),
                        Identifier('INDEX_COMMENT', alias=Identifier('Index_comment')),
                        Identifier('IS_VISIBLE', alias=Identifier('Visible')),
                        Identifier('EXPRESSION', alias=Identifier('Expression'))
                    ],
                    from_table=Identifier(parts=['information_schema', 'STATISTICS']),
                    where=statement.where
                )
                query = SQLQuery(
                    new_statement,
                    session=self.session
                )
                return self.answer_select(query)
            # FIXME if have answer on that request, then DataGrip show warning '[S0022] Column 'Non_unique' not found.'
            elif 'show create table' in sql_lower:
                # SHOW CREATE TABLE `MINDSDB`.`predictors`
                table = sql[sql.rfind('.') + 1:].strip(' .;\n\t').replace('`', '')
                return self.answer_show_create_table(table)
            elif sql_category in ('character set', 'charset'):
                where = statement.where
                if statement.like is not None:
                    like = BinaryOperation('like', args=[Identifier('CHARACTER_SET_NAME'), Constant(statement.like)])
                    if where is not None:
                        where = BinaryOperation('and', args=[where, like])
                    else:
                        where = like
                new_statement = Select(
                    targets=[
                        Identifier('CHARACTER_SET_NAME', alias=Identifier('Charset')),
                        Identifier('DEFAULT_COLLATE_NAME', alias=Identifier('Description')),
                        Identifier('DESCRIPTION', alias=Identifier('Default collation')),
                        Identifier('MAXLEN', alias=Identifier('Maxlen'))
                    ],
                    from_table=Identifier(parts=['INFORMATION_SCHEMA', 'CHARACTER_SETS']),
                    where=where
                )
                query = SQLQuery(
                    new_statement,
                    session=self.session
                )
                return self.answer_select(query)
            elif sql_category == 'warnings':
                return self.answer_show_warnings()
            elif sql_category == 'engines':
                new_statement = Select(
                    targets=[Star()],
                    from_table=Identifier(parts=['information_schema', 'ENGINES'])
                )
                query = SQLQuery(
                    new_statement,
                    session=self.session
                )
                return self.answer_select(query)
            elif sql_category == 'collation':
                where = statement.where
                if statement.like is not None:
                    like = BinaryOperation('like', args=[Identifier('Collation'), Constant(statement.like)])
                    if where is not None:
                        where = BinaryOperation('and', args=[where, like])
                    else:
                        where = like
                new_statement = Select(
                    targets=[
                        Identifier('COLLATION_NAME', alias=Identifier('Collation')),
                        Identifier('CHARACTER_SET_NAME', alias=Identifier('Charset')),
                        Identifier('ID', alias=Identifier('Id')),
                        Identifier('IS_DEFAULT', alias=Identifier('Default')),
                        Identifier('IS_COMPILED', alias=Identifier('Compiled')),
                        Identifier('SORTLEN', alias=Identifier('Sortlen')),
                        Identifier('PAD_ATTRIBUTE', alias=Identifier('Pad_attribute'))
                    ],
                    from_table=Identifier(parts=['INFORMATION_SCHEMA', 'COLLATIONS']),
                    where=where
                )
                query = SQLQuery(
                    new_statement,
                    session=self.session
                )
                return self.answer_select(query)
            elif sql_category == 'table status':
                # TODO improve it
                # SHOW TABLE STATUS LIKE 'table'
                table_name = None
                if statement.like is not None:
                    table_name = statement.like
                # elif condition == 'from' and type(expression) == Identifier:
                #     table_name = expression.parts[-1]
                if table_name is None:
                    err_str = f"Can't determine table name in query: {sql}"
                    log.warning(err_str)
                    raise ErTableExistError(err_str)
                return self.answer_show_table_status(table_name)
            elif sql_category == 'columns':
                is_full = statement.modes is not None and 'full' in statement.modes
                return self.answer_show_columns(statement.from_table, statement.where, statement.like, is_full=is_full)
            else:
                raise ErNotSupportedYet(f'Statement not implemented: {sql}')
        elif type(statement) in (StartTransaction, CommitTransaction, RollbackTransaction):
            return ExecuteAnswer(ANSWER_TYPE.OK)
        elif type(statement) == Set:
            category = (statement.category or '').lower()
            if category == '' and type(statement.arg) == BinaryOperation:
                return ExecuteAnswer(ANSWER_TYPE.OK)
            elif category == 'autocommit':
                return ExecuteAnswer(ANSWER_TYPE.OK)
            elif category == 'names':
                # set names utf8;
                charsets = {
                    'utf8': CHARSET_NUMBERS['utf8_general_ci'],
                    'utf8mb4': CHARSET_NUMBERS['utf8mb4_general_ci']
                }
                self.charset = statement.arg.parts[0]
                self.charset_text_type = charsets.get(self.charset)
                if self.charset_text_type is None:
                    log.warning(f"Unknown charset: {self.charset}. Setting up 'utf8_general_ci' as charset text type.")
                    self.charset_text_type = CHARSET_NUMBERS['utf8_general_ci']
                return ExecuteAnswer(
                    ANSWER_TYPE.OK,
                    state_track=[
                        ['character_set_client', self.charset],
                        ['character_set_connection', self.charset],
                        ['character_set_results', self.charset]
                    ]
                )
            else:
                log.warning(f'SQL statement is not processable, return OK package: {sql}')
                return ExecuteAnswer(ANSWER_TYPE.OK)
        elif type(statement) == Use:
            db_name = statement.value.parts[-1]
            self.change_default_db(db_name)
            return ExecuteAnswer(ANSWER_TYPE.OK)
        elif type(statement) == CreatePredictor:
            return self.answer_create_predictor(statement)
        elif type(statement) == CreateView:
            return self.answer_create_view(statement)
        elif type(statement) == Delete:
            if self.session.database != 'mindsdb' and statement.table.parts[0] != 'mindsdb':
                raise ErBadTableError("Only 'DELETE' from database 'mindsdb' is possible at this moment")
            if statement.table.parts[-1] != 'predictors':
                raise ErBadTableError("Only 'DELETE' from table 'mindsdb.predictors' is possible at this moment")
            self.delete_predictor_query(statement)
            return ExecuteAnswer(ANSWER_TYPE.OK)
        elif type(statement) == Insert:
            if statement.from_select is None:
                return self.process_insert(statement)
            else:
                # run with planner
                SQLQuery(
                    statement,
                    session=self.session,
                    execute=True
                )
                return ExecuteAnswer(ANSWER_TYPE.OK)
        elif type(statement) == Update:
            raise ErNotSupportedYet('Update is not implemented')
        elif type(statement) == Alter and ('disable keys' in sql_lower) or ('enable keys' in sql_lower):
            return ExecuteAnswer(ANSWER_TYPE.OK)
        elif type(statement) == Select:
            if statement.from_table is None:
                return self.answer_single_row_select(statement)

            query = SQLQuery(
                statement,
                session=self.session
            )
            return self.answer_select(query)
        elif type(statement) == Explain:
            return self.answer_show_columns(statement.target)
        elif type(statement) == CreateTable:
            # TODO
            return self.answer_apply_predictor(statement)
        else:
            log.warning(f'Unknown SQL statement: {sql}')
            raise ErNotSupportedYet(f'Unknown SQL statement: {sql}')

    def answer_describe_predictor(self, predictor_value):
        predictor_attr = None
        if isinstance(predictor_value, (list, tuple)):
            predictor_name = predictor_value[0]
            predictor_attr = predictor_value[1]
        else:
            predictor_name = predictor_value
        model_interface = self.session.model_interface
        models = model_interface.get_models()
        if predictor_name not in [x['name'] for x in models]:
            raise ErBadTableError(f"Can't describe predictor. There is no predictor with name '{predictor_name}'")
        description = model_interface.get_model_description(predictor_name)

        if predictor_attr is None:
            columns = [
                Column(name='accuracies', table_name='', type='str'),
                Column(name='column_importances', table_name='', type='str'),
                Column(name='outputs', table_name='', type='str'),
                Column(name='inputs', table_name='', type='str'),
                Column(name='datasource', table_name='', type='str'),
                Column(name='model', table_name='', type='str'),
            ]
            description = [
                description['accuracies'],
                description['column_importances'],
                description['outputs'],
                description['inputs'],
                description['datasource'],
                description['model']
            ]
            data = [description]
        else:
            data = model_interface.get_model_data(predictor_name)
            if predictor_attr == "features":
                data = self._get_features_info(data)
                columns = [{
                    'table_name': '',
                    'name': 'column',
                    'type': TYPES.MYSQL_TYPE_VAR_STRING
                }, {
                    'table_name': '',
                    'name': 'type',
                    'type': TYPES.MYSQL_TYPE_VAR_STRING
                }, {
                    'table_name': '',
                    'name': "encoder",
                    'type': TYPES.MYSQL_TYPE_VAR_STRING
                }, {
                    'table_name': '',
                    'name': 'role',
                    'type': TYPES.MYSQL_TYPE_VAR_STRING
                }]
                columns = [Column(**d) for d in columns]
            elif predictor_attr == "model":
                data = self._get_model_info(data)
                columns = [{
                    'table_name': '',
                    'name': 'name',
                    'type': TYPES.MYSQL_TYPE_VAR_STRING
                }, {
                    'table_name': '',
                    'name': 'performance',
                    'type': TYPES.MYSQL_TYPE_VAR_STRING
                }, {
                    'table_name': '',
                    'name': 'training_time',
                    'type': TYPES.MYSQL_TYPE_VAR_STRING
                }, {
                    'table_name': '',
                    'name': "selected",
                    'type': TYPES.MYSQL_TYPE_VAR_STRING
                }]
                columns = [Column(**d) for d in columns]
            elif predictor_attr == "ensemble":
                data = self._get_ensemble_data(data)
                columns = [
                    Column(name='ensemble', table_name='', type='str')
                ]
            else:
                raise ErNotSupportedYet("DESCRIBE '%s' predictor attribute is not supported yet" % predictor_attr)

        return ExecuteAnswer(
            answer_type=ANSWER_TYPE.TABLE,
            columns=columns,
            data=data
        )

    def answer_retrain_predictor(self, predictor_name):
        model_interface = self.session.model_interface
        models = model_interface.get_models()
        if predictor_name not in [x['name'] for x in models]:
            raise ErBadTableError(f"Can't retrain predictor. There is no predictor with name '{predictor_name}'")
        model_interface.update_model(predictor_name)
        return ExecuteAnswer(ANSWER_TYPE.OK)

    def answer_create_datasource(self, struct: dict):
        ''' create new datasource (integration in old terms)
            Args:
                struct: data for creating integration
        '''
        datasource_name = struct['datasource_name']
        engine = struct['database_type']
        connection_args = struct['connection_args']

        # we have connection checkers not for any db. So do nothing if fail
        # TODO return rich error message

        status = HandlerStatusResponse(success=False)

        try:
            handler = self.session.integration_controller.create_handler(
                handler_type=engine,
                connection_data=connection_args
            )
            status = handler.check_connection()
        except Exception as e:
            status.error_message = str(e)

        if status.success is False:
            raise SqlApiException(f"Can't connect to db: {status.error_message}")

        integration = self.session.integration_controller.get(datasource_name)
        if integration is not None:
            raise SqlApiException(f"Database '{datasource_name}' already exists.")

        self.session.integration_controller.add(datasource_name, engine, connection_args)
        return ExecuteAnswer(ANSWER_TYPE.OK)

    def answer_drop_datasource(self, ds_name):
        try:
            integration = self.session.integration_controller.get(ds_name)
            self.session.integration_controller.delete(integration['name'])
        except Exception:
            raise ErDbDropDelete(f"Something went wrong during deleting of datasource '{ds_name}'.")
        return ExecuteAnswer(answer_type=ANSWER_TYPE.OK)

    def answer_drop_tables(self, statement):
        """ answer on 'drop table [if exists] {name}'
            Args:
                statement: ast
        """
        if statement.if_exists is False:
            for table in statement.tables:
                if len(table.parts) > 1:
                    db_name = table.parts[0]
                else:
                    db_name = self.session.database
                if db_name not in ['files', 'mindsdb']:
                    raise SqlApiException(f"Cannot delete a table from database '{db_name}'")
                table_name = table.parts[-1]
                dn = self.session.datahub[db_name]
                if dn.has_table(table_name) is False:
                    raise SqlApiException(f"Cannot delete a table from database '{db_name}': table does not exists")

        for table in statement.tables:
            if len(table.parts) > 1:
                db_name = table.parts[0]
            else:
                db_name = self.session.database
            if db_name not in ['files', 'mindsdb']:
                raise SqlApiException(f"Cannot delete a table from database '{db_name}'")
            table_name = table.parts[-1]
            dn = self.session.datahub[db_name]
            if dn.has_table(table_name):
                if db_name == 'mindsdb':
                    self.session.datahub['mindsdb'].delete_predictor(table_name)
                elif db_name == 'files':
                    self.session.datahub['files'].query(
                        DropTables(tables=[Identifier(table_name)])
                    )
        return ExecuteAnswer(ANSWER_TYPE.OK)

    def answer_create_view(self, statement):
        name = statement.name
        query = str(statement.query_str)
        datasource_name = None
        if statement.from_table is not None:
            datasource_name = statement.from_table.parts[-1]

        self.session.view_interface.add(name, query, datasource_name)
        return ExecuteAnswer(answer_type=ANSWER_TYPE.OK)

    def answer_create_predictor(self, statement):
        integration_name = None
        struct = {
            'predictor_name': statement.name.parts[-1],
            'select': statement.query_str,
            'predict': [x.parts[-1] for x in statement.targets]
        }

        if len(struct['predict']) > 1:
            raise SqlApiException("Only one field can be in 'PREDICT'")
        if isinstance(statement.integration_name, Identifier):
            struct['integration_name'] = statement.integration_name.parts[-1]
        if statement.using is not None:
            struct['using'] = statement.using
        if statement.datasource_name is not None:
            struct['datasource_name'] = statement.datasource_name.parts[-1]
        if statement.order_by is not None:
            struct['order_by'] = [x.field.parts[-1] for x in statement.order_by]
            if len(struct['order_by']) > 1:
                raise SqlApiException("Only one field can be in 'OPRDER BY'")
        if statement.group_by is not None:
            struct['group_by'] = [x.parts[-1] for x in statement.group_by]
        if statement.window is not None:
            struct['window'] = statement.window
        if statement.horizon is not None:
            struct['horizon'] = statement.horizon

        model_interface = self.session.model_interface

        models = model_interface.get_models()
        model_names = [x['name'] for x in models]
        if struct['predictor_name'] in model_names:
            raise SqlApiException(f"Predictor with name '{struct['predictor_name']}' already exists. Each predictor must have unique name.")

        predictor_name = struct['predictor_name']
        integration_name = struct.get('integration_name')

        integration_id = None
        fetch_data_query = None
        if integration_name is not None:
            handler = self.session.integration_controller.get_handler(integration_name)
            integration_meta = self.session.integration_controller.get(integration_name)
            if integration_meta is None:
                raise SqlApiException(f"Integration '{integration_meta}' does not exists.")
            integration_id = integration_meta.get('id')
            # TODO
            # raise ErBadDbError(f"Unknown datasource: {integration_name}")
            result = handler.native_query(struct['select'])
            fetch_data_query = struct['select']

            if result.type != RESPONSE_TYPE.TABLE:
                raise Exception(f'Error during query: {result.error_message}')

            ds_data_df = result.data_frame
            ds_column_names = list(ds_data_df.columns)

            predict = self._check_predict_columns(struct['predict'], ds_column_names)

            for i, p in enumerate(predict):
                predict[i] = get_column_in_case(ds_column_names, p)
        else:
            predict = struct['predict']

        timeseries_settings = {}
        for w in ['order_by', 'group_by', 'window', 'horizon']:
            if w in struct:
                timeseries_settings[w] = struct.get(w)

        kwargs = struct.get('using', {})
        if len(timeseries_settings) > 0:
            if 'timeseries_settings' not in kwargs:
                kwargs['timeseries_settings'] = timeseries_settings
            else:
                if isinstance(kwargs.get('timeseries_settings'), str):
                    kwargs['timeseries_settings'] = json.loads(kwargs['timeseries_settings'])
                kwargs['timeseries_settings'].update(timeseries_settings)

        # Cast all column names to same case
        if isinstance(kwargs.get('timeseries_settings'), dict):
            order_by = kwargs['timeseries_settings'].get('order_by')
            if order_by is not None:
                for i, col in enumerate(order_by):
                    new_name = get_column_in_case(ds_column_names, col)
                    if new_name is None:
                        raise ErSqlWrongArguments(
                            f'Cant get appropriate cast column case. Columns: {ds_column_names}, column: {col}'
                        )
                    kwargs['timeseries_settings']['order_by'][i] = new_name
            group_by = kwargs['timeseries_settings'].get('group_by')
            if group_by is not None:
                for i, col in enumerate(group_by):
                    new_name = get_column_in_case(ds_column_names, col)
                    kwargs['timeseries_settings']['group_by'][i] = new_name
                    if new_name is None:
                        raise ErSqlWrongArguments(
                            f'Cant get appropriate cast column case. Columns: {ds_column_names}, column: {col}'
                        )

        model_interface.learn(
            predictor_name, ds_data_df, predict, integration_id=integration_id,
            fetch_data_query=fetch_data_query, kwargs=kwargs, user_class=self.session.user_class
        )

        return ExecuteAnswer(ANSWER_TYPE.OK)

    def delete_predictor_query(self, query):

        query2 = Select(targets=[Identifier('name')],
                        from_table=query.table,
                        where=query.where)
        # fake_sql = sql.strip(' ')
        # fake_sql = 'select name ' + fake_sql[len('delete '):]
        sqlquery = SQLQuery(
            query2.to_string(),
            session=self.session
        )

        result = sqlquery.fetch(
            self.session.datahub
        )

        predictors_names = [x[0] for x in result['result']]

        if len(predictors_names) == 0:
            raise SqlApiException('nothing to delete')

        for predictor_name in predictors_names:
            self.session.datahub['mindsdb'].delete_predictor(predictor_name)

    def handle_custom_command(self, command):
        command = command.strip(' ;').split()

        if command[0].lower() == 'delete' and command[1].lower() == 'predictor':
            if len(command) != 3:
                raise ErSqlSyntaxError("wrong syntax of 'DELETE PREDICTOR {NAME}' command")

            predictor_name = command[2]
            self.delete_predictor_query(parse_sql(
                f"delete from mindsdb.predictors where name = '{predictor_name}'",
                'mindsdb'
            ))
            return ExecuteAnswer(ANSWER_TYPE.OK)

        raise ErSqlSyntaxError("at this moment only 'delete predictor' command supported")

    def process_insert(self, statement):
        db_name = self.session.database
        if len(statement.table.parts) == 2:
            db_name = statement.table.parts[0].lower()
        table_name = statement.table.parts[-1].lower()
        if db_name != 'mindsdb' or table_name != 'predictors':
            raise ErNonInsertableTable("At this moment only insert to 'mindsdb.predictors' is possible")
        column_names = []
        for column_identifier in statement.columns:
            if isinstance(column_identifier, Identifier):
                if len(column_identifier.parts) != 1:
                    raise ErKeyColumnDoesNotExist(f'Incorrect column name: {column_identifier}')
                column_name = column_identifier.parts[0].lower()
                column_names.append(column_name)
            elif isinstance(column_identifier, TableColumn):
                column_names.append(column_identifier.name)
            else:
                raise ErKeyColumnDoesNotExist(f'Incorrect column name: {column_identifier}')
        if len(statement.values) > 1:
            raise SqlApiException('At this moment only 1 row can be inserted.')
        for row in statement.values:
            values = []
            for value in row:
                values.append(value.value)
            insert_dict = dict(zip(column_names, values))
        if table_name == 'commands':
            return self.handle_custom_command(insert_dict['command'])
        elif table_name == 'predictors':
            return self.insert_predictor_answer(insert_dict)

    def answer_show_columns(self, target: Identifier, where: Optional[Operation] = None,
                            like: Optional[str] = None, is_full=False):
        if len(target.parts) > 1:
            db = target.parts[0]
        elif isinstance(self.session.database, str) and len(self.session.database) > 0:
            db = self.session.database
        else:
            db = 'mindsdb'
        table_name = target.parts[-1]

        new_where = BinaryOperation('and', args=[
            BinaryOperation('=', args=[Identifier('TABLE_SCHEMA'), Constant(db)]),
            BinaryOperation('=', args=[Identifier('TABLE_NAME'), Constant(table_name)])
        ])
        if where is not None:
            new_where = BinaryOperation('and', args=[new_where, where])
        if like is not None:
            like = BinaryOperation('like', args=[Identifier('View'), Constant(like)])
            new_where = BinaryOperation('and', args=[new_where, like])

        targets = [
            Identifier('COLUMN_NAME', alias=Identifier('Field')),
            Identifier('COLUMN_TYPE', alias=Identifier('Type')),
            Identifier('IS_NULLABLE', alias=Identifier('Null')),
            Identifier('COLUMN_KEY', alias=Identifier('Key')),
            Identifier('COLUMN_DEFAULT', alias=Identifier('Default')),
            Identifier('EXTRA', alias=Identifier('Extra'))
        ]
        if is_full:
            targets.extend([
                Constant('COLLATION', alias=Identifier('Collation')),
                Constant('PRIVILEGES', alias=Identifier('Privileges')),
                Constant('COMMENT', alias=Identifier('Comment')),
            ])
        new_statement = Select(
            targets=targets,
            from_table=Identifier(parts=['information_schema', 'COLUMNS']),
            where=new_where
        )

        query = SQLQuery(
            new_statement,
            session=self.session
        )
        return self.answer_select(query)

    def answer_single_row_select(self, statement):
        columns = []
        data = []
        for target in statement.targets:
            target_type = type(target)
            if target_type == Variable:
                var_name = target.value
                column_name = f'@@{var_name}'
                column_alias = target.alias or column_name
                result = SERVER_VARIABLES.get(column_name)
                if result is None:
                    log.error(f'Unknown variable: {column_name}')
                    raise Exception(f"Unknown variable '{var_name}'")
                else:
                    result = result[0]
            elif target_type == Function:
                function_name = target.op.lower()
                if function_name == 'connection_id':
                    return self.answer_connection_id()

                functions_results = {
                    # 'connection_id': self.executor.sqlserver.connection_id,
                    'database': self.session.database,
                    'current_user': self.session.username,
                    'user': self.session.username,
                    'version': '8.0.17'
                }

                column_name = f'{target.op}()'
                column_alias = target.alias or column_name
                result = functions_results[function_name]
            elif target_type == Constant:
                result = target.value
                column_name = str(result)
                column_alias = '.'.join(target.alias.parts) if type(target.alias) == Identifier else column_name
            elif target_type == NullConstant:
                result = None
                column_name = 'NULL'
                column_alias = 'NULL'
            elif target_type == Identifier:
                result = '.'.join(target.parts)
                raise Exception(f"Unknown column '{result}'")
            else:
                raise ErSqlWrongArguments(f'Unknown constant type: {target_type}')

            columns.append(
                Column(
                    name=column_name, alias=column_alias,
                    table_name='',
                    type=TYPES.MYSQL_TYPE_VAR_STRING if isinstance(result, str) else TYPES.MYSQL_TYPE_LONG,
                    charset=self.charset_text_type if isinstance(result, str) else CHARSET_NUMBERS['binary']
                )
            )
            data.append(result)

        return ExecuteAnswer(
            answer_type=ANSWER_TYPE.TABLE,
            columns=columns,
            data=[data]
        )

    def answer_show_create_table(self, table):
        columns = [
            Column(table_name='', name='Table', type=TYPES.MYSQL_TYPE_VAR_STRING),
            Column(table_name='', name='Create Table', type=TYPES.MYSQL_TYPE_VAR_STRING),
        ]
        return ExecuteAnswer(
            answer_type=ANSWER_TYPE.TABLE,
            columns=columns,
            data=[[table, f'create table {table} ()']]
        )

    def answer_function_status(self):
        columns = [
            Column(name='Db', alias='Db',
                   table_name='schemata', table_alias='ROUTINES',
                   type='str', database='mysql', charset=self.charset_text_type),
            Column(name='Db', alias='Db',
                   table_name='routines', table_alias='ROUTINES',
                   type='str', database='mysql', charset=self.charset_text_type),
            Column(name='Type', alias='Type',
                   table_name='routines', table_alias='ROUTINES',
                   type='str', database='mysql', charset=CHARSET_NUMBERS['utf8_bin']),
            Column(name='Definer', alias='Definer',
                   table_name='routines', table_alias='ROUTINES',
                   type='str', database='mysql', charset=CHARSET_NUMBERS['utf8_bin']),
            Column(name='Modified', alias='Modified',
                   table_name='routines', table_alias='ROUTINES',
                   type=TYPES.MYSQL_TYPE_TIMESTAMP, database='mysql',
                   charset=CHARSET_NUMBERS['binary']),
            Column(name='Created', alias='Created',
                   table_name='routines', table_alias='ROUTINES',
                   type=TYPES.MYSQL_TYPE_TIMESTAMP, database='mysql',
                   charset=CHARSET_NUMBERS['binary']),
            Column(name='Security_type', alias='Security_type',
                   table_name='routines', table_alias='ROUTINES',
                   type=TYPES.MYSQL_TYPE_STRING, database='mysql',
                   charset=CHARSET_NUMBERS['utf8_bin']),
            Column(name='Comment', alias='Comment',
                   table_name='routines', table_alias='ROUTINES',
                   type=TYPES.MYSQL_TYPE_BLOB, database='mysql',
                   charset=CHARSET_NUMBERS['utf8_bin']),
            Column(name='character_set_client', alias='character_set_client',
                   table_name='character_sets', table_alias='ROUTINES',
                   type=TYPES.MYSQL_TYPE_VAR_STRING, database='mysql',
                   charset=self.charset_text_type),
            Column(name='collation_connection', alias='collation_connection',
                   table_name='collations', table_alias='ROUTINES',
                   type=TYPES.MYSQL_TYPE_VAR_STRING, database='mysql',
                   charset=self.charset_text_type),
            Column(name='Database Collation', alias='Database Collation',
                   table_name='collations', table_alias='ROUTINES',
                   type=TYPES.MYSQL_TYPE_VAR_STRING, database='mysql',
                   charset=self.charset_text_type)
        ]

        return ExecuteAnswer(
            answer_type=ANSWER_TYPE.TABLE,
            columns=columns,
            data=[]
        )

    def answer_show_table_status(self, table_name):
        # NOTE at this moment parsed statement only like `SHOW TABLE STATUS LIKE 'table'`.
        # NOTE some columns has {'database': 'mysql'}, other not. That correct. This is how real DB sends messages.
        columns = [{
            'database': 'mysql',
            'table_name': 'tables',
            'name': 'Name',
            'alias': 'Name',
            'type': TYPES.MYSQL_TYPE_VAR_STRING,
            'charset': self.charset_text_type
        }, {
            'database': '',
            'table_name': 'tables',
            'name': 'Engine',
            'alias': 'Engine',
            'type': TYPES.MYSQL_TYPE_VAR_STRING,
            'charset': self.charset_text_type
        }, {
            'database': '',
            'table_name': 'tables',
            'name': 'Version',
            'alias': 'Version',
            'type': TYPES.MYSQL_TYPE_LONGLONG,
            'charset': CHARSET_NUMBERS['binary']
        }, {
            'database': 'mysql',
            'table_name': 'tables',
            'name': 'Row_format',
            'alias': 'Row_format',
            'type': TYPES.MYSQL_TYPE_VAR_STRING,
            'charset': self.charset_text_type
        }, {
            'database': '',
            'table_name': 'tables',
            'name': 'Rows',
            'alias': 'Rows',
            'type': TYPES.MYSQL_TYPE_LONGLONG,
            'charset': CHARSET_NUMBERS['binary']
        }, {
            'database': '',
            'table_name': 'tables',
            'name': 'Avg_row_length',
            'alias': 'Avg_row_length',
            'type': TYPES.MYSQL_TYPE_LONGLONG,
            'charset': CHARSET_NUMBERS['binary']
        }, {
            'database': '',
            'table_name': 'tables',
            'name': 'Data_length',
            'alias': 'Data_length',
            'type': TYPES.MYSQL_TYPE_LONGLONG,
            'charset': CHARSET_NUMBERS['binary']
        }, {
            'database': '',
            'table_name': 'tables',
            'name': 'Max_data_length',
            'alias': 'Max_data_length',
            'type': TYPES.MYSQL_TYPE_LONGLONG,
            'charset': CHARSET_NUMBERS['binary']
        }, {
            'database': '',
            'table_name': 'tables',
            'name': 'Index_length',
            'alias': 'Index_length',
            'type': TYPES.MYSQL_TYPE_LONGLONG,
            'charset': CHARSET_NUMBERS['binary']
        }, {
            'database': '',
            'table_name': 'tables',
            'name': 'Data_free',
            'alias': 'Data_free',
            'type': TYPES.MYSQL_TYPE_LONGLONG,
            'charset': CHARSET_NUMBERS['binary']
        }, {
            'database': '',
            'table_name': 'tables',
            'name': 'Auto_increment',
            'alias': 'Auto_increment',
            'type': TYPES.MYSQL_TYPE_LONGLONG,
            'charset': CHARSET_NUMBERS['binary']
        }, {
            'database': '',
            'table_name': 'tables',
            'name': 'Create_time',
            'alias': 'Create_time',
            'type': TYPES.MYSQL_TYPE_TIMESTAMP,
            'charset': CHARSET_NUMBERS['binary']
        }, {
            'database': '',
            'table_name': 'tables',
            'name': 'Update_time',
            'alias': 'Update_time',
            'type': TYPES.MYSQL_TYPE_TIMESTAMP,
            'charset': CHARSET_NUMBERS['binary']
        }, {
            'database': '',
            'table_name': 'tables',
            'name': 'Check_time',
            'alias': 'Check_time',
            'type': TYPES.MYSQL_TYPE_TIMESTAMP,
            'charset': CHARSET_NUMBERS['binary']
        }, {
            'database': 'mysql',
            'table_name': 'tables',
            'name': 'Collation',
            'alias': 'Collation',
            'type': TYPES.MYSQL_TYPE_VAR_STRING,
            'charset': self.charset_text_type
        }, {
            'database': '',
            'table_name': 'tables',
            'name': 'Checksum',
            'alias': 'Checksum',
            'type': TYPES.MYSQL_TYPE_LONGLONG,
            'charset': CHARSET_NUMBERS['binary']
        }, {
            'database': '',
            'table_name': 'tables',
            'name': 'Create_options',
            'alias': 'Create_options',
            'type': TYPES.MYSQL_TYPE_VAR_STRING,
            'charset': self.charset_text_type
        }, {
            'database': '',
            'table_name': 'tables',
            'name': 'Comment',
            'alias': 'Comment',
            'type': TYPES.MYSQL_TYPE_BLOB,
            'charset': self.charset_text_type
        }]
        columns = [Column(**d) for d in columns]
        data = [[
            table_name,     # Name
            'InnoDB',       # Engine
            10,             # Version
            'Dynamic',      # Row_format
            1,              # Rows
            16384,          # Avg_row_length
            16384,          # Data_length
            0,              # Max_data_length
            0,              # Index_length
            0,              # Data_free
            None,           # Auto_increment
            datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),  # Create_time
            datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),  # Update_time
            None,           # Check_time
            'utf8mb4_0900_ai_ci',   # Collation
            None,           # Checksum
            '',             # Create_options
            ''              # Comment
        ]]
        return ExecuteAnswer(
            answer_type=ANSWER_TYPE.TABLE,
            columns=columns,
            data=data
        )

    def answer_show_warnings(self):
        columns = [{
            'database': '',
            'table_name': '',
            'name': 'Level',
            'alias': 'Level',
            'type': TYPES.MYSQL_TYPE_VAR_STRING,
            'charset': self.charset_text_type
        }, {
            'database': '',
            'table_name': '',
            'name': 'Code',
            'alias': 'Code',
            'type': TYPES.MYSQL_TYPE_LONG,
            'charset': CHARSET_NUMBERS['binary']
        }, {
            'database': '',
            'table_name': '',
            'name': 'Message',
            'alias': 'Message',
            'type': TYPES.MYSQL_TYPE_VAR_STRING,
            'charset': self.charset_text_type
        }]
        columns = [Column(**d) for d in columns]
        return ExecuteAnswer(
            answer_type=ANSWER_TYPE.TABLE,
            columns=columns,
            data=[]
        )

    def answer_connection_id(self):
        columns = [{
            'database': '',
            'table_name': '',
            'name': 'conn_id',
            'alias': 'conn_id',
            'type': TYPES.MYSQL_TYPE_LONG,
            'charset': CHARSET_NUMBERS['binary']
        }]
        columns = [Column(**d) for d in columns]
        data = [[self.executor.sqlserver.connection_id]]
        return ExecuteAnswer(
            answer_type=ANSWER_TYPE.TABLE,
            columns=columns,
            data=data
        )

    def answer_apply_predictor(self, statement):
        SQLQuery(
            statement,
            session=self.session,
            execute=True
        )
        return ExecuteAnswer(ANSWER_TYPE.OK)

    def answer_select(self, query):
        query.fetch(
            self.session.datahub
        )

        return ExecuteAnswer(
            answer_type=ANSWER_TYPE.TABLE,
            columns=query.columns_list,
            data=query.result,
        )

    def is_db_exists(self, db_name):
        sql_statement = Select(
            targets=[Identifier(parts=["schema_name"], alias=Identifier('Database'))],
            from_table=Identifier(parts=['information_schema', 'SCHEMATA']),
            where=BinaryOperation('=', args=[Identifier('schema_name'), Constant(db_name)])
        )
        query = SQLQuery(
            sql_statement,
            session=self.session
        )
        result = query.fetch(
            self.session.datahub
        )
        if result.get('success') is True and len(result.get('result')) > 0:
            return True
        return False

    def change_default_db(self, db_name):
        # That fix for bug in mssql: it keeps connection for a long time, but after some time mssql can
        # send packet with COM_INIT_DB=null. In this case keep old database name as default.
        if db_name != 'null':
            if self.is_db_exists(db_name):
                self.session.database = db_name
            else:
                raise ErBadDbError(f"Database {db_name} does not exists")

    def insert_predictor_answer(self, insert):
        ''' Start learn new predictor.
            Parameters:
             - insert - dict with keys as columns of mindsb.predictors table.
        '''
        model_interface = self.session.model_interface
        integration_controller = self.session.integration_controller

        select_data_query = insert.get('select_data_query')
        if isinstance(select_data_query, str) is False or len(select_data_query) == 0:
            raise ErSqlWrongArguments("'select_data_query' should not be empty")

        models = model_interface.get_models()
        if insert['name'] in [x['name'] for x in models]:
            raise ErSqlWrongArguments(f"predictor with name '{insert['name']}'' already exists")

        kwargs = {}
        if isinstance(insert.get('training_options'), str) \
                and len(insert['training_options']) > 0:
            try:
                kwargs = json.loads(insert['training_options'])
            except json.JSONDecodeError:
                raise ErSqlWrongArguments('training_options should be in valid JSON string')

        integration = self.session.integration
        if isinstance(integration, str) is False or len(integration) == 0:
            raise ErSqlWrongArguments('select_data_query can be used only in query from database')

        insert['select_data_query'] = insert['select_data_query'].replace(r"\'", "'")

        integration_handler = integration_controller.get_handler(integration)
        result = integration_handler.native_query(insert['select_data_query'])
        ds_data_df = result['data_frame']
        ds_column_names = list(ds_data_df.columns)

        insert['predict'] = [x.strip() for x in insert['predict'].split(',')]

        for col in insert['predict']:
            if col not in ds_column_names:
                raise ErKeyColumnDoesNotExist(f"Column '{col}' not exists")

        insert['predict'] = self._check_predict_columns(insert['predict'], ds_column_names)

        integration_meta = integration_controller.get(integration)
        integration_id = integration_meta.get('id')
        fetch_data_query = insert['select_data_query']

        model_interface.learn(
            insert['name'], ds_data_df, insert['predict'], integration_id=integration_id,
            fetch_data_query=fetch_data_query, kwargs=kwargs, user_class=self.session.user_class
        )

        return ExecuteAnswer(ANSWER_TYPE.OK)

    def _check_predict_columns(self, predict_column_names, ds_column_names):
        ''' validate 'predict' column names

            predict_column_names: list of 'predict' columns
            ds_column_names: list of all datasource columns
        '''
        cleaned_predict_column_names = []
        for predict_column_name in predict_column_names:
            candidate = None
            for column_name in ds_column_names:
                if column_name == predict_column_name:
                    if candidate is not None:
                        raise ErKeyColumnDoesNotExist("It is not possible to determine appropriate column name for 'predict' column: {predict_column_name}")
                    candidate = column_name
            if candidate is None:
                for column_name in ds_column_names:
                    if column_name.lower() == predict_column_name.lower():
                        if candidate is not None:
                            raise ErKeyColumnDoesNotExist("It is not possible to determine appropriate column name for 'predict' column: {predict_column_name}")
                        candidate = column_name
            if candidate is None:
                raise ErKeyColumnDoesNotExist(f"Datasource has not column with name '{predict_column_name}'")
            cleaned_predict_column_names.append(candidate)

        if len(cleaned_predict_column_names) != len(set(cleaned_predict_column_names)):
            raise ErDubFieldName("'predict' column name is duplicated")

        return cleaned_predict_column_names

    def _get_features_info(self, data):
        ai_info = data.get('json_ai', {})
        if ai_info == {}:
            raise ErBadTableError("predictor doesn't contain enough data to generate 'feature' attribute.")
        data = []
        dtype_dict = ai_info["dtype_dict"]
        for column in dtype_dict:
            c_data = []
            c_data.append(column)
            c_data.append(dtype_dict[column])
            c_data.append(ai_info["encoders"][column]["module"])
            if ai_info["encoders"][column]["args"].get("is_target", "False") == "True":
                c_data.append("target")
            else:
                c_data.append("feature")
            data.append(c_data)
        return data

    def _get_model_info(self, data):
        models_data = data.get("submodel_data", [])
        if models_data == []:
            raise ErBadTableError("predictor doesn't contain enough data to generate 'model' attribute")
        data = []
        for model in models_data:
            m_data = []
            m_data.append(model["name"])
            m_data.append(model["accuracy"])
            m_data.append(model.get("training_time", "unknown"))
            m_data.append(1 if model["is_best"] else 0)
            data.append(m_data)
        return data

    def _get_ensemble_data(self, data):
        ai_info = data.get('json_ai', {})
        if ai_info == {}:
            raise ErBadTableError("predictor doesn't contain enough data to generate 'ensamble' attribute. Please wait until predictor is complete.")
        ai_info_str = json.dumps(ai_info, indent=2)
        return [[ai_info_str]]
