"""
*******************************************************
 * Copyright (C) 2017 MindsDB Inc. <copyright@mindsdb.com>
 *
 * This file is part of MindsDB Server.
 *
 * MindsDB Server can not be copied and/or distributed without the express
 * permission of MindsDB Inc
 *******************************************************
"""


import os
import sys
import socketserver as SocketServer
import ssl
import traceback
import json
import atexit
import tempfile
import datetime
import socket
import struct
from collections import OrderedDict
from functools import partial
import select
import base64

import pandas as pd
from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast import (
    RollbackTransaction,
    CommitTransaction,
    StartTransaction,
    BinaryOperation,
    Identifier,
    Parameter,
    Constant,
    Function,
    Explain,
    Insert,
    Select,
    Star,
    Show,
    Set,
    Use
)
from mindsdb_sql.parser.dialects.mysql import Variable
from mindsdb_sql.parser.dialects.mindsdb import DropPredictor, DropDatasource, CreateDatasource

from mindsdb.api.mysql.mysql_proxy.utilities.sql import query_df
from mindsdb.utilities.wizards import make_ssl_cert
from mindsdb.utilities.config import Config
from mindsdb.api.mysql.mysql_proxy.data_types.mysql_packet import Packet
from mindsdb.api.mysql.mysql_proxy.controllers.session_controller import SessionController
from mindsdb.api.mysql.mysql_proxy.classes.client_capabilities import ClentCapabilities
from mindsdb.api.mysql.mysql_proxy.classes.server_capabilities import server_capabilities
from mindsdb.api.mysql.mysql_proxy.classes.sql_statement_parser import SqlStatementParser, SQL_PARAMETER, SQL_DEFAULT
from mindsdb.api.mysql.mysql_proxy.utilities import log
from mindsdb.api.mysql.mysql_proxy.external_libs.mysql_scramble import scramble as scramble_func
from mindsdb.api.mysql.mysql_proxy.classes.sql_query import (
    SQLQuery,
    NotImplementedError,
    SqlError
)

from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import (
    getConstName,
    CHARSET_NUMBERS,
    ERR,
    COMMANDS,
    TYPES,
    SERVER_VARIABLES,
    DEFAULT_AUTH_METHOD,
    SERVER_STATUS,
    FIELD_FLAG,
    CAPABILITIES
)

from mindsdb.api.mysql.mysql_proxy.data_types.mysql_packets import (
    ErrPacket,
    HandshakePacket,
    FastAuthFail,
    PasswordAnswer,
    HandshakeResponsePacket,
    OkPacket,
    SwitchOutPacket,
    SwitchOutResponse,
    CommandPacket,
    ColumnCountPacket,
    ColumnDefenitionPacket,
    ResultsetRowPacket,
    EofPacket,
    STMTPrepareHeaderPacket,
    BinaryResultsetRowPacket
)

from mindsdb.interfaces.datastore.datastore import DataStore
from mindsdb.interfaces.model.model_interface import ModelInterface
from mindsdb.interfaces.database.integrations import DatasourceController

connection_id = 0


def empty_fn():
    pass


def check_auth(username, password, scramble_func, salt, company_id, config):
    '''
    '''
    try:
        hardcoded_user = config['api']['mysql']['user']
        hardcoded_password = config['api']['mysql']['password']
        hardcoded_password_hash = scramble_func(hardcoded_password, salt)
        hardcoded_password = hardcoded_password.encode()

        if password is None:
            password = ''
        if isinstance(password, str):
            password = password.encode()

        integration = None
        integration_type = None
        extracted_username = username
        integrations_names = DatasourceController().get_db_integrations(company_id).keys()
        for integration_name in integrations_names:
            if username == f'{hardcoded_user}_{integration_name}':
                extracted_username = hardcoded_user
                integration = integration_name
                integration_type = DatasourceController().get_db_integration(integration, company_id)['type']

        if extracted_username != hardcoded_user:
            log.warning(f'Check auth, user={username}: user mismatch')
            return {
                'success': False
            }

        if password != hardcoded_password and password != hardcoded_password_hash:
            log.warning(f'check auth, user={username}: password mismatch')
            return {
                'success': False
            }

        log.info(f'Check auth, user={username}: Ok')
        return {
            'success': True,
            'username': extracted_username,
            'integration': integration,
            'integration_type': integration_type
        }
    except Exception as e:
        log.error(f'Check auth, user={username}: ERROR')
        log.error(e)
        log.error(traceback.format_exc())


class MysqlProxy(SocketServer.BaseRequestHandler):
    """
    The Main Server controller class
    """

    @staticmethod
    def server_close(srv):
        srv.server_close()

    def __init__(self, request, client_address, server):
        self.charset = 'utf8'
        self.charset_text_type = CHARSET_NUMBERS['utf8_general_ci']
        self.session = None
        self.client_capabilities = None
        super().__init__(request, client_address, server)

    def init_session(self, company_id=None):
        global connection_id
        log.debug('New connection [{ip}:{port}]'.format(
            ip=self.client_address[0], port=self.client_address[1]))
        log.debug(self.__dict__)

        if self.server.connection_id >= 65025:
            self.server.connection_id = 0
        self.server.connection_id += 1
        self.connection_id = self.server.connection_id
        self.session = SessionController(
            server=self.server,
            company_id=company_id
        )

        if hasattr(self.server, 'salt') and isinstance(self.server.salt, str):
            self.salt = self.server.salt
        else:
            self.salt = base64.b64encode(os.urandom(15)).decode()

        self.socket = self.request
        self.logging = log

        self.current_transaction = None

        log.debug('session salt: {salt}'.format(salt=self.salt))

    def handshake(self):
        def switch_auth(method='mysql_native_password'):
            self.packet(SwitchOutPacket, seed=self.salt, method=method).send()
            switch_out_answer = self.packet(SwitchOutResponse)
            switch_out_answer.get()
            password = switch_out_answer.password
            if method == 'mysql_native_password' and len(password) == 0:
                password = scramble_func('', self.salt)
            return password

        def get_fast_auth_password():
            log.debug('Asking for fast auth password')
            self.packet(FastAuthFail).send()
            password_answer = self.packet(PasswordAnswer)
            password_answer.get()
            try:
                password = password_answer.password.value.decode()
            except Exception:
                log.warning('error: no password in Fast Auth answer')
                self.packet(ErrPacket, err_code=ERR.ER_PASSWORD_NO_MATCH, msg='Is not password in connection query.').send()
                return None
            return password

        username = None
        password = None

        log.debug('send HandshakePacket')
        self.packet(HandshakePacket).send()

        handshake_resp = self.packet(HandshakeResponsePacket)
        handshake_resp.get()
        if handshake_resp.length == 0:
            log.warning('HandshakeResponsePacket empty')
            self.packet(OkPacket).send()
            return False
        self.client_capabilities = ClentCapabilities(handshake_resp.capabilities.value)

        client_auth_plugin = handshake_resp.client_auth_plugin.value.decode()

        self.session.is_ssl = False

        if handshake_resp.type == 'SSLRequest':
            log.debug('switch to SSL')
            self.session.is_ssl = True

            ssl_context = ssl.SSLContext()
            ssl_context.load_cert_chain(self.server.cert_path)
            ssl_socket = ssl_context.wrap_socket(
                self.socket,
                server_side=True,
                do_handshake_on_connect=True
            )

            self.socket = ssl_socket
            handshake_resp = self.packet(HandshakeResponsePacket)
            handshake_resp.get()
            client_auth_plugin = handshake_resp.client_auth_plugin.value.decode()

        username = handshake_resp.username.value.decode()

        if client_auth_plugin != DEFAULT_AUTH_METHOD:
            if client_auth_plugin == 'mysql_native_password':
                password = switch_auth('mysql_native_password')
            else:
                new_method = 'caching_sha2_password' if client_auth_plugin == 'caching_sha2_password' else 'mysql_native_password'

                if new_method == 'caching_sha2_password' and self.session.is_ssl is False:
                    log.warning(f'Check auth, user={username}, ssl={self.session.is_ssl}, auth_method={client_auth_plugin}: '
                                'error: cant switch to caching_sha2_password without SSL')
                    self.packet(ErrPacket, err_code=ERR.ER_PASSWORD_NO_MATCH, msg='caching_sha2_password without SSL not supported').send()
                    return False

                log.debug(f'Check auth, user={username}, ssl={self.session.is_ssl}, auth_method={client_auth_plugin}: '
                          f'switch auth method to {new_method}')
                password = switch_auth(new_method)

                if new_method == 'caching_sha2_password':
                    if password == b'\x00':
                        password = ''
                    else:
                        password = get_fast_auth_password()
        elif 'caching_sha2_password' in client_auth_plugin:
            log.debug(
                f'Check auth, user={username}, ssl={self.session.is_ssl}, auth_method={client_auth_plugin}: '
                'check auth using caching_sha2_password'
            )
            password = handshake_resp.enc_password.value
            if password == b'\x00':
                password = ''
            else:
                # FIXME https://github.com/mindsdb/mindsdb/issues/1374
                # if self.session.is_ssl:
                #     password = get_fast_auth_password()
                # else:
                password = switch_auth()
        elif 'mysql_native_password' in client_auth_plugin:
            log.debug(f'Check auth, user={username}, ssl={self.session.is_ssl}, auth_method={client_auth_plugin}: '
                      'check auth using mysql_native_password')
            password = handshake_resp.enc_password.value
        else:
            log.debug(f'Check auth, user={username}, ssl={self.session.is_ssl}, auth_method={client_auth_plugin}: '
                      'unknown method, possible ERROR. Try to switch to mysql_native_password')
            password = switch_auth('mysql_native_password')

        try:
            self.session.database = handshake_resp.database.value.decode()
        except Exception:
            self.session.database = None
        log.debug(f'Check auth, user={username}, ssl={self.session.is_ssl}, auth_method={client_auth_plugin}: '
                  f'connecting to database {self.session.database}')

        auth_data = self.server.check_auth(username, password, scramble_func, self.salt, self.session.company_id)
        if auth_data['success']:
            self.session.username = auth_data['username']
            self.session.auth = True
            self.session.integration = auth_data['integration']
            self.session.integration_type = auth_data['integration_type']
            self.packet(OkPacket).send()
            return True
        else:
            self.packet(ErrPacket, err_code=ERR.ER_PASSWORD_NO_MATCH, msg=f'Access denied for user {username}').send()
            log.warning(f'Access denied for user {username}')
            return False

    def send_package_group(self, packages):
        string = b''.join([x.accum() for x in packages])
        self.socket.sendall(string)

    def insert_predictor_answer(self, insert):
        ''' Start learn new predictor.
            Parameters:
             - insert - dict with keys as columns of mindsb.predictors table.
        '''
        model_interface = self.session.model_interface
        data_store = self.session.data_store

        for key in insert.keys():
            if insert[key] is SQL_DEFAULT:
                insert[key] = None  # all default values at this moment is null (None)

        select_data_query = insert.get('select_data_query')
        if isinstance(select_data_query, str) is False or len(select_data_query) == 0:
            self.packet(
                ErrPacket,
                err_code=ERR.ER_WRONG_ARGUMENTS,
                msg="'select_data_query' should not be empty"
            ).send()
            return

        models = model_interface.get_models()
        if insert['name'] in [x['name'] for x in models]:
            self.packet(
                ErrPacket,
                err_code=ERR.ER_WRONG_ARGUMENTS,
                msg=f"predictor with name '{insert['name']}'' already exists"
            ).send()
            return

        kwargs = {}
        if isinstance(insert.get('training_options'), str) \
                and len(insert['training_options']) > 0:
            try:
                kwargs = json.loads(insert['training_options'])
            except Exception:
                self.packet(
                    ErrPacket,
                    err_code=ERR.ER_WRONG_ARGUMENTS,
                    msg='training_options should be in valid JSON string'
                ).send()
                return

        integration = self.session.integration
        if isinstance(integration, str) is False or len(integration) == 0:
            self.packet(
                ErrPacket,
                err_code=ERR.ER_WRONG_ARGUMENTS,
                msg='select_data_query can be used only in query from database'
            ).send()
            return
        insert['select_data_query'] = insert['select_data_query'].replace(r"\'", "'")
        ds_name = data_store.get_vacant_name(insert['name'])
        ds = data_store.save_datasource(ds_name, integration, {'query': insert['select_data_query']})

        insert['predict'] = [x.strip() for x in insert['predict'].split(',')]

        ds_data = data_store.get_datasource(ds_name)
        if ds_data is None:
            raise Exception(f"DataSource '{ds_name}' does not exists")
        ds_columns = [x['name'] for x in ds_data['columns']]
        for col in insert['predict']:
            if col not in ds_columns:
                data_store.delete_datasource(ds_name)
                raise Exception(f"Column '{col}' not exists")

        try:
            insert['predict'] = self._check_predict_columns(insert['predict'], ds_columns)
        except Exception:
            data_store.delete_datasource(ds_name)
            raise

        model_interface.learn(
            insert['name'], ds, insert['predict'], ds_data['id'], kwargs=kwargs, delete_ds_on_fail=True
        )

        self.packet(OkPacket).send()

    def answer_create_ai_table(self, struct):
        ai_table = self.session.ai_table
        model_interface = self.session.model_interface

        table = ai_table.get_ai_table(struct['ai_table_name'])
        if table is not None:
            raise Exception(f"AT Table with name {struct['ai_table_name']} already exists")

        # check predictor exists
        models = model_interface.get_models()
        models_names = [x['name'] for x in models]
        if struct['predictor_name'] not in models_names:
            raise Exception(f"Predictor with name {struct['predictor_name']} not exists")

        # check integration exists
        if self.session.datasource_interface.get_db_integration(struct['integration_name']) is None:
            raise Exception(f"Integration with name {struct['integration_name']} not exists")

        ai_table.add(
            name=struct['ai_table_name'],
            integration_name=struct['integration_name'],
            integration_query=struct['integration_query'],
            query_fields=struct['query_fields'],
            predictor_name=struct['predictor_name'],
            predictor_fields=struct['predictor_fields']
        )

        self.packet(OkPacket).send()

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
                        raise Exception("It is not possible to determine appropriate column name for 'predict' column: {predict_column_name}")
                    candidate = column_name
            if candidate is None:
                for column_name in ds_column_names:
                    if column_name.lower() == predict_column_name.lower():
                        if candidate is not None:
                            raise Exception("It is not possible to determine appropriate column name for 'predict' column: {predict_column_name}")
                        candidate = column_name
            if candidate is None:
                raise Exception(f"Datasource has not column with name '{predict_column_name}'")
            cleaned_predict_column_names.append(candidate)

        if len(cleaned_predict_column_names) != len(set(cleaned_predict_column_names)):
            raise Exception("'predict' column name is duplicated")

        return cleaned_predict_column_names

    def answer_describe_predictor(self, predictor_name):
        model_interface = self.session.model_interface
        models = model_interface.get_models()
        if predictor_name not in [x['name'] for x in models]:
            raise Exception(f"Can't describe predictor. There is no predictor with name '{predictor_name}'")
        description = model_interface.get_model_description(predictor_name)
        description = [
            description['accuracies'],
            description['column_importances'],
            description['outputs'],
            description['inputs'],
            description['datasource'],
            description['model']
        ]
        packages = self.get_tabel_packets(
            columns=[{
                'table_name': '',
                'name': 'accuracies',
                'type': TYPES.MYSQL_TYPE_VAR_STRING
            }, {
                'table_name': '',
                'name': 'column_importances',
                'type': TYPES.MYSQL_TYPE_VAR_STRING
            }, {
                'table_name': '',
                'name': "outputs",
                'type': TYPES.MYSQL_TYPE_VAR_STRING
            }, {
                'table_name': '',
                'name': 'inputs',
                'type': TYPES.MYSQL_TYPE_VAR_STRING
            }, {
                'table_name': '',
                'name': 'datasource',
                'type': TYPES.MYSQL_TYPE_VAR_STRING
            }, {
                'table_name': '',
                'name': 'model',
                'type': TYPES.MYSQL_TYPE_VAR_STRING
            }],
            data=[description]
        )
        packages.append(self.last_packet())
        self.send_package_group(packages)
        return

    def answer_retrain_predictor(self, predictor_name):
        model_interface = self.session.model_interface
        models = model_interface.get_models()
        if predictor_name not in [x['name'] for x in models]:
            raise Exception(f"Can't retrain predictor. There is no predictor with name '{predictor_name}'")
        model_interface.update_model(predictor_name)
        self.packet(OkPacket).send()

    def answer_create_datasource(self, struct: dict):
        ''' create new datasource (integration in old terms)
            Args:
                struct: data for creating integration
        '''
        datasource_name = struct['datasource_name']
        database_type = struct['database_type']
        connection_args = struct['connection_args']
        connection_args['type'] = database_type

        self.session.datasource_interface.add_db_integration(datasource_name, connection_args)
        self.packet(OkPacket).send()

    def answer_drop_datasource(self, ds_name):
        try:
            ds = self.session.datasource_interface.get_db_integration(ds_name)
            self.session.datasource_interface.remove_db_integration(ds['database_name'])
        except Exception:
            raise Exception(f"Something went wrong during deleting of datasource '{ds_name}'.")
        self.packet(OkPacket).send()

    def answer_create_predictor(self, struct):
        model_interface = self.session.model_interface
        data_store = self.session.data_store

        predictor_name = struct['predictor_name']
        integration_name = struct['integration_name']

        if integration_name.lower().startswith('datasource.'):
            ds_name = integration_name[integration_name.find('.') + 1:]
            ds = data_store.get_datasource_obj(ds_name, raw=True)
            ds_data = data_store.get_datasource(ds_name)
        else:
            if self.session.datasource_interface.get_db_integration(integration_name) is None:
                raise Exception(f"Unknown integration: {integration_name}")

            ds_name = struct.get('datasource_name')
            if ds_name is None:
                ds_name = data_store.get_vacant_name(predictor_name)

            ds = data_store.save_datasource(ds_name, integration_name, {'query': struct['select']})
            ds_data = data_store.get_datasource(ds_name)

        # TODO add alias here
        predict = [x['name'] for x in struct['predict']]

        timeseries_settings = {}
        for w in ['order_by', 'group_by', 'window', 'nr_predictions']:
            if w in struct:
                timeseries_settings[w] = struct.get(w)

        kwargs = struct.get('using', {})
        if len(timeseries_settings) > 0:
            if 'timeseries_settings' not in kwargs:
                kwargs['timeseries_settings'] = timeseries_settings
            else:
                kwargs['timeseries_settings'].update(timeseries_settings)

        ds_column_names = [x['name'] for x in ds_data['columns']]
        try:
            predict = self._check_predict_columns(predict, ds_column_names)
        except Exception:
            data_store.delete_datasource(ds_name)
            raise

        model_interface.learn(predictor_name, ds, predict, ds_data['id'], kwargs=kwargs, delete_ds_on_fail=True)

        self.packet(OkPacket).send()

    def delete_predictor_sql(self, sql):
        fake_sql = sql.strip(' ')
        fake_sql = 'select name ' + fake_sql[len('delete '):]
        query = SQLQuery(
            fake_sql,
            session=self.session
        )

        result = query.fetch(
            self.session.datahub
        )

        if result['success'] is False:
            self.packet(
                ErrPacket,
                err_code=result['error_code'],
                msg=result['msg']
            ).send()
            return

        predictors_names = [x[0] for x in result['result']]

        if len(predictors_names) == 0:
            raise NotImplementedError('nothing to delete')

        for predictor_name in predictors_names:
            self.session.datahub['mindsdb'].delete_predictor(predictor_name)

    def handle_custom_command(self, command):
        command = command.strip(' ;').split()

        if command[0].lower() == 'delete' and command[1].lower() == 'predictor':
            if len(command) != 3:
                self.packet(
                    ErrPacket,
                    err_code=ERR.ER_SYNTAX_ERROR,
                    msg="wrong syntax of 'DELETE PREDICTOR {NAME}' command"
                ).send()
                return
            predictor_name = command[2]
            self.delete_predictor_sql(f"delete from mindsdb.predictors where name = '{predictor_name}'")
            self.packet(OkPacket).send()
            return

        self.packet(
            ErrPacket,
            err_code=ERR.ER_SYNTAX_ERROR,
            msg="at this moment only 'delete predictor' command supported"
        ).send()

    def answer_stmt_prepare(self, statement):
        sql = statement.sql
        sql_lower = sql.lower()
        stmt_id = self.session.register_stmt(statement)
        prepared_stmt = self.session.prepared_stmts[stmt_id]

        if statement.keyword == 'insert':
            prepared_stmt['type'] = 'insert'

            statement = parse_sql(sql, dialect='mindsdb')
            if (
                len(statement.table.parts) > 1 and statement.table.parts[0].lower() != 'mindsdb'
                or len(statement.table.parts) == 1 and self.session.database != 'mindsdb'
            ):
                raise Exception("Only parametrized insert into table from 'mindsdb' database supported at this moment")
            table_name = statement.table.parts[-1]
            if table_name not in ['predictors', 'commands']:
                raise Exception("Only parametrized insert into 'predictors' or 'commands' supported at this moment")

            new_statement = Select(
                targets=statement.columns,
                from_table=Identifier(parts=['mindsdb', table_name]),
                limit=Constant(0)
            )

            query = SQLQuery(
                str(new_statement),
                session=self.session
            )

            num_params = 0
            for row in statement.values:
                for item in row:
                    if isinstance(item, Parameter):
                        num_params = num_params + 1
            num_columns = len(query.columns) - num_params

            if num_columns != 0:
                raise Exception("At this moment supported only insert where all values is parameters.")

            columns_def = []
            for col in query.columns:
                columns_def.append(dict(
                    database='',
                    table_alias='',
                    table_name='',
                    alias='',
                    name='?',
                    type=TYPES.MYSQL_TYPE_VAR_STRING,
                    charset=CHARSET_NUMBERS['binary']
                ))
        elif statement.keyword == 'select' and statement.ends_with('for update'):
            # postgres when execute "delete from mindsdb.predictors where name = 'x'" sends for it prepare statement:
            # 'select name from mindsdb.predictors where name = 'x' FOR UPDATE;'
            # and after it send prepare for delete query.
            prepared_stmt['type'] = 'lock'
            statement.cut_from_tail('for update')
            query = SQLQuery(statement.sql, session=self.session)
            num_columns = len(query.columns)
            num_params = 0
            columns_def = query.columns
            for col in columns_def:
                col['charset'] = CHARSET_NUMBERS['utf8_general_ci']

        elif statement.keyword == 'delete':
            prepared_stmt['type'] = 'delete'

            fake_sql = sql.replace('?', '"?"')
            fake_sql = 'select name ' + fake_sql[len('delete '):]
            query = SQLQuery(fake_sql, session=self.session)
            num_columns = 0
            num_params = sql.count('?')
            columns_def = []
            for i in range(num_params):
                columns_def.append(dict(
                    database='',
                    table_alias='',
                    table_name='',
                    alias='?',
                    name='',
                    type=TYPES.MYSQL_TYPE_VAR_STRING,
                    charset=CHARSET_NUMBERS['utf8_general_ci'],
                    flags=sum([FIELD_FLAG.BINARY_COLLATION])
                ))
        elif statement.keyword == 'select' and 'connection_id()' in sql.lower():
            prepared_stmt['type'] = 'select'
            num_columns = 1
            num_params = 0
            columns_def = [{
                'database': '',
                'table_name': '',
                'name': 'conn_id',
                'alias': 'conn_id',
                'type': TYPES.MYSQL_TYPE_LONG,
                'charset': CHARSET_NUMBERS['binary']
            }]
        elif statement.keyword == 'select':
            prepared_stmt['type'] = 'select'
            query = SQLQuery(sql, session=self.session)
            num_columns = len(query.columns)
            num_params = 0
            columns_def = query.columns
        elif (
            'show variables' in sql_lower
            or 'show session variables' in sql_lower
            or 'show session status' in sql_lower
        ):
            prepared_stmt['type'] = 'show variables'
            num_columns = 2
            num_params = 0
            columns_def = [{
                'table_name': '',
                'name': 'Variable_name',
                'type': TYPES.MYSQL_TYPE_VAR_STRING
            }, {
                'table_name': '',
                'name': 'Value',
                'type': TYPES.MYSQL_TYPE_VAR_STRING
            }]
        else:
            raise SqlError(f"Only 'SELECT' and 'INSERT' statements supported. Got: {sql}")

        packages = [
            self.packet(
                STMTPrepareHeaderPacket,
                stmt_id=stmt_id,
                num_columns=num_columns,
                num_params=num_params
            )
        ]

        packages.extend(
            self._get_column_defenition_packets(columns_def)
        )

        if self.client_capabilities.DEPRECATE_EOF is False:
            status = sum([SERVER_STATUS.SERVER_STATUS_AUTOCOMMIT])
            packages.append(self.packet(EofPacket, status=status))

        self.send_package_group(packages)

    def answer_stmt_execute(self, stmt_id, parameters):
        prepared_stmt = self.session.prepared_stmts[stmt_id]
        if prepared_stmt['type'] == 'select':
            sql = prepared_stmt['statement'].sql

            # +++
            if sql.lower() == 'select connection_id()':
                self.answer_connection_id(sql)
                return
            # ---

            # +++
            if "SELECT `table_name`, `column_name`" in sql:
                # TABLEAU
                # SELECT `table_name`, `column_name`
                # FROM `information_schema`.`columns`
                # WHERE `data_type`='enum' AND `table_schema`='mindsdb'
                packages = self.get_tabel_packets(
                    columns=[{
                        'table_name': '',
                        'name': 'TABLE_NAME',
                        'type': TYPES.MYSQL_TYPE_VAR_STRING
                    }, {
                        'table_name': '',
                        'name': 'COLUMN_NAME',
                        'type': TYPES.MYSQL_TYPE_VAR_STRING
                    }],
                    data=[]
                )
                if self.client_capabilities.DEPRECATE_EOF is True:
                    packages.append(self.packet(OkPacket, eof=True))
                else:
                    packages.append(self.packet(EofPacket))
                self.send_package_group(packages)
                return
            # ---

            query = SQLQuery(sql, session=self.session)

            columns = query.columns
            packages = [self.packet(ColumnCountPacket, count=len(columns))]
            packages.extend(self._get_column_defenition_packets(columns))

            packages.append(self.last_packet(status=0x0062))
            self.send_package_group(packages)
        elif prepared_stmt['type'] == 'insert':
            statement = parse_sql(prepared_stmt['statement'].sql, dialect='mindsdb')
            parameter_index = 0
            for row in statement.values:
                for item_index, item in enumerate(row):
                    if isinstance(item, Parameter):
                        row[item_index] = Constant(parameters[parameter_index])
                        parameter_index += 1
            self.process_insert(statement)
        elif prepared_stmt['type'] == 'lock':
            sql = prepared_stmt['statement'].sql
            query = SQLQuery(sql, session=self.session)

            columns = query.columns
            packages = [self.packet(ColumnCountPacket, count=len(columns))]
            packages.extend(self._get_column_defenition_packets(columns))

            status = sum([
                SERVER_STATUS.SERVER_STATUS_AUTOCOMMIT,
                SERVER_STATUS.SERVER_STATUS_CURSOR_EXISTS,
            ])

            packages.append(self.last_packet(status=status))
            self.send_package_group(packages)
        elif prepared_stmt['type'] == 'delete':
            if len(parameters) == 0:
                raise SqlError("Delete statement must content 'where' filter")
            sql = prepared_stmt['statement'].sql
            sql = sql[:sql.find('?')] + f"'{parameters[0]}'"
            self.delete_predictor_sql(sql)
            self.packet(OkPacket, affected_rows=1).send()
        elif prepared_stmt['type'] == 'show variables':
            sql = prepared_stmt['statement'].sql
            self.answer_show_variables(sql)
        else:
            raise NotImplementedError(f"Unknown statement type: {prepared_stmt['type']}")

    def answer_stmt_fetch(self, stmt_id, limit=100000):
        prepared_stmt = self.session.prepared_stmts[stmt_id]
        sql = prepared_stmt['statement'].sql
        fetched = prepared_stmt['fetched']
        query = SQLQuery(sql, session=self.session)

        result = query.fetch(
            self.session.datahub
        )

        if result['success'] is False:
            self.packet(
                ErrPacket,
                err_code=result['error_code'],
                msg=result['msg']
            ).send()
            return

        packages = []
        columns = query.columns
        for row in query.result[fetched:limit]:
            packages.append(
                self.packet(BinaryResultsetRowPacket, data=row, columns=columns)
            )

        prepared_stmt['fetched'] += len(query.result[fetched:limit])

        if len(query.result) <= limit:
            status = sum([
                SERVER_STATUS.SERVER_STATUS_AUTOCOMMIT,
                SERVER_STATUS.SERVER_STATUS_LAST_ROW_SENT,
            ])
        else:
            status = sum([
                SERVER_STATUS.SERVER_STATUS_AUTOCOMMIT,
                SERVER_STATUS.SERVER_STATUS_CURSOR_EXISTS,
            ])

        packages.append(self.last_packet(status=status))
        self.send_package_group(packages)

    def answer_stmt_close(self, stmt_id):
        self.session.unregister_stmt(stmt_id)

    def answer_explain_table(self, target):
        db = ((self.session.database or 'mindsdb') if len(target) != 2 else target[0]).lower()
        table = target[-1].lower()
        if table == 'predictors' and db == 'mindsdb':
            self.answer_explain_predictors()
        elif table == 'commands' and db == 'mindsdb':
            self.answer_explain_commands()
        else:
            raise NotImplementedError("Only 'EXPLAIN predictors' and 'EXPLAIN commands' supported")

    def _get_explain_columns(self):
        return [{
            'database': 'information_schema',
            'table_name': 'COLUMNS',
            'name': 'COLUMN_NAME',
            'alias': 'Field',
            'type': TYPES.MYSQL_TYPE_VAR_STRING,
            'charset': self.charset_text_type,
            'flags': sum([FIELD_FLAG.NOT_NULL])
        }, {
            'database': 'information_schema',
            'table_name': 'COLUMNS',
            'name': 'COLUMN_TYPE',
            'alias': 'Type',
            'type': TYPES.MYSQL_TYPE_BLOB,
            'charset': self.charset_text_type,
            'flags': sum([
                FIELD_FLAG.NOT_NULL,
                FIELD_FLAG.BLOB
            ])
        }, {
            'database': 'information_schema',
            'table_name': 'COLUMNS',
            'name': 'IS_NULLABLE',
            'alias': 'Null',
            'type': TYPES.MYSQL_TYPE_VAR_STRING,
            'charset': self.charset_text_type,
            'flags': sum([FIELD_FLAG.NOT_NULL])
        }, {
            'database': 'information_schema',
            'table_name': 'COLUMNS',
            'name': 'COLUMN_KEY',
            'alias': 'Key',
            'type': TYPES.MYSQL_TYPE_VAR_STRING,
            'charset': self.charset_text_type,
            'flags': sum([FIELD_FLAG.NOT_NULL])
        }, {
            'database': 'information_schema',
            'table_name': 'COLUMNS',
            'name': 'COLUMN_DEFAULT',
            'alias': 'Default',
            'type': TYPES.MYSQL_TYPE_BLOB,
            'charset': self.charset_text_type,
            'flags': sum([FIELD_FLAG.BLOB])
        }, {
            'database': 'information_schema',
            'table_name': 'COLUMNS',
            'name': 'EXTRA',
            'alias': 'Extra',
            'type': TYPES.MYSQL_TYPE_VAR_STRING,
            'charset': self.charset_text_type,
            'flags': sum([FIELD_FLAG.NOT_NULL])
        }]

    def answer_explain_predictors(self):
        status = sum([
            SERVER_STATUS.SERVER_STATUS_AUTOCOMMIT,
            SERVER_STATUS.SERVER_QUERY_NO_INDEX_USED,
        ])

        packages = self.get_tabel_packets(
            columns=self._get_explain_columns(),
            # [Field, Type, Null, Key, Default, Extra]
            data=[
                ['name', 'varchar(255)', 'NO', 'PRI', None, ''],
                ['status', 'varchar(255)', 'YES', '', None, ''],
                ['accuracy', 'varchar(255)', 'YES', '', None, ''],
                ['predict', 'varchar(255)', 'YES', '', None, ''],
                ['select_data_query', 'varchar(255)', 'YES', '', None, ''],
                ['training_options', 'varchar(255)', 'YES', '', None, ''],
            ],
            status=status
        )

        packages.append(self.last_packet(status=status))
        self.send_package_group(packages)

    def answer_explain_commands(self):
        status = sum([
            SERVER_STATUS.SERVER_STATUS_AUTOCOMMIT,
            SERVER_STATUS.SERVER_QUERY_NO_INDEX_USED,
        ])

        packages = self.get_tabel_packets(
            columns=self._get_explain_columns(),
            data=[
                # [Field, Type, Null, Key, Default, Extra]
                ['command', 'varchar(255)', 'NO', 'PRI', None, '']
            ],
            status=status
        )

        packages.append(self.last_packet(status=status))
        self.send_package_group(packages)

    def process_insert(self, statement):
        db_name = self.session.database
        if len(statement.table.parts) == 2:
            db_name = statement.table.parts[0].lower()
        table_name = statement.table.parts[-1].lower()
        if db_name != 'mindsdb' or table_name not in ('predictors', 'commands'):
            raise Exception("At this moment only insert to 'mindsdb.predictors' or 'mindsdb.commands' is possible")
        column_names = []
        for column_identifier in statement.columns:
            if isinstance(column_identifier, Identifier) is False or len(column_identifier.parts) != 1:
                raise Exception(f'Incorrect column name: {column_identifier}')
            column_name = column_identifier.parts[0].lower()
            column_names.append(column_name)
        if len(statement.values) > 1:
            raise Exception('At this moment only 1 row can be inserted.')
        for row in statement.values:
            values = []
            for value in row:
                values.append(value.value)
            insert_dict = dict(zip(column_names, values))
        if table_name == 'commands':
            self.handle_custom_command(insert_dict['command'])
        elif table_name == 'predictors':
            self.insert_predictor_answer(insert_dict)

    def query_answer(self, sql):
        # +++
        # if query not for mindsdb then process that query in integration db
        # TODO redirect only select data queries
        if (
            isinstance(self.session.database, str)
            and len(self.session.database) > 0
            and self.session.database.lower() != 'mindsdb'
            and '@@' not in sql.lower()
            and (
                (
                    sql.lower().startswith('select')
                    and 'from' in sql.lower()
                )
                or (
                    sql.lower().startswith('show')
                    # and 'databases' in sql.lower()
                    and 'tables' in sql.lower()
                )
            )
        ):
            datanode = self.session.datahub.get(self.session.database)
            if datanode is None:
                raise Exception('datanode is none')
            result, _column_names = datanode.select(sql.replace('`', ''))

            columns = []
            data = []
            if len(result) > 0:
                columns = [{
                    'table_name': '',
                    'name': x,
                    'type': TYPES.MYSQL_TYPE_VAR_STRING
                } for x in result[0].keys()]
                data = [[str(value) for key, value in x.items()] for x in result]

            packages = []
            packages += self.get_tabel_packets(
                columns=columns,
                data=data
            )
            packages.append(self.packet(OkPacket, eof=True))
            self.send_package_group(packages)
            return
        # ---

        statement = SqlStatementParser(sql)
        sql = statement.sql
        sql_lower = sql.lower()
        sql_lower = sql_lower.replace('`', '')

        keyword = statement.keyword
        struct = statement.struct

        try:
            try:
                statement = parse_sql(sql, dialect='mindsdb')
            except Exception:
                statement = parse_sql(sql, dialect='mysql')
        except Exception:
            # not all statemts are parse by parse_sql
            pass

        if isinstance(statement, CreateDatasource):
            struct = {
                'datasource_name': statement.name,
                'database_type': statement.engine,
                'connection_args': statement.parameters
            }
            self.answer_create_datasource(struct)
            return
        if isinstance(statement, DropPredictor):
            predictor_name = statement.name.parts[-1]
            self.session.datahub['mindsdb'].delete_predictor(predictor_name)
            self.packet(OkPacket).send()
        elif keyword == 'create_datasource':
            # fallback for statement
            self.answer_create_datasource(struct)
            return
        elif isinstance(statement, DropDatasource):
            ds_name = statement.name.parts[-1]
            self.answer_drop_datasource(ds_name)
            return
        elif keyword == 'describe':
            self.answer_describe_predictor(struct['predictor_name'])
            return
        elif keyword == 'retrain':
            self.answer_retrain_predictor(struct['predictor_name'])
            return
        elif isinstance(statement, Show) or keyword == 'show':
            sql_category = statement.category.lower()
            condition = statement.condition.lower() if isinstance(statement.condition, str) else statement.condition
            expression = statement.expression
            if sql_category == 'predictors':
                if expression is not None:
                    raise Exception("'SHOW PREDICTORS' query should be used without filters")
                new_statement = Select(
                    targets=[Star()],
                    from_table=Identifier(parts=[self.session.database or 'mindsdb', 'predictors'])
                )
                query = SQLQuery(
                    str(new_statement),
                    session=self.session
                )
                self.answer_select(query)
                return
            if sql_category == 'plugins':
                if expression is not None:
                    raise Exception("'SHOW PLUGINS' query should be used without filters")
                new_statement = Select(
                    targets=[Star()],
                    from_table=Identifier(parts=['information_schema', 'PLUGINS'])
                )
                query = SQLQuery(
                    str(new_statement),
                    session=self.session
                )
                self.answer_select(query)
                return
            elif sql_category in ('databases', 'schemas'):
                new_statement = Select(
                    targets=[Identifier(parts=["schema_name"], alias=Identifier('Database'))],
                    from_table=Identifier(parts=['information_schema', 'SCHEMATA'])
                )
                if condition == 'like':
                    new_statement.where = BinaryOperation('like', args=[Identifier('schema_name'), expression])
                elif condition is not None:
                    raise Exception(f'Not implemented: {sql}')

                query = SQLQuery(
                    str(new_statement),
                    session=self.session
                )
                self.answer_select(query)
                return
            elif sql_category == 'datasources':
                new_statement = Select(
                    targets=[Star()],
                    from_table=Identifier(parts=['mindsdb', 'datasources'])
                )
                query = SQLQuery(
                    str(new_statement),
                    session=self.session
                )
                self.answer_select(query)
                return
            elif sql_category in ('tables', 'full tables'):
                schema = self.session.database or 'mindsdb'
                if condition == 'from':
                    schema = expression.parts[0]
                elif condition is not None:
                    raise Exception(f'Unknown condition in query: {statement}')

                new_statement = Select(
                    targets=[Identifier(parts=['table_name'], alias=Identifier(f'Tables_in_{schema}'))],
                    from_table=Identifier(parts=['information_schema', 'TABLES']),
                    where=BinaryOperation('and', args=[
                        BinaryOperation('=', args=[Identifier('table_schema'), Constant(schema)]),
                        BinaryOperation('like', args=[Identifier('table_type'), Constant('BASE TABLE')])
                    ])
                )

                query = SQLQuery(
                    str(new_statement),
                    session=self.session
                )
                self.answer_select(query)
                return
            elif sql_category in ('variables', 'session variables', 'session status', 'global variables'):
                new_statement = Select(
                    targets=[Identifier(parts=['Variable_name']), Identifier(parts=['Value'])],
                    from_table=Identifier(parts=['dataframe']),
                )

                if condition == 'like':
                    new_statement.where = BinaryOperation('like', args=[Identifier('Variable_name'), expression])
                elif condition == 'where':
                    new_statement.where = expression
                elif condition is not None:
                    raise Exception(f'Unknown condition in query: {statement}')

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

                packages = []
                packages += self.get_tabel_packets(
                    columns=[{
                        'table_name': 'session_variables',
                        'name': 'Variable_name',
                        'type': TYPES.MYSQL_TYPE_VAR_STRING
                    }, {
                        'table_name': 'session_variables',
                        'name': 'Value',
                        'type': TYPES.MYSQL_TYPE_VAR_STRING
                    }],
                    data=data
                )

                packages.append(self.last_packet())
                self.send_package_group(packages)
                return
            elif "show status like 'ssl_version'" in sql_lower:
                packages = []
                packages += self.get_tabel_packets(
                    columns=[{
                        'table_name': 'session_variables',
                        'name': 'Variable_name',
                        'type': TYPES.MYSQL_TYPE_VAR_STRING
                    }, {
                        'table_name': 'session_variables',
                        'name': 'Value',
                        'type': TYPES.MYSQL_TYPE_VAR_STRING
                    }],
                    data=[['Ssl_version', 'TLSv1.1']]   # FIX
                )

                packages.append(self.last_packet())
                self.send_package_group(packages)
                return
            elif sql_category in ('function status', 'procedure status'):
                # SHOW FUNCTION STATUS WHERE Db = 'MINDSDB';
                # SHOW PROCEDURE STATUS WHERE Db = 'MINDSDB'
                # SHOW FUNCTION STATUS WHERE Db = 'MINDSDB' AND Name LIKE '%';
                self.answer_function_status()
                return
            elif 'show index from' in sql_lower:
                # SHOW INDEX FROM `ny_output` FROM `data`;
                self.answer_show_index()
                return
            # FIXME if have answer on that request, then DataGrip show warning '[S0022] Column 'Non_unique' not found.'
            elif 'show create table' in sql_lower:
                # SHOW CREATE TABLE `MINDSDB`.`predictors`
                table = sql[sql.rfind('.') + 1:].strip(' .;\n\t').replace('`', '')
                self.answer_show_create_table(table)
                return
            elif sql_category in ('character set', 'charset'):
                charset = None
                if condition == 'where':
                    if isinstance(expression, BinaryOperation):
                        if expression.op == '=':
                            if isinstance(expression.args[0], Identifier):
                                if expression.args[0].parts[0].lower() == 'charset':
                                    charset = expression.args[1].value
                                else:
                                    raise Exception(
                                        f'Error during processing query: {sql}\n'
                                        f"Only filter by 'charset' supported 'WHERE', but '{expression.args[0].parts[0]}' found"
                                    )
                            else:
                                raise Exception(
                                    f'Error during processing query: {sql}\n'
                                    f"Expected identifier in 'WHERE', but '{expression.args[0]}' found"
                                )
                        else:
                            raise Exception(
                                f'Error during processing query: {sql}\n'
                                f"Expected '=' comparison in 'WHERE', but '{expression.op}' found"
                            )
                    else:
                        raise Exception(
                            f'Error during processing query: {sql}\n'
                            f"Expected binary operation in 'WHERE', but '{expression}' found"
                        )
                elif condition is not None:
                    raise Exception(
                        f'Error during processing query: {sql}\n'
                        f"Only 'WHERE' filter supported, but '{condition}' found"
                    )
                self.answer_show_charset(charset)
                return
            elif sql_category == 'warnings':
                self.answer_show_warnings()
                return
            elif sql_category == 'engines':
                new_statement = Select(
                    targets=[Star()],
                    from_table=Identifier(parts=['information_schema', 'ENGINES'])
                )
                query = SQLQuery(
                    str(new_statement),
                    session=self.session
                )
                self.answer_select(query)
                return
            elif sql_category == 'collation':
                self.answer_show_collation()
                return
            elif sql_category == 'table status':
                # SHOW TABLE STATUS LIKE 'table'
                table_name = None
                if condition == 'like' and isinstance(expression, Constant):
                    table_name = expression.value
                elif condition == 'from' and isinstance(expression, Identifier):
                    table_name = expression.parts[-1]
                if table_name is None:
                    err_str = f"Can't determine table name in query: {sql}"
                    log.warning(err_str)
                    raise Exception(err_str)
                self.answer_show_table_status(table_name)
                return
            else:
                raise Exception(f'Statement not implemented: {sql}')
        elif isinstance(statement, (StartTransaction, CommitTransaction, RollbackTransaction)):
            self.packet(OkPacket).send()
        elif isinstance(statement, Set):
            category = (statement.category or '').lower()
            if category == '' and isinstance(statement.arg, BinaryOperation):
                self.packet(OkPacket).send()
            elif category == 'autocommit':
                self.packet(OkPacket).send()
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
                self.packet(
                    OkPacket,
                    state_track=[
                        ['character_set_client', self.charset],
                        ['character_set_connection', self.charset],
                        ['character_set_results', self.charset]
                    ]
                ).send()
            else:
                log.warning(f'SQL statement is not processable, return OK package: {sql}')
                self.packet(OkPacket).send()
        elif isinstance(statement, Use):
            db_name = statement.value.parts[-1]
            self.change_default_db(db_name)
            self.packet(OkPacket).send()
        elif keyword == 'set':
            log.warning(f'Unknown SET query, return OK package: {sql}')
            self.packet(OkPacket).send()
        elif keyword == 'create_ai_table':
            self.answer_create_ai_table(struct)
        elif keyword == 'create_predictor' or keyword == 'create_table':
            self.answer_create_predictor(struct)
        elif keyword == 'delete' and \
                ('mindsdb.predictors' in sql_lower or self.session.database == 'mindsdb' and 'predictors' in sql_lower):
            self.delete_predictor_sql(sql)
            self.packet(OkPacket).send()
        elif isinstance(statement, Insert):
            self.process_insert(statement)
        elif keyword in ('update', 'insert'):
            raise NotImplementedError('Update and Insert not implemented')
        elif keyword == 'alter' and ('disable keys' in sql_lower) or ('enable keys' in sql_lower):
            self.packet(OkPacket).send()
        elif isinstance(statement, Select):
            if statement.from_table is None:
                self.answer_single_row_select(statement)
                return
            if "table_name,table_comment,if(table_type='base table', 'table', table_type)" in sql_lower:
                # TABLEAU
                # SELECT TABLE_NAME,TABLE_COMMENT,IF(TABLE_TYPE='BASE TABLE', 'TABLE', TABLE_TYPE),TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA LIKE 'mindsdb' AND ( TABLE_TYPE='BASE TABLE' OR TABLE_TYPE='VIEW' )  ORDER BY TABLE_SCHEMA, TABLE_NAME
                # SELECT TABLE_NAME,TABLE_COMMENT,IF(TABLE_TYPE='BASE TABLE', 'TABLE', TABLE_TYPE),TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=DATABASE() AND ( TABLE_TYPE='BASE TABLE' OR TABLE_TYPE='VIEW' )  ORDER BY TABLE_SCHEMA, TABLE_NAME
                packages = []
                if "table_schema like 'mindsdb'" in sql_lower:
                    data = [
                        ['predictors', '', 'TABLE', 'mindsdb']
                    ]
                else:
                    data = []
                packages += self.get_tabel_packets(
                    columns=[{
                        'table_name': '',
                        'name': 'TABLE_NAME',
                        'type': TYPES.MYSQL_TYPE_VAR_STRING
                    }, {
                        'table_name': '',
                        'name': 'TABLE_COMMENT',
                        'type': TYPES.MYSQL_TYPE_VAR_STRING
                    }, {
                        'table_name': '',
                        'name': "IF(TABLE_TYPE='BASE TABLE', 'TABLE', TABLE_TYPE)",
                        'type': TYPES.MYSQL_TYPE_VAR_STRING
                    }, {
                        'table_name': '',
                        'name': 'TABLE_SCHEMA',
                        'type': TYPES.MYSQL_TYPE_VAR_STRING
                    }],
                    data=data
                )

                packages.append(self.last_packet())
                self.send_package_group(packages)
                return

            query = SQLQuery(
                sql,
                session=self.session
            )
            self.answer_select(query)
        elif isinstance(statement, Explain):
            self.answer_explain_table(statement.target.parts)
        else:
            print(sql)
            log.warning(f'Unknown SQL statement: {sql}')
            raise NotImplementedError('Action not implemented')

    def answer_single_row_select(self, statement):
        columns = []
        data = []
        for target in statement.targets:
            if isinstance(target, Variable):
                var_name = target.value
                column_name = f'@@{var_name}'
                column_alias = target.alias or column_name
                result = SERVER_VARIABLES.get(column_name)
                if result is None:
                    log.warning(f'Unknown variable: {column_name}')
                    result = ''
                else:
                    result = result[0]
            elif isinstance(target, Function):
                functions_results = {
                    'connection_id': self.connection_id,
                    'database': self.session.database,
                    'current_user': self.session.username,
                    'user': self.session.username,
                    'version': '8.0.17'
                }
                function_name = target.op.lower()
                column_name = f'{target.op}()'
                column_alias = target.alias or column_name
                result = functions_results[function_name]
            elif isinstance(target, Constant):
                result = target.value
                column_name = str(result)
                column_alias = '.'.join(target.alias.parts) if isinstance(target.alias, Identifier) else column_name
            elif isinstance(target, Identifier):
                result = '.'.join(target.parts)
                column_name = str(result)
                column_alias = '.'.join(target.alias.parts) if isinstance(target.alias, Identifier) else column_name

            columns.append({
                'table_name': '',
                'name': column_name,
                'alias': column_alias,
                'type': TYPES.MYSQL_TYPE_VAR_STRING if isinstance(result, str) else TYPES.MYSQL_TYPE_LONG,
                'charset': self.charset_text_type if isinstance(result, str) else CHARSET_NUMBERS['binary']
            })
            data.append(result)

        packages = self.get_tabel_packets(
            columns=columns,
            data=[data]
        )

        packages.append(self.last_packet())
        self.send_package_group(packages)

    def answer_show_create_table(self, table):
        packages = []
        packages += self.get_tabel_packets(
            columns=[{
                'table_name': '',
                'name': 'Table',
                'type': TYPES.MYSQL_TYPE_VAR_STRING
            }, {
                'table_name': '',
                'name': 'Create Table',
                'type': TYPES.MYSQL_TYPE_VAR_STRING
            }],
            data=[[table, f'create table {table} ()']]
        )

        packages.append(self.last_packet())
        self.send_package_group(packages)

    def answer_show_index(self):
        packages = []
        packages += self.get_tabel_packets(
            columns=[{
                'database': 'mysql',
                'table_name': 'tables',
                'table_alias': 'SHOW_STATISTICS',
                'name': 'Table',
                'alias': 'Table',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': CHARSET_NUMBERS['utf8_bin']
            }, {
                'database': '',
                'table_name': '',
                'table_alias': 'SHOW_STATISTICS',
                'name': 'Non_unique',
                'alias': 'Non_unique',
                'type': TYPES.MYSQL_TYPE_LONG,
                'charset': CHARSET_NUMBERS['binary']
            }, {
                'database': '',
                'table_name': '',
                'table_alias': 'SHOW_STATISTICS',
                'name': 'Key_name',
                'alias': 'Key_name',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }, {
                'database': 'mysql',
                'table_name': 'index_column_usage',
                'table_alias': 'SHOW_STATISTICS',
                'name': 'Seq_in_index',
                'alias': 'Seq_in_index',
                'type': TYPES.MYSQL_TYPE_LONG,
                'charset': CHARSET_NUMBERS['binary']
            }, {
                'database': '',
                'table_name': '',
                'table_alias': 'SHOW_STATISTICS',
                'name': 'Column_name',
                'alias': 'Column_name',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }, {
                'database': '',
                'table_name': '',
                'table_alias': 'SHOW_STATISTICS',
                'name': 'Collation',
                'alias': 'Collation',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }, {
                'database': '',
                'table_name': '',
                'table_alias': 'SHOW_STATISTICS',
                'name': 'Cardinality',
                'alias': 'Cardinality',
                'type': TYPES.MYSQL_TYPE_LONGLONG,
                'charset': CHARSET_NUMBERS['binary']
            }, {
                'database': '',
                'table_name': '',
                'table_alias': 'SHOW_STATISTICS',
                'name': 'Sub_part',
                'alias': 'Sub_part',
                'type': TYPES.MYSQL_TYPE_LONGLONG,
                'charset': CHARSET_NUMBERS['binary']
            }, {
                'database': '',
                'table_name': '',
                'table_alias': '',
                'name': '',
                'alias': 'Packed',
                'type': TYPES.MYSQL_TYPE_NULL,
                'charset': CHARSET_NUMBERS['binary']
            }, {
                'database': '',
                'table_name': '',
                'table_alias': 'SHOW_STATISTICS',
                'name': 'Null',
                'alias': 'Null',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }, {
                'database': '',
                'table_name': '',
                'table_alias': 'SHOW_STATISTICS',
                'name': 'Index_type',
                'alias': 'Index_type',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': CHARSET_NUMBERS['utf8_bin']
            }, {
                'database': '',
                'table_name': '',
                'table_alias': 'SHOW_STATISTICS',
                'name': 'Comment',
                'alias': 'Comment',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }, {
                'database': 'mysql',
                'table_name': 'indexes',
                'table_alias': 'SHOW_STATISTICS',
                'name': 'Index_comment',
                'alias': 'Index_comment',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': CHARSET_NUMBERS['utf8_bin']
            }, {
                'database': 'mysql',
                'table_name': 'indexes',
                'table_alias': 'SHOW_STATISTICS',
                'name': 'Visible',
                'alias': 'Visible',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }, {
                'database': '',
                'table_name': '',
                'table_alias': 'SHOW_STATISTICS',
                'name': 'Expression',
                'alias': 'Expression',
                'type': TYPES.MYSQL_TYPE_BLOB,
                'charset': CHARSET_NUMBERS['utf8_bin']
            }],
            data=[]
        )

        packages.append(self.last_packet())
        self.send_package_group(packages)

    def answer_function_status(self):
        packages = []
        packages += self.get_tabel_packets(
            columns=[{
                'database': 'mysql',
                'table_name': 'schemata',
                'table_alias': 'ROUTINES',
                'name': 'Db',
                'alias': 'Db',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }, {
                'database': 'mysql',
                'table_name': 'routines',
                'table_alias': 'ROUTINES',
                'name': 'name',
                'alias': 'name',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }, {
                'database': 'mysql',
                'table_name': 'routines',
                'table_alias': 'ROUTINES',
                'name': 'Type',
                'alias': 'Type',
                'type': TYPES.MYSQL_TYPE_STRING,
                'charset': CHARSET_NUMBERS['utf8_bin']
            }, {
                'database': 'mysql',
                'table_name': 'routines',
                'table_alias': 'ROUTINES',
                'name': 'Definer',
                'alias': 'Definer',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': CHARSET_NUMBERS['utf8_bin']
            }, {
                'database': 'mysql',
                'table_name': 'routines',
                'table_alias': 'ROUTINES',
                'name': 'Modified',
                'alias': 'Modified',
                'type': TYPES.MYSQL_TYPE_TIMESTAMP,
                'charset': CHARSET_NUMBERS['binary']
            }, {
                'database': 'mysql',
                'table_name': 'routines',
                'table_alias': 'ROUTINES',
                'name': 'Created',
                'alias': 'Created',
                'type': TYPES.MYSQL_TYPE_TIMESTAMP,
                'charset': CHARSET_NUMBERS['binary']
            }, {
                'database': 'mysql',
                'table_name': 'routines',
                'table_alias': 'ROUTINES',
                'name': 'Security_type',
                'alias': 'Security_type',
                'type': TYPES.MYSQL_TYPE_STRING,
                'charset': CHARSET_NUMBERS['utf8_bin']
            }, {
                'database': 'mysql',
                'table_name': 'routines',
                'table_alias': 'ROUTINES',
                'name': 'Comment',
                'alias': 'Comment',
                'type': TYPES.MYSQL_TYPE_BLOB,
                'charset': CHARSET_NUMBERS['utf8_bin']
            }, {
                'database': 'mysql',
                'table_name': 'character_sets',
                'table_alias': 'ROUTINES',
                'name': 'character_set_client',
                'alias': 'character_set_client',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }, {
                'database': 'mysql',
                'table_name': 'collations',
                'table_alias': 'ROUTINES',
                'name': 'collation_connection',
                'alias': 'collation_connection',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }, {
                'database': 'mysql',
                'table_name': 'collations',
                'table_alias': 'ROUTINES',
                'name': 'Database Collation',
                'alias': 'Database Collation',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }],
            data=[]
        )

        packages.append(self.last_packet())
        self.send_package_group(packages)

    def answer_show_table_status(self, table_name):
        # NOTE at this moment parsed statement only like `SHOW TABLE STATUS LIKE 'table'`.
        # NOTE some columns has {'database': 'mysql'}, other not. That correct. This is how real DB sends messages.
        packages = self.get_tabel_packets(
            columns=[{
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
            }],
            data=[[
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
        )
        packages.append(self.last_packet())
        self.send_package_group(packages)

    def answer_show_warnings(self):
        packages = []
        packages += self.get_tabel_packets(
            columns=[{
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
            }],
            data=[]
        )
        packages.append(self.last_packet())
        self.send_package_group(packages)

    def answer_show_collation(self):
        packages = []
        packages += self.get_tabel_packets(
            columns=[{
                'database': 'information_schema',
                'table_name': 'COLLATIONS',
                'name': 'COLLATION_NAME',
                'alias': 'Collation',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }, {
                'database': 'information_schema',
                'table_name': 'COLLATIONS',
                'name': 'CHARACTER_SET_NAME',
                'alias': 'Charset',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }, {
                'database': 'information_schema',
                'table_name': 'COLLATIONS',
                'name': 'ID',
                'alias': 'Id',
                'type': TYPES.MYSQL_TYPE_LONGLONG,
                'charset': CHARSET_NUMBERS['binary']
            }, {
                'database': 'information_schema',
                'table_name': 'COLLATIONS',
                'name': 'IS_DEFAULT',
                'alias': 'Default',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }, {
                'database': 'information_schema',
                'table_name': 'COLLATIONS',
                'name': 'IS_COMPILED',
                'alias': 'Compiled',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }, {
                'database': 'information_schema',
                'table_name': 'COLLATIONS',
                'name': 'SORTLEN',
                'alias': 'Sortlen',
                'type': TYPES.MYSQL_TYPE_LONGLONG,
                'charset': CHARSET_NUMBERS['binary']
            }],
            data=[
                ['utf8_general_ci', 'utf8', 33, 'Yes', 'Yes', 1],
                ['latin1_swedish_ci', 'latin1', 8, 'Yes', 'Yes', 1]
            ]
        )
        packages.append(self.last_packet())
        self.send_package_group(packages)

    def answer_show_charset(self, charset=None):
        charsets = {
            'utf8': ['utf8', 'UTF-8 Unicode', 'utf8_general_ci', 3],
            'latin1': ['latin1', 'cp1252 West European', 'latin1_swedish_ci', 1],
            'utf8mb4': ['utf8mb4', 'UTF-8 Unicode', 'utf8mb4_general_ci', 4]
        }
        if charset is None:
            data = list(charsets.values())
        elif charset not in charsets:
            err_str = f'Unknown charset: {charset}'
            log.warning(err_str)
            raise Exception(err_str)
        else:
            data = [charsets.get(charset)]

        packages = []
        packages += self.get_tabel_packets(
            columns=[{
                'database': 'information_schema',
                'table_name': 'CHARACTER_SETS',
                'name': 'CHARACTER_SET_NAME',
                'alias': 'Charset',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }, {
                'database': 'information_schema',
                'table_name': 'CHARACTER_SETS',
                'name': 'DESCRIPTION',
                'alias': 'Description',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }, {
                'database': 'information_schema',
                'table_name': 'CHARACTER_SETS',
                'name': 'DEFAULT_COLLATE_NAME',
                'alias': 'Default collation',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }, {
                'database': 'information_schema',
                'table_name': 'CHARACTER_SETS',
                'name': 'MAXLEN',
                'alias': 'Maxlen',
                'type': TYPES.MYSQL_TYPE_LONGLONG,
                'charset': CHARSET_NUMBERS['binary']
            }],
            data=data
        )

        packages.append(self.last_packet())
        self.send_package_group(packages)

    def answer_connection_id(self, sql):
        packages = self.get_tabel_packets(
            columns=[{
                'database': '',
                'table_name': '',
                'name': 'conn_id',
                'alias': 'conn_id',
                'type': TYPES.MYSQL_TYPE_LONG,
                'charset': CHARSET_NUMBERS['binary']
            }],
            data=[[self.connection_id]]
        )
        packages.append(self.last_packet())
        self.send_package_group(packages)

    def answer_select(self, query):
        result = query.fetch(
            self.session.datahub
        )

        if result['success'] is False:
            self.packet(
                ErrPacket,
                err_code=result['error_code'],
                msg=result['msg']
            ).send()
            return

        packages = []
        packages += self.get_tabel_packets(
            columns=query.columns,
            data=query.result
        )
        packages.append(self.packet(OkPacket, eof=True))
        self.send_package_group(packages)

    def _get_column_defenition_packets(self, columns, data=[]):
        packets = []
        for i, column in enumerate(columns):
            table_name = column.get('table_name', 'table_name')
            column_name = column.get('name', 'column_name')
            column_alias = column.get('alias', column_name)
            flags = column.get('flags', 0)
            if self.session.integration_type == 'mssql':
                # mssql raise error if value more then this.
                length = 0x2000
            else:
                if len(data) == 0:
                    length = 0xffff
                else:
                    length = 1
                    for row in data:
                        if isinstance(row, dict):
                            length = max(len(str(row[column_alias])), length)
                        else:
                            length = max(len(str(row[i])), length)

            packets.append(
                self.packet(
                    ColumnDefenitionPacket,
                    schema=column.get('database', 'mindsdb_schema'),
                    table_alias=column.get('table_alias', table_name),
                    table_name=table_name,
                    column_alias=column_alias,
                    column_name=column_name,
                    column_type=column['type'],
                    charset=column.get('charset', CHARSET_NUMBERS["utf8_unicode_ci"]),
                    max_length=length,
                    flags=flags
                )
            )
        return packets

    def get_tabel_packets(self, columns, data, status=0):
        # TODO remove columns order
        packets = [self.packet(ColumnCountPacket, count=len(columns))]
        packets.extend(self._get_column_defenition_packets(columns, data))

        if self.client_capabilities.DEPRECATE_EOF is False:
            packets.append(self.packet(EofPacket, status=status))

        packets += [self.packet(ResultsetRowPacket, data=x) for x in data]
        return packets

    def decode_utf(self, text):
        try:
            return text.decode('utf-8')
        except Exception as e:
            log.error(f'SQL contains non utf-8 values: {text}')
            self.packet(
                ErrPacket,
                err_code=ERR.ER_SYNTAX_ERROR,
                msg=str(e)
            ).send()
            raise

    def is_cloud_connection(self):
        ''' Determine source of connection. Must be call before handshake.
            Idea based on: real mysql connection does not send anything before server handshake, so
            soket should be in 'out' state. In opposite, clout connection sends '0000' right after
            connection. '0000' selected because in real mysql connection it should be lenght of package,
            and it can not be 0.
        '''
        if sys.platform != 'linux':
            return {
                'is_cloud': False
            }

        read_poller = select.poll()
        read_poller.register(self.request, select.POLLIN)
        events = read_poller.poll(0)

        if len(events) == 0:
            return {
                'is_cloud': False
            }

        first_byte = self.request.recv(4, socket.MSG_PEEK)
        if first_byte == b'\x00\x00\x00\x00':
            self.request.recv(4)
            client_capabilities = self.request.recv(8)
            client_capabilities = struct.unpack('L', client_capabilities)[0]

            company_id = self.request.recv(4)
            company_id = struct.unpack('I', company_id)[0]

            database_name_len = self.request.recv(2)
            database_name_len = struct.unpack('H', database_name_len)[0]

            database_name = ''
            if database_name_len > 0:
                database_name = self.request.recv(database_name_len).decode()

            return {
                'is_cloud': True,
                'client_capabilities': client_capabilities,
                'company_id': company_id,
                'database': database_name
            }

        return {
            'is_cloud': False
        }

    def is_db_exists(self, db_name):
        sql_statement = Select(
            targets=[Identifier(parts=["schema_name"], alias=Identifier('Database'))],
            from_table=Identifier(parts=['information_schema', 'SCHEMATA']),
            where=BinaryOperation('=', args=[Identifier('schema_name'), Constant(db_name)])
        )
        query = SQLQuery(
            str(sql_statement),
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
                raise Exception(f"Database {db_name} does not exists")

    def handle(self):
        """
        Handle new incoming connections
        :return:
        """
        self.server.hook_before_handle()

        log.debug('handle new incoming connection')
        cloud_connection = self.is_cloud_connection()
        self.init_session(company_id=cloud_connection.get('company_id'))
        if cloud_connection['is_cloud'] is False:
            if self.handshake() is False:
                return
        else:
            self.client_capabilities = ClentCapabilities(cloud_connection['client_capabilities'])
            self.session.database = cloud_connection['database']
            self.session.username = 'cloud'
            self.session.auth = True
            self.session.integration = None
            self.session.integration_type = None

        while True:
            log.debug('Got a new packet')
            p = self.packet(CommandPacket)

            try:
                success = p.get()
            except Exception:
                log.error('Session closed, on packet read error')
                log.error(traceback.format_exc())
                return

            if success is False:
                log.debug('Session closed by client')
                return

            log.debug('Command TYPE: {type}'.format(
                type=getConstName(COMMANDS, p.type.value)))

            try:
                if p.type.value == COMMANDS.COM_QUERY:
                    sql = self.decode_utf(p.sql.value)
                    sql = SqlStatementParser.clear_sql(sql)
                    log.debug(f'COM_QUERY: {sql}')
                    self.query_answer(sql)
                elif p.type.value == COMMANDS.COM_STMT_PREPARE:
                    # https://dev.mysql.com/doc/internals/en/com-stmt-prepare.html
                    sql = self.decode_utf(p.sql.value)
                    statement = SqlStatementParser(sql)
                    log.debug(f'COM_STMT_PREPARE: {statement.sql}')
                    self.answer_stmt_prepare(statement)
                elif p.type.value == COMMANDS.COM_STMT_EXECUTE:
                    self.answer_stmt_execute(p.stmt_id.value, p.parameters)
                elif p.type.value == COMMANDS.COM_STMT_FETCH:
                    self.answer_stmt_fetch(p.stmt_id.value, p.limit.value)
                elif p.type.value == COMMANDS.COM_STMT_CLOSE:
                    self.answer_stmt_close(p.stmt_id.value)
                elif p.type.value == COMMANDS.COM_QUIT:
                    log.debug('Session closed, on client disconnect')
                    self.session = None
                    break
                elif p.type.value == COMMANDS.COM_INIT_DB:
                    new_database = p.database.value.decode()
                    self.change_default_db(new_database)
                    self.packet(OkPacket).send()
                elif p.type.value == COMMANDS.COM_FIELD_LIST:
                    # this command is deprecated, but console client still use it.
                    self.packet(OkPacket).send()
                else:
                    log.warning('Command has no specific handler, return OK msg')
                    log.debug(str(p))
                    # p.pprintPacket() TODO: Make a version of print packet
                    # that sends it to debug isntead
                    self.packet(OkPacket).send()

            except Exception as e:
                log.error(
                    f'ERROR while executing query\n'
                    f'{traceback.format_exc()}\n'
                    f'{e}'
                )
                self.packet(
                    ErrPacket,
                    err_code=ERR.ER_SYNTAX_ERROR,
                    msg=str(e)
                ).send()

    def packet(self, packetClass=Packet, **kwargs):
        """
        Factory method for packets

        :param packetClass:
        :param kwargs:
        :return:
        """
        p = packetClass(
            socket=self.socket,
            session=self.session,
            proxy=self,
            **kwargs
        )
        self.session.inc_packet_sequence_number()
        return p

    def last_packet(self, status=0x0002):
        if self.client_capabilities.DEPRECATE_EOF is True:
            return self.packet(OkPacket, eof=True, status=status)
        else:
            return self.packet(EofPacket, status=status)

    @staticmethod
    def startProxy():
        """
        Create a server and wait for incoming connections until Ctrl-C
        """
        config = Config()

        cert_path = config['api']['mysql'].get('certificate_path')
        if cert_path is None or cert_path == '':
            cert_path = tempfile.mkstemp(prefix='mindsdb_cert_', text=True)[1]
            make_ssl_cert(cert_path)
            atexit.register(lambda: os.remove(cert_path))

        # TODO make it session local
        server_capabilities.set(
            CAPABILITIES.CLIENT_SSL,
            config['api']['mysql']['ssl']
        )

        host = config['api']['mysql']['host']
        port = int(config['api']['mysql']['port'])

        log.info(f'Starting MindsDB Mysql proxy server on tcp://{host}:{port}')

        SocketServer.TCPServer.allow_reuse_address = True
        server = SocketServer.ThreadingTCPServer((host, port), MysqlProxy)
        server.mindsdb_config = config
        server.check_auth = partial(check_auth, config=config)
        server.cert_path = cert_path
        server.connection_id = 0
        server.hook_before_handle = empty_fn

        server.original_model_interface = ModelInterface()
        server.original_data_store = DataStore()
        server.original_datasource_controller = DatasourceController()

        atexit.register(MysqlProxy.server_close, srv=server)

        # Activate the server; this will keep running until you
        # interrupt the program with Ctrl-C
        log.info('Waiting for incoming connections...')
        server.serve_forever()
