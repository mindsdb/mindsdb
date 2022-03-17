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
from typing import List, Dict

import pandas as pd
from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast import (
    RollbackTransaction,
    CommitTransaction,
    StartTransaction,
    BinaryOperation,
    NullConstant,
    Identifier,
    Parameter,
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
    Use
)
from mindsdb_sql.parser.dialects.mysql import Variable
from mindsdb_sql.parser.dialects.mindsdb import (
    CreateDatasource,
    RetrainPredictor,
    CreatePredictor,
    DropDatasource,
    DropPredictor,
    CreateView
)

from mindsdb.api.mysql.mysql_proxy.utilities.sql import query_df
from mindsdb.utilities.wizards import make_ssl_cert
from mindsdb.utilities.config import Config
from mindsdb.api.mysql.mysql_proxy.data_types.mysql_packet import Packet
from mindsdb.api.mysql.mysql_proxy.controllers.session_controller import SessionController
from mindsdb.api.mysql.mysql_proxy.classes.client_capabilities import ClentCapabilities
from mindsdb.api.mysql.mysql_proxy.classes.server_capabilities import server_capabilities
from mindsdb.api.mysql.mysql_proxy.classes.sql_statement_parser import SqlStatementParser
from mindsdb.api.mysql.mysql_proxy.utilities import log
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
)
from mindsdb.api.mysql.mysql_proxy.utilities.functions import get_column_in_case

from mindsdb.api.mysql.mysql_proxy.external_libs.mysql_scramble import scramble as scramble_func
from mindsdb.api.mysql.mysql_proxy.classes.sql_query import (
    SQLQuery,
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
from mindsdb.interfaces.database.integrations import IntegrationController
from mindsdb.interfaces.database.views import ViewController


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
        integrations_names = IntegrationController().get_all(company_id).keys()
        for integration_name in integrations_names:
            if username == f'{hardcoded_user}_{integration_name}':
                extracted_username = hardcoded_user
                integration = integration_name
                integration_type = IntegrationController().get(integration, company_id)['type']

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


class ANSWER_TYPE:
    __slots__ = ()
    TABLE = 'table'
    OK = 'ok'
    ERROR = 'error'


ANSWER_TYPE = ANSWER_TYPE()


class SQLAnswer:
    def __init__(self, answer_type: ANSWER_TYPE, columns: List[Dict] = None, data: List[Dict] = None,
                 status: int = None, state_track: List[List] = None, error_code: int = None, error_message: str = None):
        self.answer_type = answer_type
        self.columns = columns
        self.data = data
        self.status = status
        self.state_track = state_track
        self.error_code = error_code
        self.error_message = error_message

    @property
    def type(self):
        return self.answer_type


class MysqlProxy(SocketServer.BaseRequestHandler):
    """
    The Main Server controller class
    """

    predictor_attrs = ("model", "features", "ensemble")

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

        select_data_query = insert.get('select_data_query')
        if isinstance(select_data_query, str) is False or len(select_data_query) == 0:
            return SQLAnswer(
                ANSWER_TYPE.ERROR,
                error_code=ERR.ER_WRONG_ARGUMENTS,
                error_message="'select_data_query' should not be empty"
            )

        models = model_interface.get_models()
        if insert['name'] in [x['name'] for x in models]:
            return SQLAnswer(
                ANSWER_TYPE.ERROR,
                error_code=ERR.ER_WRONG_ARGUMENTS,
                error_message=f"predictor with name '{insert['name']}'' already exists"
            )

        kwargs = {}
        if isinstance(insert.get('training_options'), str) \
                and len(insert['training_options']) > 0:
            try:
                kwargs = json.loads(insert['training_options'])
            except Exception:
                return SQLAnswer(
                    ANSWER_TYPE.ERROR,
                    error_code=ERR.ER_WRONG_ARGUMENTS,
                    error_message='training_options should be in valid JSON string'
                )

        integration = self.session.integration
        if isinstance(integration, str) is False or len(integration) == 0:
            return SQLAnswer(
                ANSWER_TYPE.ERROR,
                error_code=ERR.ER_WRONG_ARGUMENTS,
                error_message='select_data_query can be used only in query from database'
            )
        insert['select_data_query'] = insert['select_data_query'].replace(r"\'", "'")
        ds_name = data_store.get_vacant_name(insert['name'])
        ds = data_store.save_datasource(ds_name, integration, {'query': insert['select_data_query']})

        insert['predict'] = [x.strip() for x in insert['predict'].split(',')]

        ds_data = data_store.get_datasource(ds_name)
        if ds_data is None:
            raise ErBadDbError(f"DataSource '{ds_name}' does not exists")
        ds_columns = [x['name'] for x in ds_data['columns']]
        for col in insert['predict']:
            if col not in ds_columns:
                data_store.delete_datasource(ds_name)
                raise ErKeyColumnDoesNotExist(f"Column '{col}' not exists")

        try:
            insert['predict'] = self._check_predict_columns(insert['predict'], ds_columns)
        except Exception:
            data_store.delete_datasource(ds_name)
            raise

        model_interface.learn(
            insert['name'], ds, insert['predict'], ds_data['id'], kwargs=kwargs, delete_ds_on_fail=True
        )

        return SQLAnswer(ANSWER_TYPE.OK)

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

    def _get_ensemble_type(self, data):
        ai_info = data.get('json_ai', {})
        if ai_info == {}:
            raise ErBadTableError("predictor doesn't contain enough data to generate 'feature' attribute.")
        return [[ai_info["model"]["module"]]]

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
            columns = [{
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
            }]
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
            elif predictor_attr == "ensemble":
                data = self._get_ensemble_type(data)
                columns = [{
                    'table_name': '',
                    'name': 'ensemble',
                    'type': TYPES.MYSQL_TYPE_VAR_STRING
                }]
            else:
                raise ErNotSupportedYet("DESCRIBE '%s' predictor attribute is not supported yet" % predictor_attr)

        return SQLAnswer(
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
        return SQLAnswer(ANSWER_TYPE.OK)

    def answer_create_datasource(self, struct: dict):
        ''' create new datasource (integration in old terms)
            Args:
                struct: data for creating integration
        '''
        datasource_name = struct['datasource_name']
        database_type = struct['database_type']
        connection_args = struct['connection_args']
        connection_args['type'] = database_type

        self.session.integration_controller.add(datasource_name, connection_args)
        return SQLAnswer(ANSWER_TYPE.OK)

    def answer_drop_datasource(self, ds_name):
        try:
            ds = self.session.integration_controller.get(ds_name)
            self.session.integration_controller.delete(ds['database_name'])
        except Exception:
            raise ErDbDropDelete(f"Something went wrong during deleting of datasource '{ds_name}'.")
        return SQLAnswer(answer_type=ANSWER_TYPE.OK)

    def answer_create_view(self, statement):
        name = statement.name
        query = str(statement.query_str)
        datasource_name = statement.from_table.parts[-1]

        self.session.view_interface.add(name, query, datasource_name)
        return SQLAnswer(answer_type=ANSWER_TYPE.OK)

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
        data_store = self.session.data_store

        predictor_name = struct['predictor_name']
        integration_name = struct.get('integration_name')

        if integration_name is not None:
            if integration_name.lower().startswith('datasource.'):
                ds_name = integration_name[integration_name.find('.') + 1:]
                ds = data_store.get_datasource_obj(ds_name, raw=True)
                ds_data = data_store.get_datasource(ds_name)
            else:
                if (
                    self.session.integration_controller.get(integration_name) is None
                    and integration_name not in ('views', 'files')
                ):
                    raise ErBadDbError(f"Unknown datasource: {integration_name}")

                ds_name = struct.get('datasource_name')
                if ds_name is None:
                    ds_name = data_store.get_vacant_name(predictor_name)

                ds_kwargs = {'query': struct['select']}
                if integration_name in ('views', 'files'):
                    parsed = parse_sql(struct['select'])
                    query_table = parsed.from_table.parts[-1]
                    if integration_name == 'files':
                        ds_kwargs['mindsdb_file_name'] = query_table
                    else:
                        ds_kwargs['source'] = query_table
                ds = data_store.save_datasource(ds_name, integration_name, ds_kwargs)
                ds_data = data_store.get_datasource(ds_name)
                ds_id = ds_data['id']

            ds_column_names = [x['name'] for x in ds_data['columns']]
            try:
                predict = self._check_predict_columns(struct['predict'], ds_column_names)
            except Exception as e:
                data_store.delete_datasource(ds_name)
                raise e

            for i, p in enumerate(predict):
                predict[i] = get_column_in_case(ds_column_names, p)
        else:
            ds = None
            ds_id = None
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
                        raise Exception(
                            f'Cant get appropriate cast column case. Columns: {ds_column_names}, column: {col}'
                        )
                    kwargs['timeseries_settings']['order_by'][i] = new_name
            group_by = kwargs['timeseries_settings'].get('group_by')
            if group_by is not None:
                for i, col in enumerate(group_by):
                    new_name = get_column_in_case(ds_column_names, col)
                    kwargs['timeseries_settings']['group_by'][i] = new_name
                    if new_name is None:
                        raise Exception(
                            f'Cant get appropriate cast column case. Columns: {ds_column_names}, column: {col}'
                        )

        model_interface.learn(predictor_name, ds, predict, ds_id, kwargs=kwargs, delete_ds_on_fail=True)

        return SQLAnswer(ANSWER_TYPE.OK)

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

        if result['success'] is False:
            self.packet(
                ErrPacket,
                err_code=result['error_code'],
                msg=result['msg']
            ).send()
            return

        predictors_names = [x[0] for x in result['result']]

        if len(predictors_names) == 0:
            raise SqlApiException('nothing to delete')

        for predictor_name in predictors_names:
            self.session.datahub['mindsdb'].delete_predictor(predictor_name)

    def handle_custom_command(self, command):
        command = command.strip(' ;').split()

        if command[0].lower() == 'delete' and command[1].lower() == 'predictor':
            if len(command) != 3:
                return SQLAnswer(
                    ANSWER_TYPE.ERROR,
                    error_code=ERR.ER_SYNTAX_ERROR,
                    error_message="wrong syntax of 'DELETE PREDICTOR {NAME}' command"
                )
            predictor_name = command[2]
            self.delete_predictor_query(parse_sql(
                f"delete from mindsdb.predictors where name = '{predictor_name}'",
                'mindsdb'
            ))
            return SQLAnswer(ANSWER_TYPE.OK)

        return SQLAnswer(
            ANSWER_TYPE.ERROR,
            error_code=ERR.ER_SYNTAX_ERROR,
            error_message="at this moment only 'delete predictor' command supported"
        )

    def to_mysql_type(self, type_name):
        if type_name == 'str':
            return TYPES.MYSQL_TYPE_VAR_STRING

        # unknown
        return TYPES.MYSQL_TYPE_VAR_STRING

    def answer_stmt_prepare(self, sql):
        sqlquery = SQLQuery(
            sql,
            session=self.session,
            execute=False
        )

        stmt_id = self.session.register_stmt(sqlquery)
        prepared_stmt = self.session.prepared_stmts[stmt_id]

        sqlquery.prepare_query()
        parameters = sqlquery.parameters
        columns_def = sqlquery.columns

        statement = sqlquery.query
        if isinstance(statement, Insert):
            prepared_stmt['type'] = 'insert'

            # ???
            if (
                len(statement.table.parts) > 1 and statement.table.parts[0].lower() != 'mindsdb'
                or len(statement.table.parts) == 1 and self.session.database != 'mindsdb'
            ):
                raise ErNonInsertableTable("Only parametrized insert into table from 'mindsdb' database supported at this moment")
            table_name = statement.table.parts[-1]
            if table_name not in ['predictors', 'commands']:
                raise ErNonInsertableTable("Only parametrized insert into 'predictors' or 'commands' supported at this moment")

            # new_statement = Select(
            #     targets=statement.columns,
            #     from_table=Identifier(parts=['mindsdb', table_name]),
            #     limit=Constant(0)
            # )

            # parameters = []
            # for row in statement.values:
            #     for item in row:
            #         if type(item) == Parameter:
            #             num_params = num_params + 1
            # num_columns = len(query.columns) - num_params

            # ???
            # if len(sqlquery.columns) != len(sqlquery.parameters):
            #     raise ErNonInsertableTable("At this moment supported only insert where all values is parameters.")

            columns_def = []
            for col in sqlquery.columns:
                col = col.copy()
                col['charset'] = CHARSET_NUMBERS['binary']
                columns_def.append(col)

        elif isinstance(statement, Select) and statement.mode == 'FOR UPDATE':
            # postgres when execute "delete from mindsdb.predictors where name = 'x'" sends for it prepare statement:
            # 'select name from mindsdb.predictors where name = 'x' FOR UPDATE;'
            # and after it send prepare for delete query.
            prepared_stmt['type'] = 'lock'

            # statement.cut_from_tail('for update')
            # query = SQLQuery(statement.sql, session=self.session)
            # num_columns = len(query.columns)
            # parameters = []
            for col in columns_def:
                col['charset'] = CHARSET_NUMBERS['utf8_general_ci']

        elif isinstance(statement, Delete):
            prepared_stmt['type'] = 'delete'
            #
            # fake_sql = sql.replace('?', '"?"')
            # fake_sql = 'select name ' + fake_sql[len('delete '):]
            # query = SQLQuery(fake_sql, session=self.session)
            # num_columns = 0
            # num_params = sql.count('?')
            columns_def = []
            for col in sqlquery.parameters:
                columns_def.append(dict(
                    database='',
                    table_alias='',
                    table_name='',
                    alias=col.name,
                    name='',
                    type=TYPES.MYSQL_TYPE_VAR_STRING,
                    charset=CHARSET_NUMBERS['utf8_general_ci'],
                    flags=sum([FIELD_FLAG.BINARY_COLLATION])
                ))
        # elif statement.keyword == 'select' and 'connection_id()' in sql.lower():
        #     prepared_stmt['type'] = 'select'
        #     num_columns = 1
        #     parameters = []
        #     columns_def = [{
        #         'database': '',
        #         'table_name': '',
        #         'name': 'conn_id',
        #         'alias': 'conn_id',
        #         'type': TYPES.MYSQL_TYPE_LONG,
        #         'charset': CHARSET_NUMBERS['binary']
        #     }]
        elif isinstance(statement, Select):
            prepared_stmt['type'] = 'select'
        #     query = SQLQuery(sql, session=self.session)
        #     num_columns = len(query.columns)
        #     parameters = []
        #     columns_def = query.columns
        elif isinstance(statement, Show) and statement.category in (
            'variables', 'session variables', 'show session status'
        ):
            prepared_stmt['type'] = 'show variables'
            num_columns = 2
            parameters = []
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
            raise SqlApiException(f"Only 'SELECT' and 'INSERT' statements supported. Got: {sql}")

        packages = [
            self.packet(
                STMTPrepareHeaderPacket,
                stmt_id=stmt_id,
                num_columns=len(columns_def),
                num_params=len(parameters)
            )
        ]

        parameters_def = sqlquery.to_mysql_columns(parameters)
        if len(parameters_def) > 0:
            packages.extend(
                self._get_column_defenition_packets(parameters_def)
            )
            if self.client_capabilities.DEPRECATE_EOF is False:
                status = sum([SERVER_STATUS.SERVER_STATUS_AUTOCOMMIT])
                packages.append(self.packet(EofPacket, status=status))

        if len(columns_def) > 0:
            packages.extend(
                self._get_column_defenition_packets(columns_def)
            )

            if self.client_capabilities.DEPRECATE_EOF is False:
                status = sum([SERVER_STATUS.SERVER_STATUS_AUTOCOMMIT])
                packages.append(self.packet(EofPacket, status=status))

        self.send_package_group(packages)

    def answer_stmt_execute(self, stmt_id, parameters):
        prepared_stmt = self.session.prepared_stmts[stmt_id]

        sqlquery = prepared_stmt['statement']
        sqlquery.execute_query(parameters)
        query = sqlquery.query
        if prepared_stmt['type'] == 'select':
            # sql = prepared_stmt['statement'].sql

            # +++

            if query == Select(targets=[Function(op='connection_id', args=())]):
                result = self.answer_connection_id()
                self.send_query_answer(result)
                return
            # ---

            # +++
            # if "SELECT `table_name`, `column_name`" in sql:
            #     # TABLEAU
            #     # SELECT `table_name`, `column_name`
            #     # FROM `information_schema`.`columns`
            #     # WHERE `data_type`='enum' AND `table_schema`='mindsdb'
            #     packages = self.get_tabel_packets(
            #         columns=[{
            #             'table_name': '',
            #             'name': 'TABLE_NAME',
            #             'type': TYPES.MYSQL_TYPE_VAR_STRING
            #         }, {
            #             'table_name': '',
            #             'name': 'COLUMN_NAME',
            #             'type': TYPES.MYSQL_TYPE_VAR_STRING
            #         }],
            #         data=[]
            #     )
            #     if self.client_capabilities.DEPRECATE_EOF is True:
            #         packages.append(self.packet(OkPacket, eof=True))
            #     else:
            #         packages.append(self.packet(EofPacket))
            #     self.send_package_group(packages)
            #     return
            # # ---

            columns = sqlquery.columns
            packages = [self.packet(ColumnCountPacket, count=len(columns))]
            packages.extend(self._get_column_defenition_packets(columns))

            packages.append(self.last_packet(status=0x0062))
            self.send_package_group(packages)
        elif prepared_stmt['type'] == 'insert':
            # statement = parse_sql(prepared_stmt['statement'].sql, dialect='mindsdb')
            # parameter_index = 0
            # for row in query.values:
            #     for item_index, item in enumerate(row):
            #         if type(item) == Parameter:
            #             row[item_index] = Constant(parameters[parameter_index])
            #             parameter_index += 1
            result = self.process_insert(query)
            self.send_query_answer(result)
            return
        elif prepared_stmt['type'] == 'lock':
            # sql = prepared_stmt['statement'].sql
            # query = SQLQuery(sql, session=self.session)

            columns = sqlquery.columns
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
                raise SqlApiException("Delete statement must content 'where' filter")
            # sql = prepared_stmt['statement'].sql
            # sql = sql[:sql.find('?')] + f"'{parameters[0]}'"
            self.delete_predictor_query(query)
            self.packet(OkPacket, affected_rows=1).send()
        elif prepared_stmt['type'] == 'show variables':
            # sql = prepared_stmt['statement'].sql

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
                data=[]
            )

            packages.append(self.last_packet())
            self.send_package_group(packages)
            return
        else:
            raise ErNotSupportedYet(f"Unknown statement type: {prepared_stmt['type']}")

    def answer_stmt_fetch(self, stmt_id, limit=100000):
        prepared_stmt = self.session.prepared_stmts[stmt_id]
        # sql = prepared_stmt['statement'].sql
        fetched = prepared_stmt['fetched']
        # query = SQLQuery(sql, session=self.session)
        sqlquery = prepared_stmt['statement']

        result = sqlquery.fetch(
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
        columns = sqlquery.columns
        for row in sqlquery.result[fetched:limit]:
            packages.append(
                self.packet(BinaryResultsetRowPacket, data=row, columns=columns)
            )

        prepared_stmt['fetched'] += len(sqlquery.result[fetched:limit])

        if len(sqlquery.result) <= limit:
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
            return self.answer_explain_predictors()
        elif table == 'commands' and db == 'mindsdb':
            return self.answer_explain_commands()
        else:
            raise ErNotSupportedYet("Only 'EXPLAIN predictors' and 'EXPLAIN commands' supported")

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

        columns = self._get_explain_columns(),
        data = [
            # [Field, Type, Null, Key, Default, Extra]
            ['name', 'varchar(255)', 'NO', 'PRI', None, ''],
            ['status', 'varchar(255)', 'YES', '', None, ''],
            ['accuracy', 'varchar(255)', 'YES', '', None, ''],
            ['predict', 'varchar(255)', 'YES', '', None, ''],
            ['select_data_query', 'varchar(255)', 'YES', '', None, ''],
            ['training_options', 'varchar(255)', 'YES', '', None, ''],
        ]

        return SQLAnswer(
            answer_type=ANSWER_TYPE.TABLE,
            columns=columns,
            data=data,
            status=status
        )

    def answer_explain_commands(self):
        status = sum([
            SERVER_STATUS.SERVER_STATUS_AUTOCOMMIT,
            SERVER_STATUS.SERVER_QUERY_NO_INDEX_USED,
        ])

        columns = self._get_explain_columns(),
        data = [
            # [Field, Type, Null, Key, Default, Extra]
            ['command', 'varchar(255)', 'NO', 'PRI', None, '']
        ]
        return SQLAnswer(
            answer_type=ANSWER_TYPE.TABLE,
            columns=columns,
            data=data,
            status=status
        )

    def process_insert(self, statement):
        db_name = self.session.database
        if len(statement.table.parts) == 2:
            db_name = statement.table.parts[0].lower()
        table_name = statement.table.parts[-1].lower()
        if db_name != 'mindsdb' or table_name not in ('predictors', 'commands'):
            raise ErNonInsertableTable("At this moment only insert to 'mindsdb.predictors' or 'mindsdb.commands' is possible")
        column_names = []
        for column_identifier in statement.columns:
            if isinstance(column_identifier, Identifier) is False or len(column_identifier.parts) != 1:
                raise ErKeyColumnDoesNotExist(f'Incorrect column name: {column_identifier}')
            column_name = column_identifier.parts[0].lower()
            column_names.append(column_name)
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

    def process_query(self, sql):
    # def query_answer(self, sql):
        # +++
        # if query not for mindsdb then process that query in integration db
        # TODO redirect only select data queries
        if (
            isinstance(self.session.database, str)
            and len(self.session.database) > 0
            and self.session.database.lower() not in ('mindsdb', 'files')
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
                raise ErBadDbError('Unknown database - %s' % self.session.database)
            result, _column_names = datanode.select(sql)

            columns = []
            data = []
            if len(result) > 0:
                columns = [{
                    'table_name': '',
                    'name': x,
                    'type': TYPES.MYSQL_TYPE_VAR_STRING
                } for x in result[0].keys()]
                data = [[str(value) for key, value in x.items()] for x in result]

            return SQLAnswer(
                answer_type=ANSWER_TYPE.TABLE,
                columns=columns,
                data=data
            )
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
            log.warning(f'SQL statement are not parsed by mindsdb_sql: {sql}')
            pass

        if type(statement) == CreateDatasource:
            struct = {
                'datasource_name': statement.name,
                'database_type': statement.engine,
                'connection_args': statement.parameters
            }
            return self.answer_create_datasource(struct)
        if type(statement) == DropPredictor:
            predictor_name = statement.name.parts[-1]
            self.session.datahub['mindsdb'].delete_predictor(predictor_name)
            self.packet(OkPacket).send()
        elif keyword == 'create_datasource':
            # fallback for statement
            return self.answer_create_datasource(struct)
        elif type(statement) == DropDatasource:
            ds_name = statement.name.parts[-1]
            return self.answer_drop_datasource(ds_name)
        elif type(statement) == Describe:
            if statement.value.parts[-1] in self.predictor_attrs:
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
                    BinaryOperation('like', args=[Identifier('table_type'), Constant('BASE TABLE')])
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

                columns = [{
                    'table_name': 'session_variables',
                    'name': 'Variable_name',
                    'type': TYPES.MYSQL_TYPE_VAR_STRING
                }, {
                    'table_name': 'session_variables',
                    'name': 'Value',
                    'type': TYPES.MYSQL_TYPE_VAR_STRING
                }]

                return SQLAnswer(
                    answer_type=ANSWER_TYPE.TABLE,
                    columns=columns,
                    data=data
                )
            elif "show status like 'ssl_version'" in sql_lower:
                return SQLAnswer(
                    answer_type=ANSWER_TYPE.TABLE,
                    columns=[{
                        'table_name': 'session_variables',
                        'name': 'Variable_name',
                        'type': TYPES.MYSQL_TYPE_VAR_STRING
                    }, {
                        'table_name': 'session_variables',
                        'name': 'Value',
                        'type': TYPES.MYSQL_TYPE_VAR_STRING
                    }],
                    data=[['Ssl_version', 'TLSv1.1']]
                )
            elif sql_category in ('function status', 'procedure status'):
                # SHOW FUNCTION STATUS WHERE Db = 'MINDSDB';
                # SHOW PROCEDURE STATUS WHERE Db = 'MINDSDB'
                # SHOW FUNCTION STATUS WHERE Db = 'MINDSDB' AND Name LIKE '%';
                return self.answer_function_status()
            elif sql_category == 'index':
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
            else:
                raise ErNotSupportedYet(f'Statement not implemented: {sql}')
        elif type(statement) in (StartTransaction, CommitTransaction, RollbackTransaction):
            return SQLAnswer(ANSWER_TYPE.OK)
        elif type(statement) == Set:
            category = (statement.category or '').lower()
            if category == '' and type(statement.arg) == BinaryOperation:
                return SQLAnswer(ANSWER_TYPE.OK)
            elif category == 'autocommit':
                return SQLAnswer(ANSWER_TYPE.OK)
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
                return SQLAnswer(
                    ANSWER_TYPE.OK,
                    state_track=[
                        ['character_set_client', self.charset],
                        ['character_set_connection', self.charset],
                        ['character_set_results', self.charset]
                    ]
                )
            else:
                log.warning(f'SQL statement is not processable, return OK package: {sql}')
                return SQLAnswer(ANSWER_TYPE.OK)
        elif type(statement) == Use:
            db_name = statement.value.parts[-1]
            self.change_default_db(db_name)
            return SQLAnswer(ANSWER_TYPE.OK)
        elif type(statement) == CreatePredictor:
            return self.answer_create_predictor(statement)
        elif type(statement) == CreateView:
            return self.answer_create_view(statement)
        elif keyword == 'set':
            log.warning(f'Unknown SET query, return OK package: {sql}')
            return SQLAnswer(ANSWER_TYPE.OK)
        elif type(statement) == Delete:
            if self.session.database != 'mindsdb' and statement.table.parts[0] != 'mindsdb':
                raise ErBadTableError("Only 'DELETE' from database 'mindsdb' is possible at this moment")
            if statement.table.parts[-1] != 'predictors':
                raise ErBadTableError("Only 'DELETE' from table 'mindsdb.predictors' is possible at this moment")
            self.delete_predictor_query(statement)
            return SQLAnswer(ANSWER_TYPE.OK)
        elif type(statement) == Insert:
            return self.process_insert(statement)
        elif keyword in ('update', 'insert'):
            raise ErNotSupportedYet('Update and Insert are not implemented')
        elif keyword == 'alter' and ('disable keys' in sql_lower) or ('enable keys' in sql_lower):
            return SQLAnswer(ANSWER_TYPE.OK)
        elif type(statement) == Select:
            if statement.from_table is None:
                return self.answer_single_row_select(statement)
            if "table_name,table_comment,if(table_type='base table', 'table', table_type)" in sql_lower:
                # TABLEAU
                # SELECT TABLE_NAME,TABLE_COMMENT,IF(TABLE_TYPE='BASE TABLE', 'TABLE', TABLE_TYPE),TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA LIKE 'mindsdb' AND ( TABLE_TYPE='BASE TABLE' OR TABLE_TYPE='VIEW' )  ORDER BY TABLE_SCHEMA, TABLE_NAME
                # SELECT TABLE_NAME,TABLE_COMMENT,IF(TABLE_TYPE='BASE TABLE', 'TABLE', TABLE_TYPE),TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=DATABASE() AND ( TABLE_TYPE='BASE TABLE' OR TABLE_TYPE='VIEW' )  ORDER BY TABLE_SCHEMA, TABLE_NAME
                if "table_schema like 'mindsdb'" in sql_lower:
                    data = [
                        ['predictors', '', 'TABLE', 'mindsdb']
                    ]
                else:
                    data = []
                columns = [{
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
                }]
                return SQLAnswer(
                    answer_type=ANSWER_TYPE.TABLE,
                    columns=columns,
                    data=[data]
                )

            query = SQLQuery(
                sql,
                session=self.session
            )
            return self.answer_select(query)
        elif type(statement) == Explain:
            return self.answer_explain_table(statement.target.parts)
        else:
            log.warning(f'Unknown SQL statement: {sql}')
            raise ErNotSupportedYet(f'Unknown SQL statement: {sql}')

    def send_query_answer(self, answer: SQLAnswer):
        if answer.type == ANSWER_TYPE.TABLE:
            packages = []
            packages += self.get_tabel_packets(
                columns=answer.columns,
                data=answer.data
            )
            if answer.status is not None:
                packages.append(self.last_packet(status=answer.status))
            else:
                packages.append(self.last_packet())
            self.send_package_group(packages)
        elif answer.type == ANSWER_TYPE.OK:
            self.packet(OkPacket, state_track=answer.state_track).send()
        elif answer.type == ANSWER_TYPE.ERROR:
            self.packet(
                ErrPacket,
                err_code=answer.error_code,
                msg=answer.error_message
            ).send()

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
                    log.warning(f'Unknown variable: {column_name}')
                    result = ''
                else:
                    result = result[0]
            elif target_type == Function:
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
                column_name = str(result)
                column_alias = '.'.join(target.alias.parts) if type(target.alias) == Identifier else column_name
            else:
                raise Exception(f'Unknown constant type: {target_type}')

            columns.append({
                'table_name': '',
                'name': column_name,
                'alias': column_alias,
                'type': TYPES.MYSQL_TYPE_VAR_STRING if isinstance(result, str) else TYPES.MYSQL_TYPE_LONG,
                'charset': self.charset_text_type if isinstance(result, str) else CHARSET_NUMBERS['binary']
            })
            data.append(result)

        return SQLAnswer(
            answer_type=ANSWER_TYPE.TABLE,
            columns=columns,
            data=[data]
        )

    def answer_show_create_table(self, table):
        columns = [{
            'table_name': '',
            'name': 'Table',
            'type': TYPES.MYSQL_TYPE_VAR_STRING
        }, {
            'table_name': '',
            'name': 'Create Table',
            'type': TYPES.MYSQL_TYPE_VAR_STRING
        }]
        return SQLAnswer(
            answer_type=ANSWER_TYPE.TABLE,
            columns=columns,
            data=[[table, f'create table {table} ()']]
        )

    def answer_function_status(self):
        columns = [{
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
        }]
        return SQLAnswer(
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
        return SQLAnswer(
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
        return SQLAnswer(
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
        data = [[self.connection_id]]
        return SQLAnswer(
            answer_type=ANSWER_TYPE.TABLE,
            columns=columns,
            data=data
        )

    def answer_select(self, query):
        result = query.fetch(
            self.session.datahub
        )

        if result['success'] is False:
            return SQLAnswer(
                ANSWER_TYPE.ERROR,
                error_code=result['error_code'],
                error_message=result['msg']
            )

        return SQLAnswer(
            answer_type=ANSWER_TYPE.TABLE,
            columns=query.columns,
            data=query.result
        )

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
                    result = self.process_query(sql)
                    self.send_query_answer(result)
                elif p.type.value == COMMANDS.COM_STMT_PREPARE:
                    # https://dev.mysql.com/doc/internals/en/com-stmt-prepare.html
                    sql = self.decode_utf(p.sql.value)
                    # statement = SqlStatementParser(sql)
                    # log.debug(f'COM_STMT_PREPARE: {statement.sql}')
                    self.answer_stmt_prepare(sql)
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

            except SqlApiException as e:
                log.error(
                    f'ERROR while executing query\n'
                    f'{traceback.format_exc()}\n'
                    f'{e}'
                )
                self.packet(
                    ErrPacket,
                    err_code=e.err_code,
                    msg=str(e)
                ).send()
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
        server.original_integration_controller = IntegrationController()
        server.original_view_controller = ViewController()

        atexit.register(MysqlProxy.server_close, srv=server)

        # Activate the server; this will keep running until you
        # interrupt the program with Ctrl-C
        log.info('Waiting for incoming connections...')
        server.serve_forever()


class Dummy:
    pass


class FakeMysqlProxy(MysqlProxy):
    def __init__(self, company_id):
        request = Dummy()
        client_address = ['', '']
        server = Dummy()
        server.connection_id = 0
        server.hook_before_handle = empty_fn
        server.original_model_interface = ModelInterface()
        server.original_data_store = DataStore()
        server.original_integration_controller = IntegrationController()
        server.original_view_controller = ViewController()

        self.request = request
        self.client_address = client_address
        self.server = server

        self.session = SessionController(
            server=self.server,
            company_id=company_id
        )
        self.session.database = 'mindsdb'

    def is_cloud_connection(self):
        return {
            'is_cloud': False
        }
