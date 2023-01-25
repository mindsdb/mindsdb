import traceback
from flask import Flask, request

# from mindsdb.utilities.config import Config
from mindsdb.utilities.log import (
    # initialize_log,
    get_log,
)
from mindsdb.utilities.context import context as ctx
from mindsdb.api.mysql.mysql_proxy.executor.executor import Executor
from mindsdb.api.mysql.mysql_proxy.controllers.session_controller import (
    SessionController,
)

logger = get_log(logger_name="main")


class SqlServerStub:
    """This class is just an emulation of Server object,
    used by Executor.
    In 'monilithic' mode of MindsDB work the Executor takes
    some information from the sql server which. Here we emulate
    this object."""

    def __init__(self, **kwargs):
        for arg in kwargs:
            setattr(self, arg, kwargs[arg])


class ExecutorService:
    """This is an Executor implementation for 'modular' mode of MindsDB.
    It has two caches to cache Session and Executor instances.
    By this way it supports work sessions. In addition it has a REST API
    to receive requests and get responses back.
    IMPORTANT: This class have to has same API as Executor."""

    def __init__(self):
        self.app = Flask(self.__class__.__name__)
        self.executors_cache = {}
        self.sessions_cache = {}

        default_router = self.app.route(
            "/",
            methods=[
                "GET",
            ],
        )
        self._index = default_router(self._index)

        delete_executer_router = self.app.route(
            "/executor",
            methods=[
                "DELETE",
                "DEL",
            ],
        )
        self._del_executor = delete_executer_router(self._del_executor)

        delete_session_router = self.app.route("/session", methods=["DELETE", "DEL"])
        self._del_session = delete_session_router(self._del_session)

        stmt_prepare_router = self.app.route(
            "/stmt_prepare",
            methods=[
                "POST",
            ],
        )
        self.stmt_prepare = stmt_prepare_router(self.stmt_prepare)

        stmt_execute_router = self.app.route(
            "/stmt_execute",
            methods=[
                "POST",
            ],
        )
        self.stmt_execute = stmt_execute_router(self.stmt_execute)

        query_execute_router = self.app.route(
            "/query_execute",
            methods=[
                "POST",
            ],
        )
        self.query_execute = query_execute_router(self.query_execute)

        execute_external_router = self.app.route(
            "/execute_external",
            methods=[
                "POST",
            ],
        )
        self.execute_external = execute_external_router(self.execute_external)

        parse_router = self.app.route(
            "/parse",
            methods=[
                "POST",
            ],
        )
        self.parse = parse_router(self.parse)

        do_execute_router = self.app.route(
            "/do_execute",
            methods=[
                "POST",
            ],
        )
        self.do_execute = do_execute_router(self.do_execute)
        change_default_db_router = self.app.route(
            "/change_default_db",
            methods=[
                "PUT",
            ],
        )
        self.change_default_db = change_default_db_router(self.change_default_db)
        logger.info(
            "%s: base params and route have been initialized", self.__class__.__name__
        )

    def _get_executor(self, params):
        # We have to send context between client and server
        # here we load the context json received from the client(mindsdb)
        # to the local context instance in this Flask thread
        ctx.load(params["context"])
        exec_id = params["id"]
        if exec_id in self.executors_cache:
            logger.info(
                "%s: executor %s found in cache", self.__class__.__name__, exec_id
            )
            return self.executors_cache[exec_id]
        session_id = params["session_id"]
        if session_id in self.sessions_cache:
            logger.info(
                "%s: session %s found in cache", self.__class__.__name__, session_id
            )
            session = self.sessions_cache[session_id]
        else:
            logger.info(
                "%s: creating new session. id - %s, params - %s",
                self.__class__.__name__,
                session_id,
                params["session"],
            )
            session = SessionController()
            self.sessions_cache[session_id] = session
        session.database = params["session"]["database"]
        session.username = params["session"]["username"]
        session.auth = params["session"]["auth"]
        session.prepared_stmts = params["session"]["prepared_stmts"]
        session.packet_sequence_number = params["session"]["packet_sequence_number"]
        sqlserver = SqlServerStub(connection_id=params["connection_id"])

        logger.info(
            "%s: session info - id=%s, params=%s",
            self.__class__.__name__,
            session_id,
            session.to_json(),
        )
        logger.info(
            "%s: creating new executor. id - %s, session_id - %s",
            self.__class__.__name__,
            exec_id,
            session_id,
        )
        executor = Executor(session, sqlserver)
        self.executors_cache[exec_id] = executor
        return executor

    def _run(self, **kwargs):
        """Launch internal Flask application."""
        self.app.run(**kwargs)

    def _index(self):
        """Default GET endpoint - '/'."""
        return "An Executor Wrapper", 200

    def _del_executor(self):
        # to delete executors
        exec_id = request.json.get("id")
        logger.info(
            "%s: removing executor instance. id - %s", self.__class__.__name__, exec_id
        )
        if exec_id is not None and exec_id in self.executors_cache:
            del self.executors_cache[exec_id]
        return "", 200

    def _del_session(self):
        # to delete sessions
        session_id = request.json.get("id")
        logger.info(
            "%s: removing session instance. id - %s",
            self.__class__.__name__,
            session_id,
        )
        if session_id is not None and session_id in self.sessions_cache:
            del self.sessions_cache[session_id]
        return "", 200

    def stmt_prepare(self):
        try:
            params = request.json
            logger.info(
                "%s.stmt_prepare: json received - %s", self.__class__.__name__, params
            )
            executor = self._get_executor(params)
            sql = params.get("sql")
            executor.stmt_prepare(sql)
            resp = executor._to_json()
            return resp, 200
        except Exception:
            err_msg = traceback.format_exc()
            logger.error("%s.stmt_prepare: execution error - %s", err_msg)
            return {"error": err_msg}, 500

    def stmt_execute(self):
        try:
            params = request.json
            logger.info(
                "%s.stmt_execute: json received - %s", self.__class__.__name__, params
            )
            executor = self._get_executor(params)
            param_values = params.get("param_values")
            executor.stmt_execute(param_values)
            resp = executor._to_json()
            return resp, 200
        except Exception:
            err_msg = traceback.format_exc()
            logger.error("%s.stmt_execute: execution error - %s", err_msg)
            return {"error": err_msg}, 500

    def query_execute(self):
        try:
            params = request.json
            logger.info(
                "%s.query_execute: json received - %s", self.__class__.__name__, params
            )
            executor = self._get_executor(params)
            sql = params.get("sql")
            executor.query_execute(sql)
            logger.debug(
                "%s.query_execute: executor.data(type of %s) - %s",
                self.__class__.__name__,
                type(executor.data),
                executor.data,
            )
            logger.debug(
                "%s.query_execute: executor.columns(type of %s) - %s",
                self.__class__.__name__,
                type(executor.columns),
                executor.columns,
            )
            logger.debug(
                "%s.query_execute: executor.params(type of %s) - %s",
                self.__class__.__name__,
                type(executor.params),
                executor.params,
            )

            resp = executor._to_json()
            return resp, 200
        except Exception:
            err_msg = traceback.format_exc()
            logger.error("%s.query_execute: execution error - %s", err_msg)
            return {"error": err_msg}, 500

    def execute_external(self):
        try:
            params = request.json
            logger.info(
                "%s.execute_external: json received - %s",
                self.__class__.__name__,
                params,
            )
            executor = self._get_executor(params)
            sql = params.get("sql")
            executor.execute_external(sql)
            resp = executor._to_json()
            return resp, 200
        except Exception:
            err_msg = traceback.format_exc()
            logger.error("%s.execute_external: execution error - %s", err_msg)
            return {"error": err_msg}, 500

    def parse(self):
        try:
            params = request.json
            logger.info("%s.parse: json received - %s", self.__class__.__name__, params)
            executor = self._get_executor(params)
            sql = params.get("sql")
            executor.parse(sql)
            resp = executor._to_json()
            return resp, 200
        except Exception:
            err_msg = traceback.format_exc()
            logger.error("%s.parse: execution error - %s", err_msg)
            return {"error": err_msg}, 500

    def do_execute(self):
        try:
            params = request.json
            logger.info(
                "%s.do_execute: json received - %s", self.__class__.__name__, params
            )
            executor = self._get_executor(params)
            executor.do_execute()
            resp = executor._to_json()
            return resp, 200
        except Exception:
            err_msg = traceback.format_exc()
            logger.error("%s.do_execute: execution error - %s", err_msg)
            return {"error": err_msg}, 500

    def change_default_db(self):
        try:
            params = request.json
            logger.info("%s.change_default_db: json received - %s", self.__class__.__name__, params)
            executor = self._get_executor(params)
            new_db = params.get("new_db")
            executor.change_default_db(new_db)
            resp = executor._to_json()
            return resp, 200
        except Exception:
            err_msg = traceback.format_exc()
            logger.error("%s.change_default_db: execution error - %s", err_msg)
            return {"error": err_msg}, 500

    def to_mysql_columns(self, foo):
        raise NotImplementedError
