from flask import Flask, request

from mindsdb.api.mysql.mysql_proxy.utilities import (
    logger
)
from mindsdb.api.mysql.mysql_proxy.executor.executor import Executor
from mindsdb.api.mysql.mysql_proxy.controllers.session_controller import ServiceSessionController


class SqlServerStub:
    def __init__(self, **kwargs):
        for arg in kwargs:
            setattr(self, arg, kwargs[arg])


class ExecutorService:

    def __init__(self):
        self.app = Flask(self.__class__.__name__)
        self.executors_cache = {}
        self.sessions_cache = {}

        default_router = self.app.route("/")
        self.index = default_router(self.index)

        delete_executer_router = self.app.route("/executor")
        self.executor = delete_executer_router(self.executor)


        delete_session_router = self.app.route("/session")
        self.session = delete_session_router(self.session)

        stmt_prepare_router = self.app.route("/stmt_prepare")
        self.stmt_prepare = stmt_prepare_router(self.stmt_prepare)

        stmt_execute_router = self.app.route("/stmt_execute")
        self.stmt_execute = stmt_execute_router(self.stmt_execute)

        query_execute_router = self.app.route("/query_execute")
        self.query_execute = query_execute_router(self.query_execute)

        execute_external_router = self.app.route("/execute_external")
        self.execute_external = execute_external_router(self.execute_external)

        parse_router = self.app.route("/parse")
        self.parse = parse_router(self.parse)

        do_execute_router = self.app.route("/do_execute")
        self.do_execute = do_execute_router(self.do_execute)
        logger.info("%s: base params and route have been initialized", self.__class__.__name__)

    def _get_executor(self, params):
        exec_id = params["id"]
        if exec_id in self.executors_cache:
            logger.debug("%s: executor %s found in cache", self.__class__.__name__, exec_id)
            return self.executors_cache[exec_id]
        session_id = params["session_id"]
        if session_id in self.sessions_cache:
            logger.debug("%s: session %s found in cache", self.__class__.__name__, session_id)
            session = self.sessions_cache[session_id]
        else:
            logger.debug("%s: creating new session. id - %s, company_id - %s, user_class - %s",
                    self.__class__.__name__,
                    session_id,
                    params["company_id"],
                    params["user_class"]
                )
            session = ServiceSessionController(params["company_id"], params["user_class"])
            self.sessions_cache[session_id] = session
        sqlserver = SqlServerStub(connection_id=params["connection_id"])
        logger.debug("%s: creating new executor. id - %s, session_id - %s",
                self.__class__.__name__,
                exec_id,
                session_id,
            )
        executor = Executor(session, sqlserver)
        self.executors_cache[exec_id] = executor
        return executor

    def run(self, **kwargs):
        """ Launch internal Flask application."""
        self.app.run(**kwargs)

    def index(self):
        """ Default GET endpoint - '/'."""
        return "An Executor Wrapper", 200

    def executor(self):
        # to delete executors
        exec_id = request.json.get("id")
        logger.info("%s: removing executor instance. id - %s", self.__class__.__name__, exec_id)
        if exec_id is not None and exec_id in self.executors_cache:
            del self.executors_cache[exec_id]

    def session(self):
        # to delete sessions
        session_id = request.json.get("id")
        logger.info("%s: removing session instance. id - %s", self.__class__.__name__, session_id)
        if session_id is not None and session_id in self.sessions_cache:
            del self.sessions_cache[session_id]

    def stmt_prepare(self):
        params = request.json
        logger.debug("%s.stmt_prepare: json received - %s", self.__class__.__name__, params)
        executor = self._get_executor(params)
        sql = params.get("sql")
        executor.stmt_prepare(sql)
        resp = executor.to_json()
        return resp, 200

    def stmt_execute(self):
        params = request.json
        logger.debug("%s.stmt_execute: json received - %s", self.__class__.__name__, params)
        executor = self._get_executor(params)
        param_values = params.get("param_values")
        executor.stmt_execute(param_values)
        resp = executor.to_json()
        return resp, 200

    def query_execute(self):
        params = request.json
        logger.debug("%s.query_execute: json received - %s", self.__class__.__name__, params)
        executor = self._get_executor(params)
        sql = params.get("sql")
        executor.query_execute(sql)
        resp = executor.to_json()
        return resp, 200

    def execute_external(self):
        params = request.json
        logger.debug("%s.execute_external: json received - %s", self.__class__.__name__, params)
        executor = self._get_executor(params)
        sql = params.get("sql")
        executor.execute_external(sql)
        resp = executor.to_json()
        return resp, 200

    def parse(self):
        params = request.json
        logger.debug("%s.parse: json received - %s", self.__class__.__name__, params)
        executor = self._get_executor(params)
        sql = params.get("sql")
        executor.parse(sql)
        resp = executor.to_json()
        return resp, 200

    def do_execute(self):
        params = request.json
        logger.debug("%s.do_execute: json received - %s", self.__class__.__name__, params)
        executor = self._get_executor(params)
        executor.do_execute()
        resp = executor.to_json()
        return resp, 200
