
import traceback
import pickle
import requests
from mindsdb.api.mysql.mysql_proxy.utilities import (
    logger
)
from mindsdb.integrations.libs.net_helpers import sending_attempts

class ExecutorClient:
    def __init__(self, session, sqlserver, service_url=None):
        self.headers = {"Content-Type": "application/json"}
        self.base_url = service_url or "http://localhost:5500"
        self.sqlserver = sqlserver
        self.session = session
        self.query = None

        # returns
        self.columns = []
        self.params = []
        self.data = None
        self.state_track = None
        self.server_status = None


        # self.predictor_metadata = {}

        self.sql = ''
        self.sql_lower = ''

    @sending_attempts()
    def _do(self, endpoint, _type="get", **params) -> requests.Response:
        """Performs several attempts to send a request to the service.

        Args:
            endpoint: a service endpoint name
            _type: request type (get, put, post)
            params: dict of request params

        Raises:
            Exception if all attempts were failed.
    
        """
        call = None
        _type = _type.lower()
        if _type == "get":
            call = requests.get
        elif _type == "post":
            call = requests.post
        elif _type == "put":
            call = requests.put

        url = f"{self.base_url}/{endpoint}"

        headers = params.get("headers", None)
        if headers is None:
            headers = self.headers
        else:
            headers.update(self.headers)
        params["headers"] = headers

        r = call(url, **params)
        return r

    def default_json(self):
        return {
                "session_id": self.session.id,
                "connection_id": self.sqlserver.connection_id,
                }

    def _update_attrs(self, response_json: dict):
        for attr in response_json:
            if hasattr(self, attr):
                logger.debug("%s: updating %s attribute, new value - %s", self.__class__.__name__, attr, response_json[attr])
                setattr(self, attr, response_json[attr])

    def stmt_prepare(self, sql):
        json_data = self.default_json()
        json_data["sql"] = sql
        logger.debug("%s.stmt_prepare: json=%s", self.__class__.__name__, json_data)
        response = None
        try:
            response = self._do("stmt_prepare", _type="post", json=json_data)
            logger.debug("%s.stmt_prepare result:status_code=%s, body=%s", self.__class__.__name__, response.status_code, response.text)
        except Exception:
            msg = traceback.format_exc()
            logger.error("%s.stmt_prepare: request has finished with error: %s", self.__class__.__name__, msg)
        try:
            self._update_attrs(response.json())
        except Exception:
            msg = traceback.format_exc()
            logger.error("%s.stmt_prepare: error reading response json: %s", self.__class__.__name__, msg)
        try:
            query = pickle.loads(response.content)
            logger.error("%s.stmt_prepare: success unpickle query object", self.__class__.__name__)
            self.query = query
        except Exception as e:
            msg = traceback.format_exc()
            logger.error("%s.stmt_prepare: error unpickle query object: %s", self.__class__.__name__, msg)
            raise e

    def query_execute(self, sql):
        json_data = self.default_json()
        json_data["sql"] = sql
        logger.debug("%s.query_execute: json=%s", self.__class__.__name__, json_data)
        response = None
        try:
            response = self._do("query_execute", _type="post", json=json_data)
            logger.debug("%s.query_execute result:status_code=%s, body=%s", self.__class__.__name__, response.status_code, response.text)
        except Exception:
            msg = traceback.format_exc()
            logger.error("%s.query_execute: request has finished with error: %s", self.__class__.__name__, msg)
        try:
            self._update_attrs(response.json())
        except Exception:
            msg = traceback.format_exc()
            logger.error("%s.query_execute: error reading response json: %s", self.__class__.__name__, msg)
        try:
            query = pickle.loads(response.content)
            logger.error("%s.query_execute: success unpickle query object", self.__class__.__name__)
            self.query = query
        except Exception as e:
            msg = traceback.format_exc()
            logger.error("%s.query_execute: error unpickle query object: %s", self.__class__.__name__, msg)
            raise e

    def execute_external(self, sql):
        json_data = self.default_json()
        json_data["sql"] = sql
        logger.debug("%s.execute_external: json=%s", self.__class__.__name__, json_data)
        response = None
        try:
            response = self._do("execute_external", _type="post", json=json_data)
            logger.debug("%s.execute_external result:status_code=%s, body=%s", self.__class__.__name__, response.status_code, response.text)
        except Exception:
            msg = traceback.format_exc()
            logger.error("%s.execute_external: request has finished with error: %s", self.__class__.__name__, msg)
        try:
            self._update_attrs(response.json())
        except Exception:
            msg = traceback.format_exc()
            logger.error("%s.execute_external: error reading response json: %s", self.__class__.__name__, msg)

    def parse(self, sql):
        self.sql = sql
        sql_lower = sql.lower()
        self.sql_lower = sql_lower.replace('`', '')

        json_data = self.default_json()
        json_data["sql"] = sql
        logger.debug("%s.parse: json=%s", self.__class__.__name__, json_data)
        response = None
        try:
            response = self._do("parse", _type="post", json=json_data)
            logger.debug("%s.parse result:status_code=%s, body=%s", self.__class__.__name__, response.status_code, response.text)
        except Exception:
            msg = traceback.format_exc()
            logger.error("%s.parse: request has finished with error: %s", self.__class__.__name__, msg)

        logger.debug("%s.parse: load query object from response.content", self.__class__.__name__)
        try:
            self._update_attrs(response.json())
        except Exception:
            msg = traceback.format_exc()
            logger.error("%s.parse: error reading response json: %s", self.__class__.__name__, msg)
        try:
            query = pickle.loads(response.content)
            logger.error("%s.parse: success unpickle query object", self.__class__.__name__)
            self.query = query
        except Exception as e:
            msg = traceback.format_exc()
            logger.error("%s.parse: error unpickle query object: %s", self.__class__.__name__, msg)
            raise e

    def do_execute(self):
        if self.is_executed:
            return
        json_data = self.default_json()
        logger.debug("%s.do_execute: json=%s", self.__class__.__name__, json_data)
        try:
            response = self._do("do_execute", _type="post", json=json_data)
            logger.debug("%s.do_execute result:status_code=%s, body=%s", self.__class__.__name__, response.status_code, response.text)
        except Exception as e:
            msg = traceback.format_exc()
            logger.error("%s.do_execute: request has finished with error: %s", self.__class__.__name__, msg)
            raise e
        try:
            self._update_attrs(response.json())
        except Exception:
            msg = traceback.format_exc()
            logger.error("%s.do_execute: error reading response json: %s", self.__class__.__name__, msg)

        self.is_executed = True
        self.data = response.json()["data"]
        self.server_status = response.json()["status"]
        if response.json().get("columns") is not None:
            self.columns = response.json()["columns"]
        self.state_track = response.json()["state_track"]
