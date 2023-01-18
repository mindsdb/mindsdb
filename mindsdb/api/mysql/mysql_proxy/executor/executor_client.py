import os
import traceback
from uuid import uuid4
import requests
from mindsdb.utilities.log import get_log
from mindsdb.utilities.context import context as ctx
from mindsdb.integrations.libs.net_helpers import sending_attempts

logger = get_log("main")


class ExecutorClient:
    """The class has the same public API with Executor
    It is used to work with Executor service.
    Thus instead of handling all incoming requests in place,
    it forwards them to Executor service, recieves responses and
    returns results back to a calling code.

    IMPORTANT: Since when MindsDB works in 'modularity' mode
    ExecutorClient and ExecutorService do the same work as
    Executor in monolithic mode these three objects have to have
    same public API
    """

    def __init__(self, session, sqlserver):
        self.id = f"executor_{uuid4()}"
        self.headers = {"Content-Type": "application/json"}
        self.base_url = os.environ.get("MINDSDB_EXECUTOR_URL", None)
        if self.base_url is None:
            raise Exception(f"""{self.__class__.__name__} can be used only in modular mode of MindsDB.
                            Use Executor as a service and specify MINDSDB_EXECUTOR_URL env variable""")

        logger.debug(
            "%s.__init__: executor url - %s", self.__class__.__name__, self.base_url
        )
        self.sqlserver = sqlserver
        self.session = session
        self.query = None

        # returns
        self.columns = []
        self.params = []
        self.data = None
        self.state_track = None
        self.server_status = None
        self.is_executed = False

        # additional attributes used in handling query process
        self.sql = ""
        self.sql_lower = ""

    # def _to_mysql_columns(self):
    #     return self.columns

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
        _type = _type.lower()
        call = getattr(requests, _type)
        url = f"{self.base_url}/{endpoint}"

        headers = params.get("headers", None)
        if headers is None:
            headers = self.headers
        else:
            headers.update(self.headers)
        params["headers"] = headers

        r = call(url, **params)
        return r

    def __del__(self):
        """Delete the appropriate ExecutorService instance(on the side of Executor service) as well."""
        logger.debug(
            "%s.%s: delete an appropriate executor instance on the serverside",
            self.__class__.__name__,
            self.id,
        )
        self._do("/executor", "delete", json={"id": self.id})

    def _default_json(self):
        """Store all required data to instanciate ExecutorService instance into json."""
        if hasattr(self.sqlserver, "connection_id"):
            connection_id = self.sqlserver.connection_id
        else:
            connection_id = -1
        return {
            # We have to send context between client and server
            # here we dump the current value of the context
            # to send it to the Executor service
            "id": self.id,
            "connection_id": connection_id,
            "session_id": self.session.id,
            "session": self.session.to_json(),
            "context": ctx.dump(),
        }

    def _update_attrs(self, response_json: dict):
        """Updates attributes by values received from the Executor service."""
        for attr in response_json:
            if hasattr(self, attr):
                logger.debug(
                    "%s: updating %s attribute, new value - %s",
                    self.__class__.__name__,
                    attr,
                    response_json[attr],
                )
                setattr(self, attr, response_json[attr])

    def to_mysql_columns(self, columns):
        return columns

    def stmt_prepare(self, sql):
        json_data = self._default_json()
        json_data["sql"] = sql
        logger.info("%s.stmt_prepare: json=%s", self.__class__.__name__, json_data)
        response = None
        try:
            response = self._do("stmt_prepare", _type="post", json=json_data)
            logger.info(
                "%s.stmt_prepare result:status_code=%s",
                self.__class__.__name__,
                response.status_code,
            )
            logger.debug(
                "%s.stmt_prepare result:body=%s", self.__class__.__name__, response.text
            )
        except Exception:
            msg = traceback.format_exc()
            logger.error(
                "%s.stmt_prepare: request has finished with error: %s",
                self.__class__.__name__,
                msg,
            )
        try:
            resp = response.json()
        except Exception as e:
            logger.error(
                "%s.stmt_prepare: error reading response json: %s",
                self.__class__.__name__,
                e,
            )
            raise e
        if response.status_code != requests.codes.ok and "error" in resp:
            err_msg = resp["error"]
            logger.error(
                "%s.stmt_prepare: executor service returned an error - %s", err_msg
            )
            raise Exception(err_msg)
        try:
            self._update_attrs(resp)
        except Exception as e:
            msg = traceback.format_exc()
            logger.error(
                "%s.stmt_prepare: error reading response json: %s",
                self.__class__.__name__,
                msg,
            )
            raise e

    def stmt_execute(self, param_values):
        if self.is_executed:
            return
        json_data = self._default_json()
        json_data["param_values"] = param_values
        logger.info("%s.stmt_execute: json=%s", self.__class__.__name__, json_data)
        response = None
        try:
            response = self._do("stmt_execute", _type="post", json=param_values)
            logger.info(
                "%s.stmt_execute result:status_code=%s",
                self.__class__.__name__,
                response.status_code,
            )
            logger.debug(
                "%s.stmt_execute result:body=%s", self.__class__.__name__, response.text
            )
        except Exception:
            msg = traceback.format_exc()
            logger.error(
                "%s.stmt_execute: request has finished with error: %s",
                self.__class__.__name__,
                msg,
            )

        try:
            resp = response.json()
        except Exception as e:
            logger.error(
                "%s.stmt_execute: error reading response json: %s",
                self.__class__.__name__,
                e,
            )
            raise e

        if response.status_code != requests.codes.ok and "error" in resp:
            err_msg = resp["error"]
            logger.error(
                "%s.stmt_execute: executor service returned an error - %s", err_msg
            )
            raise Exception(err_msg)

        try:
            self._update_attrs(resp)
        except Exception as e:
            msg = traceback.format_exc()
            logger.error(
                "%s.stmt_execute: error reading response json: %s",
                self.__class__.__name__,
                msg,
            )
            raise e

    def query_execute(self, sql):
        json_data = self._default_json()
        json_data["sql"] = sql
        logger.info("%s.query_execute: json=%s", self.__class__.__name__, json_data)
        response = None
        try:
            response = self._do("query_execute", _type="post", json=json_data)
            logger.info(
                "%s.query_execute result:status_code=%s",
                self.__class__.__name__,
                response.status_code,
            )
            logger.debug(
                "%s.query_execute result:body=%s",
                self.__class__.__name__,
                response.text,
            )
        except Exception:
            msg = traceback.format_exc()
            logger.error(
                "%s.query_execute: request has finished with error: %s",
                self.__class__.__name__,
                msg,
            )

        try:
            resp = response.json()
        except Exception as e:
            logger.error(
                "%s.query_execute: error reading response json: %s",
                self.__class__.__name__,
                e,
            )
            raise e

        if response.status_code != requests.codes.ok and "error" in resp:
            err_msg = resp["error"]
            logger.error(
                "%s.query_execute: executor service returned an error - %s", err_msg
            )
            raise Exception(err_msg)

        try:
            self._update_attrs(resp)
        except Exception as e:
            msg = traceback.format_exc()
            logger.error(
                "%s.query_execute: error reading response json: %s",
                self.__class__.__name__,
                msg,
            )
            raise e

    def execute_external(self, sql):
        json_data = self._default_json()
        json_data["sql"] = sql
        logger.info(
            "%s.execute_external[NOT IMPLEMENTED]: json=%s",
            self.__class__.__name__,
            json_data,
        )
        return

    def parse(self, sql):
        self.sql = sql
        sql_lower = sql.lower()
        self.sql_lower = sql_lower.replace("`", "")

        json_data = self._default_json()
        json_data["sql"] = sql
        logger.info("%s.parse: json=%s", self.__class__.__name__, json_data)
        response = None
        try:
            response = self._do("parse", _type="post", json=json_data)
            logger.info(
                "%s.parse result:status_code=%s",
                self.__class__.__name__,
                response.status_code,
            )
            logger.debug(
                "%s.parse result:body=%s", self.__class__.__name__, response.text
            )
        except Exception:
            msg = traceback.format_exc()
            logger.debug(
                "%s.parse: request has finished with error: %s",
                self.__class__.__name__,
                msg,
            )
        try:
            resp = response.json()
        except Exception as e:
            logger.error(
                "%s.parse: error reading response json: %s", self.__class__.__name__, e
            )
            raise e

        if response.status_code != requests.codes.ok and "error" in resp:
            err_msg = resp["error"]
            logger.error("%s.parse: executor service returned an error - %s", err_msg)
            raise Exception(err_msg)

        try:
            self._update_attrs(resp)
        except Exception as e:
            msg = traceback.format_exc()
            logger.error(
                "%s.parse: error reading response json: %s",
                self.__class__.__name__,
                msg,
            )
            raise e

    def do_execute(self):
        if self.is_executed:
            return
        json_data = self._default_json()
        logger.info("%s.do_execute: json=%s", self.__class__.__name__, json_data)
        try:
            response = self._do("do_execute", _type="post", json=json_data)
            logger.info(
                "%s.do_execute result:status_code=%s",
                self.__class__.__name__,
                response.status_code,
            )
            logger.debug(
                "%s.do_execute result:body=%s", self.__class__.__name__, response.text
            )
        except Exception as e:
            msg = traceback.format_exc()
            logger.error(
                "%s.do_execute: request has finished with error: %s",
                self.__class__.__name__,
                msg,
            )
            raise e

        try:
            resp = response.json()
        except Exception as e:
            logger.error(
                "%s.do_execute: error reading response json: %s",
                self.__class__.__name__,
                e,
            )
            raise e

        if response.status_code != requests.codes.ok and "error" in resp:
            err_msg = resp["error"]
            logger.error("%s.parse: executor service returned an error - %s", err_msg)
            raise Exception(err_msg)

        try:
            self._update_attrs(resp)
        except Exception as e:
            msg = traceback.format_exc()
            logger.error(
                "%s.do_execute: error reading response json: %s",
                self.__class__.__name__,
                msg,
            )
            raise e

    def change_default_db(self, new_db):

        json_data = self._default_json()
        json_data["new_db"] = new_db
        logger.info("%s.change_default_db: json=%s", self.__class__.__name__, json_data)
        response = None
        try:
            response = self._do("change_default_db", _type="put", json=json_data)
            logger.info(
                "%s.change_default_db result:status_code=%s",
                self.__class__.__name__,
                response.status_code,
            )
            logger.debug(
                "%s.change_default_db result:body=%s", self.__class__.__name__, response.text
            )
        except Exception:
            msg = traceback.format_exc()
            logger.debug(
                "%s.change_default_db: request has finished with error: %s",
                self.__class__.__name__,
                msg,
            )
        try:
            resp = response.json()
        except Exception as e:
            logger.error(
                "%s.change_default_db: error reading response json: %s", self.__class__.__name__, e
            )
            raise e

        if response.status_code != requests.codes.ok and "error" in resp:
            err_msg = resp["error"]
            logger.error("%s.change_default_db: executor service returned an error - %s", err_msg)
            raise Exception(err_msg)

        try:
            self._update_attrs(resp)
        except Exception as e:
            msg = traceback.format_exc()
            logger.error(
                "%s.change_default_db: error reading response json: %s",
                self.__class__.__name__,
                msg,
            )
            raise e
