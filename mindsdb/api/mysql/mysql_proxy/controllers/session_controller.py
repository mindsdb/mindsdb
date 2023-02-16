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
from uuid import uuid4

import requests

from mindsdb.api.mysql.mysql_proxy.datahub import init_datahub
from mindsdb.api.mysql.mysql_proxy.utilities import logger
from mindsdb.utilities.config import Config
from mindsdb.interfaces.model.model_controller import ModelController
from mindsdb.interfaces.database.database import DatabaseController
from mindsdb.interfaces.database.integrations import IntegrationController


class SessionController:
    """
    This class manages the server session
    """

    def __init__(self) -> object:
        """
        Initialize the session
        """

        self.username = None
        self.auth = False
        self.logging = logger
        self.database = None

        self.config = Config()

        self.model_controller = ModelController()
        self.integration_controller = IntegrationController()
        self.database_controller = DatabaseController()

        self.datahub = init_datahub(self)

        self.prepared_stmts = {}
        self.packet_sequence_number = 0

    def inc_packet_sequence_number(self):
        self.packet_sequence_number = (self.packet_sequence_number + 1) % 256

    def register_stmt(self, statement):
        i = 1
        while i in self.prepared_stmts and i < 100:
            i = i + 1
        if i == 100:
            raise Exception("Too many unclosed queries")

        self.prepared_stmts[i] = dict(type=None, statement=statement, fetched=0)
        return i

    def unregister_stmt(self, stmt_id):
        del self.prepared_stmts[stmt_id]

    def to_json(self):
        return {
            "username": self.username,
            "auth": self.auth,
            "database": self.database,
            "prepared_stmts": self.prepared_stmts,
            "packet_sequence_number": self.packet_sequence_number,
        }

    def from_json(self, updated):
        for key in updated:
            setattr(self, key, updated[key])


class ServerSessionContorller(SessionController):
    """SessionController implementation for case of Executor service.
    The difference with SessionController is that there is an id in this one.
    The instance uses the id to synchronize its settings with the appropriate
    ServiceSessionController instance on the Executor side."""

    def __init__(self):
        super().__init__()
        self.id = f"session_{uuid4()}"
        self.executor_url = os.environ.get("MINDSDB_EXECUTOR_URL", None)
        self.executor_host = os.environ.get("MINDSDB_EXECUTOR_SERVICE_HOST", None)
        self.executor_port = os.environ.get("MINDSDB_EXECUTOR_SERVICE_PORT", None)
        if (self.executor_host is None or self.executor_port is None) and self.executor_url is None:
            raise Exception(f"""{self.__class__.__name__} can be used only in modular mode of MindsDB.
                            Use Executor as a service and specify MINDSDB_EXECUTOR_URL env variable""")
        logger.info(
            "%s.__init__: executor url - %s", self.__class__.__name__, self.executor_url
        )

    def __del__(self):
        """Terminate the appropriate ServiceSessionController instance as well."""
        if self.executor_url is not None:
            url = self.executor_url + "/" + "session"
            logger.info(
                "%s.__del__: delete an appropriate ServiceSessionController with, id - %s on the Executor service side",
                self.__class__.__name__,
                self.id,
            )
            requests.delete(url, json={"id": self.id})
