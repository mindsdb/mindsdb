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
from mindsdb.utilities.with_kwargs_wrapper import WithKWArgsWrapper

from mindsdb.interfaces.model.model_controller import ModelController
from mindsdb.interfaces.database.integrations import IntegrationController
from mindsdb.interfaces.database.views import ViewController
from mindsdb.interfaces.database.projects import ProjectController
from mindsdb.interfaces.database.database import DatabaseController


class SessionController():
    '''
    This class manages the server session
    '''

    def __init__(self, server, company_id: int = None, user_class: int = None) -> object:
        """
        Initialize the session
        :param company_id:
        """

        self.username = None
        self.user_class = user_class
        self.auth = False
        self.company_id = company_id
        self.logging = logger
        self.database = None

        self.config = Config()

        self.model_controller = WithKWArgsWrapper(
            server.original_model_controller,
            company_id=company_id
        )

        self.integration_controller = WithKWArgsWrapper(
            server.original_integration_controller,
            company_id=company_id
        )

        self.project_controller = WithKWArgsWrapper(
            server.original_project_controller,
            company_id=company_id
        )

        self.database_controller = WithKWArgsWrapper(
            server.original_database_controller,
            company_id=company_id
        )

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
            raise Exception('Too many unclosed queries')

        self.prepared_stmts[i] = dict(
            type=None,
            statement=statement,
            fetched=0
        )
        return i

    def unregister_stmt(self, stmt_id):
        del self.prepared_stmts[stmt_id]

    def to_json(self):
        return {
                "username": self.username,
                "user_class": self.user_class,
                "auth": self.auth,
                "company_id": self.company_id,
                "database": self.database,
                "prepared_stmts": self.prepared_stmts,
                "packet_sequence_number": self.packet_sequence_number,
                }


class ServerSessionContorller(SessionController):
    """SessionController implementation for case of Executor service.
    The difference with SessionController is that there is an id in this one.
    The instance uses the id to synchronize its settings with the appropriate
    ServiceSessionController instance on the Executor side."""
    def __init__(self, server, company_id=None, user_class=None):
        super().__init__(server, company_id, user_class)
        self.id = f"session_{uuid4()}"
        executor_host = os.environ.get("MINDSDB_EXECUTOR_HOSTNAME")
        executor_port = os.environ.get("MINDSDB_EXECUTOR_PORT")
        if executor_host and executor_port:
            self.executor_url = f"http://{executor_host}:{executor_port}"
        else:
            self.executor_url = "http://localhost:5500"

        logger.debug("%s.__init__: executor url - %s", self.__class__.__name__, self.executor_url)

    def __del__(self):
        """Terminate the appropriate ServiceSessionController instance as well."""
        url = self.executor_url + "/" + "session"
        requests.delete(url, json={"id":self.id})


class ServiceSessionController(SessionController):
    """Slight modification of SessionController class to use it in Executor service.
    The class doens't depend from mysql server."""

    def __init__(self, company_id=None, user_class=None):
        """
        Initialize the session
        :param company_id:
        """

        self.username = None
        self.user_class = user_class
        self.auth = False
        self.company_id = company_id
        self.logging = logger
        self.database = None

        self.config = Config()

        self.model_controller = WithKWArgsWrapper(
            ModelController(),
            company_id=company_id
        )

        self.integration_controller = WithKWArgsWrapper(
            IntegrationController(),
            company_id=company_id
        )

        self.view_controller = WithKWArgsWrapper(
            ViewController(),
            company_id=company_id
        )

        self.project_controller = WithKWArgsWrapper(
            ProjectController(),
            company_id=company_id
        )

        self.database_controller = WithKWArgsWrapper(
            DatabaseController(),
            company_id=company_id
        )

        self.datahub = init_datahub(self)

        self.prepared_stmts = {}
        self.packet_sequence_number = 0
