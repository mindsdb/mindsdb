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

from mindsdb.api.mysql.mysql_proxy.datahub import init_datahub
from mindsdb.api.mysql.mysql_proxy.utilities import logger
from mindsdb.utilities.config import Config
from mindsdb.utilities.with_kwargs_wrapper import WithKWArgsWrapper
from mindsdb.utilities.context import context as ctx


class SessionController():
    '''
    This class manages the server session
    '''

    def __init__(self, server) -> object:
        """
        Initialize the session
        """

        self.username = None
        self.user_class = ctx.user_class
        self.company_id = ctx.company_id
        self.auth = False
        self.logging = logger
        self.database = None

        self.config = Config()

        self.model_controller = WithKWArgsWrapper(
            server.original_model_controller,
            company_id=ctx.company_id
        )

        self.integration_controller = server.original_integration_controller

        self.project_controller = WithKWArgsWrapper(
            server.original_project_controller,
            company_id=ctx.company_id
        )

        self.database_controller = WithKWArgsWrapper(
            server.original_database_controller,
            company_id=ctx.company_id
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
