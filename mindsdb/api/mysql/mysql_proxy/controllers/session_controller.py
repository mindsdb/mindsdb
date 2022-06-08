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
from mindsdb.api.mysql.mysql_proxy.utilities import log
from mindsdb.utilities.config import Config
from mindsdb.utilities.with_kwargs_wrapper import WithKWArgsWrapper


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
        self.logging = log

        self.integration = None
        self.integration_type = None
        self.database = None

        self.config = Config()

        self.model_interface = WithKWArgsWrapper(
            server.original_model_interface,
            company_id=company_id
        )

        self.integration_controller = WithKWArgsWrapper(
            server.original_integration_controller,
            company_id=company_id
        )

        self.view_interface = WithKWArgsWrapper(
            server.original_view_controller,
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
