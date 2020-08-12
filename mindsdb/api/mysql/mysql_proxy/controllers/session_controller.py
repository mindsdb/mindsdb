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

from mindsdb.api.mysql.mysql_proxy.controllers.log import log


class SessionController():
    '''
    This class manages the server session
    '''

    def __init__(self) -> object:
        """
        Initialize the session
        :param socket:
        """

        self.username = None
        self.auth = False
        self.logging = log

        self.integration = None
        self.database = None

        self.statements = {}

    def register_statement(self, sql):
        self.statements.keys()
        i = 1
        while i in self.statements and i < 100:
            i = i + 1
        if i == 100:
            raise Exception('Too many unclosed queries')
        self.statements[i] = dict(
            type=None,
            sql=sql,
            insert=None,
            fetched=0
        )
        return i

    def unregister_statement(self, statement_id):
        del self.statements[statement_id]
