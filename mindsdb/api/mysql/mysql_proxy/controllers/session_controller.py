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

        self.current_transaction = None

    def newTransaction(self, sql_query):
        return {
            'output_metadata': {},
            'output_data_array': []
        }
