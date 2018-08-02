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

# import logging
from libs.helpers.logging import logging

from pymongo import MongoClient
import config as CONFIG
from libs.controllers.transaction_controller import TransactionController
from libs.constants.mindsdb import *

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
        self.logging = logging
        self.mongo = MongoClient(CONFIG.MONGO_SERVER_HOST)
        #self.drill = PyDrill(host=CONFIG.DRILL_SERVER_HOST, port=CONFIG.DRILL_SERVER_PORT)

        self.current_transaction = None


    def newTransaction(self, sql_query, breakpoint = PHASE_END):
        return TransactionController(session=self, sql_query=sql_query, breakpoint = breakpoint)

