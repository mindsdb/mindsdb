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
from mindsdb.libs.helpers.logging import logging

from pymongo import MongoClient
import mindsdb.config as CONFIG
from mindsdb.libs.controllers.transaction_controller import TransactionController
from mindsdb.libs.constants.mindsdb import *

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


    def newTransaction(self, transaction_metadata, breakpoint = PHASE_END):

        return TransactionController(session=self, transaction_metadata=transaction_metadata, breakpoint = breakpoint)

