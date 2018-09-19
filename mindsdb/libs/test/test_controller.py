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

from mindsdb.libs.controllers.session_controller import SessionController
import pprint
import click
from mindsdb.libs.helpers.logging import logging


logging.basicConfig(level=10)


"""
The idea behind this controller is:
 That we can load a fake session and test phases and other key components

 Simply do TestController.loadModule(ModuleClass)
 and you will have an instance of your module with all the session variables that you need

"""


class TestController():

    def __init__(self, sql = 'show schemas;', breakpoint=None):
        """
        Initialize a session and pass a dummy socket env
        """
        self.request = TestSocket()
        self.session = SessionController()
        self.session.newTransaction(sql_query=sql, breakpoint=breakpoint)

    @staticmethod
    def loadModule(module_class, sql = 'show schemas;', breakpoint=None):
        """
        return an instance of a module for testing

        :param module_class: the actual class to initialize
        :return: an instance of the module
        """
        c = TestController(sql, breakpoint)
        m = module_class(c.session)
        return m


class TestSocket():
    """
    This emulates the mysql socket even though we dont actually have one
    """
    @staticmethod
    def sendall(str_to_send):
        pprint.pprint(str_to_send)

    @staticmethod
    def recv(len_header):
        read = click.prompt(
            'This is a simulation as no socket is here, please input the string you want to pass to the socket')
        return read
