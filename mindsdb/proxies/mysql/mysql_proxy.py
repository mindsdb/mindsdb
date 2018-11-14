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

import random
import socketserver as SocketServer

from mindsdb.mindsdb_server.proxies.mysql.data_types.mysql_packet import Packet
from mindsdb.mindsdb_server.proxies.mysql.data_types.mysql_packets import ErrPacket
from mindsdb.mindsdb_server.proxies.mysql.data_types.mysql_packets import HandshakePacket
from mindsdb.mindsdb_server.proxies.mysql.data_types.mysql_packets import HandshakeResponsePacket
from mindsdb.mindsdb_server.proxies.mysql.data_types.mysql_packets import OkPacket
from mindsdb.mindsdb_server.proxies.mysql.data_types.mysql_packets import SwitchOutPacket
from mindsdb.mindsdb_server.proxies.mysql.data_types.mysql_packets.resultset_packet import ResultsetPacket

import config as CONFIG
from libs.constants.mysql import *
from libs.controllers.session_controller import SessionController
from mindsdb.mindsdb_server.proxies.mysql.data_types.mysql_packets.command_packet import CommandPacket

connection_id = 0

import traceback

ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"


class MysqlProxy(SocketServer.BaseRequestHandler):

    """
    The Main Server controller class
    """

    def initSession(self):

        global connection_id, ALPHABET
        logging.info('New connection [{ip}:{port}]'.format(
            ip=self.client_address[0], port=self.client_address[1]))
        logging.debug(self.__dict__)

        connection_id += 1

        self.session = SessionController()
        self.salt = ''.join([random.choice(ALPHABET) for i in range(20)])
        self.socket = self
        self.count = 0
        self.connection_id = connection_id
        self.logging = logging

        self.current_transaction = None


        logging.debug('session salt: {salt}'.format(salt=self.salt))



    def handle(self):
        """
        Handle new incoming connections
        :return:
        """
        self.initSession()

        HARDCODED_PASSWORD = 'test1'
        HARDCODED_USER = 'root'

        # start by sending of the handshake
        self.packet(HandshakePacket).send()
        # get the response
        handshake_resp = self.packet(HandshakeResponsePacket)
        handshake_resp.get()
        if handshake_resp.length == 0:
            self.packet(OkPacket).send()
            return
            # get the response

        # check if the authentication matches the desired password
        if handshake_resp.isAuthOk(HARDCODED_USER, HARDCODED_PASSWORD):
            salt = ''.join([random.choice(ALPHABET) for i in range(20)])
            self.packet(SwitchOutPacket, seed=salt).send()
            tmp = self.socket.request.recv(MAX_PACKET_SIZE)
            self.packet(OkPacket).send()
            # stay on ping pong loop
            while True:
                logging.debug('Got a new packet')
                p = self.packet(CommandPacket)

                try:
                    success = p.get()

                    if success == False:
                        logging.info('Session closed by client')
                        return

                    logging.info('Command TYPE: {type}'.format(
                        type=VAR_NAME(p.type.value, prefix='COM')))

                    if p.type.value == COM_QUERY:
                        try:
                            sql = p.sql.value.decode('utf-8')
                        except:
                            logging.error('SQL contains non utf-8 values: {sql}'.format(sql=p.sql.value))
                            self.packet(OkPacket).send()
                            continue
                        self.current_transaction = self.session.newTransaction(sql_query=sql)

                        if self.current_transaction.output_data_array is None:
                            self.packet(OkPacket).send()
                        else:
                            self.packet(ResultsetPacket, metadata=self.current_transaction.output_metadata,
                                        data_array=self.current_transaction.output_data_array).send()

                    else:
                        logging.info('Command has no specific handler, return OK msg')
                        logging.debug(str(p))
                        # p.pprintPacket() TODO: Make a version of print packet
                        # that sends it to debug isntead
                        self.packet(OkPacket).send()
                except:
                    logging.warning('Session closed, on packet read error')
                    logging.debug(traceback.format_exc())
                    break

        # else send error packet
        else:
            msg = 'Access denied for user {user} (using password:  YES)'.format(
                user=self.session.username)
            self.packet(ErrPacket, err_code=ER_PASSWORD_NO_MATCH,
                        msg=msg).send()
            logging.warning('AUTH FAIL')

    def packet(self, packetClass=Packet, **kwargs):
        """
        Factory method for packets

        :param packetClass:
        :param kwargs:
        :return:
        """
        return packetClass(socket=self.socket, seq=self.count, session=self.session, proxy=self, **kwargs)

    @staticmethod
    def startProxy():
        """
        Create a server and wait for incoming connections until Ctrl-C
        """
        logging.basicConfig(**CONFIG.PROXY_LOG_CONFIG)
        HOST, PORT = CONFIG.PROXY_SERVER_HOST, CONFIG.PROXY_SERVER_PORT
        logging.info('Starting MindsDB Mysql proxy server on tcp://{host}:{port}'.format(host=HOST, port=PORT))

        # Create the server
        #server = SocketServer.ThreadingUnixDatagramServer(HOST, MysqlProxy)
        server = SocketServer.ThreadingTCPServer(("192.168.1.17", 3306), MysqlProxy)
        # Activate the server; this will keep running until you
        # interrupt the program with Ctrl-C
        logging.info('Waiting for incoming connections...')
        server.serve_forever()

if __name__ == "__main__":
    MysqlProxy.startProxy()
