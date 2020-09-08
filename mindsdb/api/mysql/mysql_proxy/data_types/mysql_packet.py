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

import struct
import logging

from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MAX_PACKET_SIZE


class Packet:
    def __init__(self, length=0, seq=0, body='', packet_string=None, socket=None, session=None, proxy=None, parent_packet=None, **kwargs):
        if parent_packet is None:
            self.mysql_socket = socket
            self.session = session
            self.proxy = proxy
        else:
            self.mysql_socket = parent_packet.mysql_socket
            self.session = parent_packet.session
            self.proxy = parent_packet.proxy

        self._kwargs = kwargs

        self.setup()
        if packet_string is not None:
            self.loadFromPacketString(packet_string)
        else:
            self.loadFromParams(length, seq, body)

    def setup(self, length=0, seq=0, body=None):
        self.loadFromParams(length=length, seq=seq, body=body)

    def loadFromParams(self, length, seq, body):
        self._length = length
        self._seq = seq
        self._body = body

    def setBody(self, body_string):
        self._body = body_string
        self._length = len(body_string)

    def loadFromPacketString(self, packet_string):
        len_header = struct.unpack('>i', struct.pack('1s', '') + packet_string[:3])[0]
        count_header = struct.unpack('B', packet_string[3])[0]
        body = packet_string[4:]
        self.loadFromParams(length=len_header, seq=count_header, body=body)

    def getPacketString(self):
        body = self.body
        len_header = struct.pack('<i', self.length)[:3]  # keep it 3 bytes
        count_header = struct.pack('B', self.seq)
        packet = len_header + count_header + body
        return packet

    def get(self):
        self.session.logging.info(f'Get packet: {self.__class__.__name__}')

        len_header = MAX_PACKET_SIZE
        body = b''
        count_header = 1
        while len_header == MAX_PACKET_SIZE:
            packet_string = self.mysql_socket.recv(4)
            if len(packet_string) < 4:
                self.session.logging.warning(f'Packet with less than 4 bytes in length: {packet_string}')
                return False
                break
            len_header = struct.unpack('i', packet_string[:3] + b'\x00')[0]
            count_header = int(packet_string[3])
            if len_header == 0:
                break
            body += self.mysql_socket.recv(len_header)
        self.session.logging.info(f'Got packet: {str(body)}')
        self.proxy.count = (int(count_header) + 1) % 256
        self.setup(len(body), count_header, body)
        return True

    def send(self):
        string = self.getPacketString()
        self.session.logging.info(f'Sending packet: {self.__class__.__name__}')
        self.session.logging.debug(string)
        self.mysql_socket.sendall(string)

    def accum(self):
        string = self.getPacketString()
        self.session.logging.info(f'Accumulating packet: {self.__class__.__name__}')
        self.session.logging.debug(string)
        return string

    def pprintPacket(self, body=None):
        if body is None:
            body = self.body
        print(str(self))
        for i, x in enumerate(body):
            part = '[BODY]'
            print('''{part}{i}:{h} ({inte}:{actual})'''.format(part=part, i=i + 1, h=hex(ord(x)), inte=ord(x), actual=str(x)))

    def isEOF(self):
        if self.length == 0:
            return True
        else:
            return False

    @property
    def length(self):
        # self._length = len(self.body)
        return self._length

    @property
    def seq(self):
        return self._seq

    @property
    def body(self):
        return self._body

    @staticmethod
    def bodyStringToPackets(body_string):
        """
        The method takes a string and turns it into mysql_packets

        :param body_string: text to turn into mysql_packets
        :return: a list of mysql_packets

        """
        ret = []
        body_len = len(body_string)
        mod = body_len % MAX_PACKET_SIZE
        num_packets = body_len / MAX_PACKET_SIZE + (1 if mod > 0 else 0)

        for i in range(num_packets):
            left_limit = i * MAX_PACKET_SIZE
            right_limit = mod if i + 1 == num_packets else MAX_PACKET_SIZE * (i + 1)
            body = body_string[left_limit:right_limit]
            ret += [Packet(length=right_limit, seq=i + 1, body=body)]

        return ret

    def __str__(self):
        return str({'body': self.body, 'length': self.length, 'seq': self.seq})


def test():
    import pprint

    pprint.pprint(Packet.bodyStringToPackets('abdds')[0].getPacketString())


# only run the test if this file is called from debugger
if __name__ == "__main__":
    test()
