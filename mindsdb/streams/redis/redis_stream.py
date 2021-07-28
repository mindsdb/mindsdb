import json
import walrus

from mindsdb.streams.base import BaseStream


class RedisStream(BaseStream):
    def __init__(self, stream, connection_info):
        self.client = walrus.Database(**connection_info)
        self.stream = self.client.Stream(stream)

    def read(self):
        for k, when_data in self.stream.read():
            yield json.loads(when_data[b''])
            self.stream.delete(k)

    def write(self, dct):
        self.stream.add({'': json.dumps(dct)})
