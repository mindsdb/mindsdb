import json
import walrus

from mindsdb.streams.base import BaseStream


class RedisStream(BaseStream):
    def __init__(self, stream, connection_info):
        self.client = walrus.Database(**connection_info)
        self.stream = self.client.Stream(stream)

    def read(self):
        for k, when_data in self.stream_in.read():
            self.stream_in.delete(k)
            yield json.loads(when_data[b''])

    def write(self, dct):
        self.stream.add({'': json.dumps(dct)})
