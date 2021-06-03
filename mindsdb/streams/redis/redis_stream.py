import json
import walrus

from mindsdb.streams.base import BaseStream


class RedisStream(BaseStream):
    def __init__(self, name, predictor, connection_info, stream_in, stream_out):
        BaseStream.__init__(self, name, predictor)
        self.client = walrus.Database(**connection_info)
        self.stream_in = self.client.Stream(stream_in)
        self.stream_out = self.client.Stream(stream_out)

    def _read_from_in_stream(self):
        print('reading from stream_in')
        for k, when_data in self.stream_in.read(block=0):
            self.stream_in.delete(k)
            yield json.loads(when_data[b''])

    def _write_to_out_stream(self, dct):
        print('writing to stream_out')
        self.stream_out.add({'': json.dumps(dct)})
