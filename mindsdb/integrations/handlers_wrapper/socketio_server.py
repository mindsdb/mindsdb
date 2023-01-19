import pickle
import functools
import traceback

import socketio
from aiohttp import web

from mindsdb.utilities.context import context as ctx
import mindsdb.interfaces.storage.db as db
from mindsdb.utilities.config import Config
from mindsdb.api.mongo.utilities import logger


def decode(enc):
    if isinstance(enc, str):
        enc = enc.encode()
    return pickle.loads(enc)


def encode(ret):
    return pickle.dumps(ret, protocol=5)


def create_server_app(create_instance_fnc):
    sio = socketio.AsyncServer(async_mode='aiohttp')

    app = web.Application()
    sio.attach(app)

    Config()
    db.init()

    @sio.event
    def connect(sid, environ):
        print('connect ', sid)

    def error_handler(fnc):
        @functools.wraps(fnc)
        async def f(sid, *args, **kwargs):
            try:
                resp = await fnc(sid, *args, **kwargs)
            except Exception as e:
                logger.error(traceback.format_exc())
                return encode(e)
            return resp

        return f

    @sio.event
    @error_handler
    async def init(sid, params_enc):
        params = decode(params_enc)

        ctx.load(params['context'])

        instance = create_instance_fnc(*params['args'], **params['kwargs'])

        await sio.save_session(sid, {'instance': instance, 'context': params['context']})

        return encode(None)

    @sio.event
    @error_handler
    async def request(sid, params_enc):
        params = decode(params_enc)

        session = await sio.get_session(sid)

        instance = session['instance']
        ctx.load(session['context'])

        method = params['method']

        fnc = getattr(instance, method)

        resp = fnc(*params['args'], **params['kwargs'])

        resp_enc = encode(resp)
        return resp_enc

    @sio.event
    def disconnect(sid):
        # TODO delete instance?

        print('disconnect ', sid)

    return app


class SocketIOClient:
    def __init__(self, service_url):
        self.sio = socketio.Client()
        self.sio.connect(service_url)

    def __getattr__(self, method_name):

        def call(*args, **kwargs):
            return self.send_command(method_name, *args, **kwargs)
        return call

    def init(self, *args, **kwargs):
        params = {
            'context': ctx.dump(),
            'args': args,
            'kwargs': kwargs
        }
        params_enc = encode(params)

        resp_enc = self.sio.call('init', params_enc, timeout=120)

        resp = decode(resp_enc)
        if isinstance(resp, Exception):
            raise resp

    def send_command(self, method_name, *args, **kwargs):

        params = {
            'method': method_name,
            'args': args,
            'kwargs': kwargs
        }

        params_enc = encode(params)

        resp_enc = self.sio.call('request', params_enc, timeout=120)

        resp = decode(resp_enc)
        if isinstance(resp, Exception):
            raise resp
        return resp

    def close(self):
        self.sio.disconnect()
