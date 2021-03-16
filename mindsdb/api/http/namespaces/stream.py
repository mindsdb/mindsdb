from flask import request
from flask_restx import Resource, abort
from flask import current_app as ca
from mindsdb.utilities.log import log
from mindsdb.api.http.namespaces.configs.streams import ns_conf
from mindsdb.streams.redis.redis_stream import RedisStream



@ns_conf.route('/')
class StreamList(Resource):
    @ns_conf.doc("get_streams")
    def get(self):
        return {'streams': ["foo", "bar"]}


@ns_conf.route('/<name>')
@ns_conf.param('name', 'Key-value storage stream')
class Stream(Resource):
    @ns_conf.doc("get_stream")
    def get(self, name):
        log.error("in get")
        if name == "fake":
            abort(404, f"Can\'t find steam: {name}")
        return {"name": name, "status": "fake"}

    @ns_conf.doc("put_stream")
    def put(self, name):
        # params = request.json.get('params')
        # if not isinstance(params, dict):
        #     abort(400, "type of 'params' must be dict")
        # if 'integration_name' not in params:
        #     abort(400, "need to specify redis integration")
        # return params, 200
        log.error("in put")
        integration_name = "redis_test"
        redis_host = "127.0.0.1"
        redis_port = 6379
        db = 0
        predictor = "metro_traffic"
        input_stream = "predict_when"
        stream = RedisStream(redis_host, redis_port, db, input_stream, predictor)
        log.error("running redis stream")
        stream.start()
