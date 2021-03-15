from flask import request
from flask_restx import Resource, abort
from flask import current_app as ca
from mindsdb.utilities.log import log
from mindsdb.api.http.namespaces.configs.streams import ns_conf



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
        if name == "fake":
            abort(404, f"Can\'t find steam: {name}")
        return {"name": name, "status": "fake"}

    @ns_conf.doc("put_stream")
    def put(self, name):
        params = request.json.get('params')
        if not isinstance(params, dict):
            abort(400, "type of 'params' must be dict")
        if 'integration_name' not in params:
            abort(400, "need to specify redis integration")
        return params, 200
