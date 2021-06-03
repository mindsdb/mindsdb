from mindsdb.interfaces.database.integrations import get_db_integration
from flask import request
from flask_restx import Resource, abort
from mindsdb.utilities.log import log
from mindsdb.api.http.namespaces.configs.streams import ns_conf

import mindsdb.interfaces.storage.db as db
from mindsdb.interfaces.storage.db import session


STREAM_INTEGRATION_TYPES = ('kafka', 'redis')


def get_streams():
    streams = db.session.query(db.Stream).filter_by(company_id=request.company_id).all()
    streams_as_dicts = []
    for s in streams:
        streams_as_dicts.append({
            'name': s.name,
            'predictor': s.predictor,
            'integration': s.integration,
            'stream_in': s.stream_in,
            'stream_out': s.stream_out,
        })
    return streams_as_dicts


@ns_conf.route('/')
class StreamList(Resource):
    @ns_conf.doc("get_streams")
    def get(self):
        return {'streams': get_streams()}


@ns_conf.route('/<name>')
@ns_conf.param('name', 'Key-value storage stream')
class Stream(Resource):
    @ns_conf.doc("get_stream")
    def get(self, name):
        for stream in get_streams():
            if stream['name'] == name:
                return stream
        return abort(404, f'Stream "{name}" doesn\'t exist')

    @ns_conf.doc("put_stream")
    def put(self, name):
        for param in ['name', 'integration', 'predictor', 'stream_in', 'stream_out']:
            if param not in request.json.keys():
                return abort(400, 'Please provide "{}"'.format(param))

        integration = get_db_integration(request.json['integration'], request.company_id)
        if integration is None:
            return abort(404, 'Integration "{}" doesn\'t exist'.format(request.json['integration']))

        if db.session.query(db.Stream).filter_by(company_id=request.company_id, name=request.json['name']).first() is not None:
            return abort(404, 'Stream "{}" already exists'.format(request.json['name']))

        if db.session.query(db.Predictor).filter_by(company_id=request.company_id, name=request.json['predictor']).first() is None:
            return abort(404, 'Predictor "{}" doesn\'t exist'.format(request.json['predictor']))

        if integration['type'] not in STREAM_INTEGRATION_TYPES:
            return abort(400, 'Integration "{}" is not of type [{}]'.format(
                request.json['integration'],
                '/'.join(STREAM_INTEGRATION_TYPES)
            ))

        stream = db.Stream(
            company_id=request.company_id,
            name=request.json['name'],
            integration=request.json['integration'],
            predictor=request.json['predictor'],
            stream_in=request.json['stream_in'],
            stream_out=request.json['stream_out'],
        )
        session.add(stream)
        session.commit()

        return {'success': True}, 200

    @ns_conf.doc("delete_stream")
    def delete(self, name):
        if 'name' not in request.json:
            return abort(400, 'Please provide stream name')

        stream = db.session.query(db.Stream).filter_by(company_id=request.company_id, name=request.json['name']).first()
        if stream is None:
            return abort(404, 'Stream "{}" doesn\'t exist'.format(request.json['name']))

        stream.delete()
        db.session.commit()

        return {"success": True}, 200
