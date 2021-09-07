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
            'type': s.type,
            'connection': s.connection_info
        })
    return streams_as_dicts


@ns_conf.route('/')
class StreamList(Resource):
    @ns_conf.doc("get_streams")
    def get(self):
        return get_streams()


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
        # Back compatibility with previous endpoint version
        params = request.json.get('params') or request.json
        params_keys = params.keys()
        for param in ['predictor', 'stream_in', 'stream_out']:
            if param not in params_keys:
                return abort(400, 'Please provide "{}"'.format(param))
        if 'integration' not in params_keys and 'connection' not in params_keys:
            return abort(400, "'integration' in case of local installation and 'connection' in case of cloud are required.")

        if 'integration' in params_keys:
            integration = get_db_integration(params['integration'], request.company_id)
            if integration is None:
                return abort(404, 'Integration "{}" doesn\'t exist'.format(params['integration']))

            if integration['type'] not in STREAM_INTEGRATION_TYPES:
                return abort(400, 'Integration "{}" is not of type [{}]'.format(
                    params['integration'],
                    '/'.join(STREAM_INTEGRATION_TYPES)
                ))

        else:
            if 'type' not in params_keys:
                return abort(404, "'type' parameter is required in case of cloud.")

        if db.session.query(db.Stream).filter_by(company_id=request.company_id, name=name).first() is not None:
            return abort(404, 'Stream "{}" already exists'.format(name))

        if db.session.query(db.Predictor).filter_by(company_id=request.company_id, name=params['predictor']).first() is None:
            return abort(404, 'Predictor "{}" doesn\'t exist'.format(params['predictor']))
        

        stream = db.Stream(
            company_id=request.company_id,
            name=name,
            integration=params.get('integration'),
            predictor=params['predictor'],
            stream_in=params['stream_in'],
            stream_out=params['stream_out'],
            anomaly_stream=params.get('anomaly_stream'),
            learning_stream=params.get('learning_stream'),
            type = params.get('type'),
            connection_info = params.get('connection')
        )

        session.add(stream)
        session.commit()

        return {'success': True}, 200

    @ns_conf.doc("delete_stream")
    def delete(self, name):
        stream = db.session.query(db.Stream).filter_by(company_id=request.company_id, name=name).first()
        if stream is None:
            return abort(404, 'Stream "{}" doesn\'t exist'.format(name))
        db.session.delete(stream)
        db.session.commit()

        return {"success": True}, 200
