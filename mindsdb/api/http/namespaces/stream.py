COMPANY_ID = None
import re
from mindsdb.integrations.base import integration
from mindsdb.api.mongo.responders import company_id
import os
from flask import request
from flask_restx import Resource, abort
from mindsdb.utilities.log import log
from mindsdb.api.http.namespaces.configs.streams import ns_conf

import mindsdb.interfaces.storage.db as db
from mindsdb.interfaces.storage.db import session


def get_streams():
    streams = db.session.query(db.Stream).filter_by(company_id=COMPANY_ID).all()
    streams_as_dicts = []
    for s in streams:
        streams_as_dicts.append({
            'name': s.name,
            'predictor': s.predictor,
            'integration': s.integration,
            'connection_info': s.connection_info,
            'stream_in': s.stream_in,
            'stream_out': s.stream_out,
            'stream_anomaly': s.stream_anomaly,
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
        for param in ['name', 'integration', 'predictor', 'connection_info', 'stream_in', 'stream_out', 'stream_anomaly']:
            if param not in request.json.keys():
                return abort(400, 'Please provide "{}"'.format(param))

        if db.session.query(db.Stream).filter_by(company_id=COMPANY_ID, name=request.json['name']).first() is not None:
            return abort(404, 'Stream "{}" already exists'.format(request.json['name']))

        if db.session.query(db.Integration).filter_by(company_id=COMPANY_ID, name=request.json['integration']).first() is None:
            return abort(404, 'Integration "{}" doesn\'t exist'.format(request.json['integration']))

        if db.session.query(db.Predictor).filter_by(company_id=COMPANY_ID, name=request.json['predictor']).first() is None:
            return abort(404, 'Predictor "{}" doesn\'t exist'.format(request.json['predictor']))

        stream = db.Stream(
            company_id=COMPANY_ID,
            name=request.json['name'],
            integration=request.json['integration'],
            predictor=request.json['predictor'],
            connection_info=request.json['connection_info'],
            stream_in=request.json['stream_in'],
            stream_out=request.json['stream_out'],
            stream_anomaly=request.json['stream_anomaly']
        )
        session.add(stream)
        session.commit()

        return {'success': True}, 200

    @ns_conf.doc("delete_stream")
    def delete(self, name):
        if 'name' not in request.json:
            return abort(400, 'Please provide stream name')

        stream = db.session.query(db.Stream).filter_by(company_id=COMPANY_ID, name=request.json['name']).first()
        if stream is None:
            return abort(404, 'Stream "{}" doesn\'t exist'.format(request.json['name']))

        stream.delete()
        db.session.commit()

        return {"success": True}, 200
