from flask_restx import fields
from collections import OrderedDict

from mindsdb.api.http.namespaces.configs.datasources import ns_conf

get_datasource_missed_files_params = OrderedDict([
    ('page[size]', {
        'description': 'page size',
        'type': 'integer',
        'in': 'path',
        'required': False,
        'default': 0
    }),
    ('page[number]', {
        'description': 'start page',
        'type': 'integer',
        'in': 'path',
        'required': False,
        'default': 0
    })
])

datasource_missed_file_metadata = ns_conf.model('DatasourceMissedFile', {
    'column_name': fields.String(required=False, description='file column name'),
    'index': fields.Integer(required=False, description='row index in datasource'),
    'path': fields.String(required=False, description='file column value')
})

datasource_missed_files_metadata = ns_conf.model('DatasourceMissedFiles', {
    'rowcount': fields.Integer(required=False, description='number of missed files'),
    'data': fields.List(fields.Nested(datasource_missed_file_metadata), required=False, description='columns description')
})

EXAMPLES = [{
    'rowcount': 2,
    'data': [{
        'column_name': 'file',
        'index': 3,
        'path': '/tmp/3.jpg'
    }, {
        'column_name': 'file',
        'index': 4,
        'path': '/tmp/4.jpg'
    }]
}]
