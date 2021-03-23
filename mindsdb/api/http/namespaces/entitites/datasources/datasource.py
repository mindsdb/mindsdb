from flask_restx import fields
from collections import OrderedDict
import datetime

from mindsdb.api.http.namespaces.configs.datasources import ns_conf

datasource_column_metadata = ns_conf.model('DatasourceColumnMetadata', {
    'name': fields.String(required=False, description='The datasource name'),
    'type': fields.String(required=False, description='column data type', enum=['string', 'number', 'file', 'dict']),
    'file_type': fields.String(required=False, description='type of files (if column type is "file")', enum=['image', 'sound']),
    'dict': fields.List(fields.String, required=False, description='dict keys (if column type is "dict")')
})

datasource_metadata = ns_conf.model('DatasourceMetadata', {
    # Primary key
    'name': fields.String(required=False, description='The datasource name'),
    # other attributes
    'source_type': fields.String(required=False, description='file/url'),
    'source': fields.String(required=False, description='The datasource source (filename, url)'),
    'missed_files': fields.Boolean(required=False, description='indicates the presence of missed files'),
    'created_at': fields.DateTime(required=False, description='The time the datasource was created at'),
    'updated_at': fields.DateTime(required=False, description='The time the datasource was last updated at'),
    'row_count': fields.Integer(required=False, description='The number of rows in dataset'),
    'columns': fields.List(fields.Nested(datasource_column_metadata), required=False, description='columns description')
})

put_datasource_params = OrderedDict([
    ('name', {
        'description': 'The datasource name',
        'type': 'string',
        'in': 'path',
        'required': True
    }),
    ('source_type', {
        'description': 'type of datasource',
        'type': 'string',
        'enum': ['file','url'],
        'in': 'FormData',
        'required': True
    }),
    ('source', {
        'description': 'file name or url',
        'type': 'string',
        'in': 'FormData',
        'required': True
    }),
    ('file', {
        'description': 'datasource file',
        'type': 'file',
        'in': 'FormData',
        'required': False
    }),
])

EXAMPLES = [{
    'name': 'realty price',
    'source': 'https://raw.githubusercontent.com/mindsdb/mindsdb/master/docs/examples/basic/home_rentals.csv',
    'source_type': 'url',
    'missed_files': False,
    'created_at': datetime.datetime.now(),
    'updated_at': datetime.datetime.now(),
    'row_count': 5037,
    'columns': [{
        'name': 'number_of_rooms',
        'type': 'number'
    }, {
        'name': 'number_of_bathrooms',
        'type': 'number'
    }, {
        'name': 'sqft',
        'type': 'number'
    }, {
        'name': 'location',
        'type': 'string'
    }, {
        'name': 'days_on_market',
        'type': 'number'
    }, {
        'name': 'initial_price',
        'type': 'number'
    }, {
        'name': 'neighborhood',
        'type': 'string'
    }, {
        'name': 'rental_price',
        'type': 'number'
    }]
    # 'columns': [{
    #     'name': 'name',
    #     'type': 'string'
    # }, {
    #     'name': 'price',
    #     'type': 'number'
    # }, {
    #     'name': 'rooms count',
    #     'type': 'number'
    # }, {
    #     'name': 'photo',
    #     'type': 'file',
    #     'file_type': 'image'
    # }, {
    #     'name': 'street',
    #     'type': 'dict',
    #     'dict': ['east', 'west']
    # }]
}]
