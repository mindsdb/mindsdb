from collections import OrderedDict
from flask_restx import fields
from mindsdb.api.http.namespaces.configs.datasources import ns_conf

get_datasource_rows_params = OrderedDict([
    ('page[size]', {
        'description': 'page size',
        'type': 'integer',
        'in': 'path',
        'required': False,
        'default': 10
    }),
    ('page[offset]', {
        'description': 'start record',
        'type': 'integer',
        'in': 'path',
        'required': False,
        'default': 0
    }),
    ('filter_{type}[{field}]', {
        'description': 'Filter for field with specified type. Type can be one of [like,in,nin,gt,lt,gte,lte,eq,neq]',
        'type': 'string',
        'in': 'path',
        'required': False,
        'default': None
    })
])

datasource_rows_metadata = ns_conf.model('RowsResponse', {
    'data': fields.List(fields.Raw, required=False, description='Filtered content of the datasource'),
    'rowcount': fields.Integer(required=False, description='Count of rows in dataset'),
    'columns_names': fields.List(fields.String, required=False, description='The name of the columns')
})

EXAMPLES = [{
    'data': [
        {
            'name': 'Mercury',
            'mass': 0.055,
            'radius': 0.3829
        }, {
            'name': 'Venus',
            'mass': 0.815,
            'radius': 0.9499
        }, {
            'name': 'Earth',
            'mass': 1.0,
            'radius': 1.0
        }
    ]
}]
