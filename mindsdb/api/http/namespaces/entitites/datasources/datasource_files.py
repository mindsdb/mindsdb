from collections import OrderedDict

put_datasource_file_params = OrderedDict([
    ('name', {
        'description': 'The datasource name',
        'type': 'string',
        'in': 'path',
        'required': True
    }),
    ('column_name', {
        'description': 'Column name',
        'type': 'string',
        'in': 'path',
        'required': True
    }),
    ('index', {
        'description': 'row number',
        'type': 'integer',
        'in': 'path',
        'required': True
    }),
    ('extension', {
        'description': 'file extension',
        'type': 'string',
        'in': 'FormData',
        'required': False
    }),
    ('file', {
        'description': 'file',
        'type': 'file',
        'in': 'FormData',
        'required': True
    })
])
