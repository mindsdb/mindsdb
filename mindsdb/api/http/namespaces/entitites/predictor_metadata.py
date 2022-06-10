from mindsdb.api.http.namespaces.configs.predictors import ns_conf
from flask_restx import fields
from collections import OrderedDict

predictor_query_params = OrderedDict([
    ('name', {
        'description': 'The predictor name',
        'type': 'string',
        'in': 'path',
        'required': True
    }),
    ('when', {
        'description': 'The query conditions: key - column, value - value',
        'type': 'object',
        'in': 'body',
        'required': True,
        'example': "{'number_of_rooms': 2,'number_of_bathrooms':1, 'sqft': 1190}"
    }),
])

upload_predictor_params = OrderedDict([
    ('file', {
        'description': 'file',
        'type': 'file',
        'in': 'FormData',
        'required': True
    })
])

put_predictor_metadata = ns_conf.model('PUTPredictorMetadata', {
    'data_source_name': fields.String(
        required=False,
        description='Datasource name. Outdated, will be removed soon.'
    ),
    'from': fields.Nested(
        ns_conf.model('PUTPredictorMetadata_from', {
            'datasource': fields.String(required=False, description='Name of datasource'),
            'query': fields.String(required=False, description='Query to datasource', )
        }),
        required=False,
        description='Source of data for predictor training'
    ),
    'to_predict': fields.String(
        required=True,
        description='Predicted field name'
    ),
    'kwargs': fields.Raw(default={})
})

put_predictor_params = OrderedDict([
    ('name', {
        'description': 'The predictor name',
        'type': 'string',
        'in': 'path',
        'required': True
    }),
    ('data_source_name', {
        'description': 'The data source name',
        'type': 'string',
        'in': 'body',
        'required': False
    }),
    ('datasource', {
        'description': '',
        'type': 'string',
        'in': 'body',
        'required': False,
        'example': '{"name": "mysql_ds", "query": "select * from db.table"}'
    }),
    ('to_predict', {
        'description': 'list of column names to predict',
        'type': 'array',
        'in': 'body',
        'required': True,
        'example': "['number_of_rooms', 'price']"
    }),
    ('kwargs', {
        'description': 'list of column names to predict',
        'type': 'object',
        'in': 'body',
        'required': False,
        'example': '{"advanced_args": {"use_selfaware_model": false}}'
    })
])
