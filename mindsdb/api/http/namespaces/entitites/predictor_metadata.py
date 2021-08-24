from mindsdb.api.http.namespaces.configs.predictors import ns_conf
from mindsdb.api.http.namespaces.entitites.data_preparation_metadata import data_preparation_metadata, EXAMPLE as PREPARATION_METADATA_EXAMPLE
from mindsdb.api.http.namespaces.entitites.target_column_metadata import target_column_metadata  # , EXAMPLES as TARGET_COLUMN_METADATA_EXAMPLES
from flask_restx import fields
from collections import OrderedDict

predictor_metadata = ns_conf.model('PredictorMetadata', {
    # Primary key
    'status': fields.String(required=False, description='The current model status', enum=['training', 'complete', 'error']),
    'current_phase': fields.String(required=False, description='Current training phase'),
    'name': fields.String(required=False, description='The predictor name'),
    'version': fields.String(required=False, description='The predictor version to publish under, this is so that we can train multiple predictors for the same problem but expose them via the same name'),
    # other attributes
    'data_preparation': fields.Nested(data_preparation_metadata, required=False, description='The metadata used in the preparation stage, in which we break the data into train, test, validation'),
    'accuracy': fields.Float(description='The current accuracy of the model'),
    'train_data_accuracy': fields.Float(description='The current accuracy of the model', required=False),
    'test_data_accuracy': fields.Float(description='The current accuracy of the model', required=False),
    'valid_data_accuracy': fields.Float(description='The current accuracy of the model', required=False),
    'model_analysis': fields.List(fields.Nested(target_column_metadata), required=False, description='The model analysis stage, in which we extract statistical information from the input data for each target variable, thus, this is a list; one item per target column')
    ,'data_analysis_v2': fields.Raw(default={})
    ,'timeseries': fields.Raw()
    ,'data_source': fields.String(required=False, description='The data source it\'s learning from')
    ,'stack_trace_on_error': fields.String(required=False, description='Why it failed, if it did')
    ,'error_explanation': fields.String(required=False, description='Why it failed, if it did, short version')
    ,'useable_input_columns': fields.Raw()
})

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


put_predictor_params = OrderedDict([
    ('name', {
        'description': 'The predictor name',
        'type': 'string',
        'in': 'path',
        'required': True
    }),
    ('data_source_name', {
        'description': 'The predictor name',
        'type': 'string',
        'in': 'body',
        'required': True
    }),
    ('from_data', {
        'description': '',
        'type': 'string',
        'in': 'body',
        'required': False
    }),
    ('to_predict', {
        'description': 'list of column names to predict',
        'type': 'array',
        'in': 'body',
        'required': True,
        'example': "['number_of_rooms', 'price']"
    })
])
