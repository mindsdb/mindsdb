from mindsdb.api.http.namespaces.configs.predictors import ns_conf

from flask_restx import fields


data_preparation_metadata = ns_conf.model('DataPreparationMetadata', {
    'accepted_margin_of_error': fields.Float(required=False, description='This is the margin of error that the user accepted when training the predictor, based on this we estimate how much data to actually sample from the provided data set'),
    'total_row_count': fields.Integer(required=False, description='The total number of rows found on the data set'),
    'used_row_count': fields.Integer(required=False, description='The number of rows sampled fro the entire dataset, this is calculated accordingly from the margin of error argument'),
    'test_row_count': fields.Integer(required=False, description='The number of rows used on the test subset'),
    'train_row_count': fields.Integer(required=False, description='The number of rows used on the train subset'),
    'validation_row_count': fields.Integer(required=False, description='The number of rows used on the validation subset')
})


EXAMPLE = {
    'accepted_margin_of_error': 0.2,
    'total_row_count': 18000,
    'used_row_count': 10000,
    'test_row_count': 1000,
    'validation_row_count': 1000,
    'train_row_count': 8000
}
