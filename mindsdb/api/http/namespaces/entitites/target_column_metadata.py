from mindsdb.api.http.namespaces.configs.predictors import ns_conf
from mindsdb.api.http.namespaces.entitites.histogram_data import histogram_data
from mindsdb.api.http.namespaces.entitites.nested_histogram_data import nested_histogram_data
from mindsdb.api.http.namespaces.entitites.confusion_matrix_data import confusion_matrix_data


from flask_restx import fields

target_column_metadata = ns_conf.model('TargetColumnMetadata', {
    'column_name': fields.String(required=False, description='The column name'),
    'overall_input_importance': fields.Nested(histogram_data, required=False, description='The overall predictor feature importance'),
    'train_accuracy_over_time': fields.Nested(histogram_data, required=False, description='The predictor train accuracy over time'),
    'test_accuracy_over_time': fields.Nested(histogram_data, required=False, description='The predictor test accuracy over time'),
    'accuracy_histogram': fields.Nested(nested_histogram_data, required=False, description='The predictor accuracy acrross values'),
    'confusion_matrix': fields.Nested(confusion_matrix_data, required=False, description='The predictor\'s confusion matrix for this column on the validation data'),
})
