from mindsdb.api.http.namespaces.configs.predictors import ns_conf
from mindsdb.api.http.namespaces.entitites.column_metadata import column_metadata

from flask_restx import fields

confusion_matrix_data = ns_conf.model('ConfusionMatrixData', {
    'matrix': fields.List(fields.List(fields.Integer, required=True)),
    'predicted': fields.List(fields.String, required=False, description='Predicted values'),
    'real': fields.List(fields.String, required=False, description='Real values'),
})
