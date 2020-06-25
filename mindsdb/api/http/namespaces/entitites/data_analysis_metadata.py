from mindsdb.api.http.namespaces.configs.predictors import ns_conf
from mindsdb.api.http.namespaces.entitites.column_metadata import column_metadata

from flask_restx import fields

data_analysis_metadata = ns_conf.model('PredictorDataAnalysisMetadata', {
    'target_columns_metadata': fields.List( fields.Nested(column_metadata), required=False, description='The number of rows used on the validation subset'),
    'input_columns_metadata': fields.List( fields.Nested(column_metadata), required=False, description='The number of rows used on the validation subset')
})


