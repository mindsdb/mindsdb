from mindsdb.api.http.namespaces.configs.predictors import ns_conf
from mindsdb.api.http.namespaces.entitites.quality_metric import quality_metric
from flask_restx import fields

quality_dimension = ns_conf.model('QualityDimension', {
    'score': fields.String(required=False, description='The data quality score (0 to 10) derived from the metrics.'),
    'metrics': fields.List(fields.Nested(quality_metric), required=False, description='List of quality metrics evaluated'),
    'description': fields.String(required=False, description='The score description')
})
