from mindsdb.api.http.namespaces.configs.predictors import ns_conf
from flask_restx import fields


quality_metric = ns_conf.model('QualityMetric', {
    'type': fields.String(required=False, description='The quality type', enum=['error', 'warning', 'info']),
    'score': fields.Float(required=False, description='The score on the specific metric value 0-1'),
    'description': fields.String(required=False, description='The quality metric description'),
    'warning': fields.String(required=False, description=''),
    'name': fields.String(required=False, description=''),
})
