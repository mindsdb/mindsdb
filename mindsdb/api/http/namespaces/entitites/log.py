from mindsdb.api.http.namespaces.configs.predictors import ns_conf
from flask_restx import fields


log = ns_conf.model('Log', {
    'source': fields.String(required=False, description='API where log produced'),
    'date': fields.Float(required=False, description='UNIX timestamp'),
    'level': fields.String(required=False, description="Log level, same as python 'logging' library levels"),
    'msg': fields.String(required=False, description='log message')
})
