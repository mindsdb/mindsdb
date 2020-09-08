from mindsdb.api.http.namespaces.configs.predictors import ns_conf
from flask_restx import fields


label_group = ns_conf.model('LabelGroup', {
    'group': fields.String(required=False, description='label name'),
    'members': fields.List(fields.String, required=False, description='members belonging to this group'),
})
