from mindsdb.api.http.namespaces.configs.predictors import ns_conf
from mindsdb.api.http.namespaces.entitites.column_metadata import column_metadata

from flask_restx import fields

nested_histogram_data = ns_conf.model('NestedHistogramData', {
    'x': fields.List(fields.String, required=False, description='Ordered labels'),
    #'y': fields.List(fields.Float, required=False, description='Count for each label'),
    'y': fields.List(fields.Raw, required=False, description='Count for each label'),
    'x_explained': fields.List(fields.List( fields.Nested(column_metadata)), required=False, description='Ordered list of lists where each element in the histogram has a list of column metadata only relevant to each  subset of data defined by the histogram bucket '),

})
