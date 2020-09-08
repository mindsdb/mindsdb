from mindsdb.api.http.namespaces.configs.predictors import ns_conf

from mindsdb.api.http.namespaces.entitites.histogram_data import histogram_data
from mindsdb.api.http.namespaces.entitites.label_group import label_group

from flask_restx import fields

data_distribution_metadata = ns_conf.model('DataDistributionMetadata', {
    'data_histogram': fields.Nested(histogram_data, required=False, description='The histogram representing the data in this column if possible'),
    'clusters': fields.List(fields.Nested(label_group), required=False, description='The labels per cluster'),
    'mean': fields.String(required=False, description='The mean value if possible, encoded as string')
})