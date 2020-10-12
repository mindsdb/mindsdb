from mindsdb.api.http.namespaces.configs.predictors import ns_conf
from mindsdb.api.http.namespaces.entitites.data_preparation_metadata import data_preparation_metadata, EXAMPLE as PREPARATION_METADATA_EXAMPLE
from mindsdb.api.http.namespaces.entitites.data_analysis_metadata import data_analysis_metadata#, EXAMPLE as DATA_ANALYSIS_METADATA_EXAMPLE
from mindsdb.api.http.namespaces.entitites.target_column_metadata import target_column_metadata#, EXAMPLES as TARGET_COLUMN_METADATA_EXAMPLES
from flask_restx import fields
from collections import OrderedDict

predictor_metadata = ns_conf.model('PredictorMetadata', {
    # Primary key
    'status': fields.String(required=False, description='The current model status', enum=['training', 'complete', 'error']),
    'current_phase': fields.String(required=False, description='Current training phase'),
    'name': fields.String(required=False, description='The predictor name'),
    'version': fields.String(required=False, description='The predictor version to publish under, this is so that we can train multiple predictors for the same problem but expose them via the same name'),
    # other attributes
    'data_preparation': fields.Nested(data_preparation_metadata, required=False, description='The metadata used in the preparation stage, in which we break the data into train, test, validation'),
    'accuracy': fields.Float(description='The current accuracy of the model'),
    'train_data_accuracy': fields.Float(description='The current accuracy of the model',required=False),
    'test_data_accuracy': fields.Float(description='The current accuracy of the model', required=False),
    'valid_data_accuracy': fields.Float(description='The current accuracy of the model', required=False),
    'data_analysis': fields.Nested(data_analysis_metadata, required=False, description='The metadata used in the analysis stage, in which we extract statistical information from the input data'),
    'model_analysis': fields.List(fields.Nested(target_column_metadata), required=False, description='The model analysis stage, in which we extract statistical information from the input data for each target variable, thus, this is a list; one item per target column')
    ,'data_analysis_v2': fields.Raw(default={})
    ,'is_custom': fields.Boolean(default=False)
})

predictor_query_params = OrderedDict([
    ('name', {
        'description': 'The predictor name',
        'type': 'string',
        'in': 'path',
        'required': True
    }),
    ('when', {
        'description': 'The query conditions: key - column, value - value',
        'type': 'object',
        'in': 'body',
        'required': True,
        'example': "{'number_of_rooms': 2,'number_of_bathrooms':1, 'sqft': 1190}"
    }),
])

upload_predictor_params = OrderedDict([
    ('file', {
        'description': 'file',
        'type': 'file',
        'in': 'FormData',
        'required': True
    })
])

put_predictor_params = OrderedDict([
    ('name', {
        'description': 'The predictor name',
        'type': 'string',
        'in': 'path',
        'required': True
    }),
    ('data_source_name', {
        'description': 'The predictor name',
        'type': 'string',
        'in': 'body',
        'required': True
    }),
    ('from_data', {
        'description': '',
        'type': 'string',
        'in': 'body',
        'required': False
    }),
    ('to_predict', {
        'description': 'list of column names to predict',
        'type': 'array',
        'in': 'body',
        'required': True,
        'example': "['number_of_rooms', 'price']"
    })
])

# examples

EXAMPLES = [{
    'name': 'Price',
    'version': 1,
    'data_preparation': {
        'accepted_margin_of_error': 0.2,
        'total_row_count': 18000,
        'used_row_count': 10000,
        'test_row_count': 1000,
        'validation_row_count': 1000,
        'train_row_count': 8000
    },
    'data_analysis':  {
        'target_columns_metadata': [
            {
                'column_name': 'price',
                'data_type':  'numeric',
                'data_type_distribution': {
                    'type': 'categorical',
                    'x': ['numeric','categorical'],
                    'y': [19998,2]
                },
                'data_distribution': {
                    'data_histogram': {
                        'type': 'numeric',
                        'x': ['1000','1100','1200','1300','1400','1500','1600','1700','1800','1900','2000','2100', '2200', '2300', '2400'],
                        'y': [10,20,30,20,20,50,60,70,100,10,100,120,130,150,90]
                    },
                    'clusters': [
                        {
                            'group': 'low',
                            'members': ['1000','1100','1200','1300','1400','1500']
                        },
                        {
                            'group': 'medium',
                            'members': ['1600','1700','1800','1900']
                        },
                        {
                            'group': 'high',
                            'members': ['2000','2100', '2200', '2300', '2400']
                        },

                    ],
                    'mean': 1800
                },
                'consistency':  {
                    'score': '9/10',
                    'metrics': [
                        {
                            'type':  'warning',
                            'score': 0.3,
                            'description': 'There are two cells that dont match the numeric type, we will ignore them'
                        }
                    ],
                    'description': 'The data is in good shape, we have identified one consistency warning'
                },
                'redundancy': {
                    'score': '10/10'
                },
                'variability': {
                    'score': '8/10',
                    'metrics': [
                        {
                            'type':  'warning',
                            'score': 0.3,
                            'description': 'variability in this column was low, this means that there is a high bias towards properties in the 1k range'
                        },
                        {
                            'type':  'warning',
                            'score': 0.5,
                            'description': 'We have identified outliers in this column in the 30k price range'
                        }
                    ],
                    'description': 'The data is ok, we have identified one variability warnings'
                }

            }
        ],
        'input_columns_metadata': [
            {
                'column_name': 'sqft',
                'data_type':  'numeric',
                'data_type_distribution': {
                    'type': 'categorical',
                    'x': ['numeric'],
                    'y': [20000]
                },
                'data_distribution': {
                    'data_histogram': {
                        'type': 'numeric',
                        'x': ['100','150','200','250','300','350','400','450'],
                        'y': [10,20,30,20,20,50,60,70]
                    },
                    'mean': 1800
                },
                'consistency':  {
                    'score': '10/10'
                },
                'redundancy': {
                    'score': '10/10'
                },
                'variability': {
                    'score': '10/10'
                }

            },
            {
                'column_name': 'rooms',
                'data_type':  'numeric',
                'data_type_distribution': {
                    'type': 'categorical',
                    'x': ['numeric'],
                    'y': [20000]
                },
                'data_distribution': {
                    'data_histogram': {
                        'type': 'numeric',
                        'x': ['0','1','2','3','4','5'],
                        'y': [100,20,3000,200,2]
                    },
                    'mean': 2
                },
                'consistency':  {
                    'score': '10/10'
                },
                'redundancy': {
                    'score': '10/10'
                },
                'variability': {
                    'score': '9/10',
                    'metrics': [
                        {
                            'type':  'warning',
                            'score': 0.3,
                            'description': 'variability in this column was low, this means that there is a high bias towards properties in the 1k range'
                        },
                        {
                            'type':  'warning',
                            'score': 0.5,
                            'description': 'We have identified outliers in this column in the 30k price range'
                        }
                    ],
                    'description': 'The data is ok, we have identified one variability warnings'
                }

            }
        ]
    },
    'model_analysis': [
        {
            'column_name': 'price',
            'overall_input_importance': {
                'type': 'categorical',
                'x': ['sqft','rooms'],
                'y': [0.8, 0.2]
            },
            'train_accuracy_over_time':  {
                'type': 'numeric',
                'x': ['1','2', '3', '4', '5'],
                'y': [0.2, 0.2, 0.6, 0.8, 0.88]
            },
            'test_accuracy_over_time': {
                'type': 'numeric',
                'x': ['1','2', '3', '4', '5'],
                'y': [0.1, 0.12, 0.6, 0.8, 0.6]
            },
            'accuracy_histogram': {
                'x': ['1000','1800','1900', '2300', '2400'],
                'y': [0.9, 0.88, 0.99, 0.92, 0.34],
                'x_explained': [
                    [

                        {
                            'column_name': 'sqft',
                            'data_distribution': {
                                'data_histogram': {
                                    'type': 'numeric',
                                    'x': ['100','150','200','250','300','350','400','450'],
                                    'y': [10,0,0,0,2,5,6,7]
                                },
                                'mean': 1800
                            },
                            'consistency':  {
                                'score': '10/10'
                            },
                            'redundancy': {
                                'score': '10/10'
                            },
                            'variability': {
                                'score': '10/10'
                            }

                        },
                        {
                            'column_name': 'rooms',
                            'data_type':  'numeric',
                            'data_type_distribution': {
                                'type': 'categorical',
                                'x': ['numeric'],
                                'y': [20000]
                            },
                            'data_distribution': {
                                'data_histogram': {
                                    'type': 'numeric',
                                    'x': ['0','1','2','3','4','5'],
                                    'y': [0,0,300,20,100]
                                },
                                'mean': 2
                            },
                            'consistency':  {
                                'score': '10/10'
                            },
                            'redundancy': {
                                'score': '10/10'
                            },
                            'variability': {
                                'score': '9/10',
                                'metrics': [
                                    {
                                        'type':  'warning',
                                        'score': 0.3,
                                        'description': 'variability in this column was low, this means that there is a high bias towards properties in the 1k range'
                                    },
                                    {
                                        'type':  'warning',
                                        'score': 0.5,
                                        'description': 'We have identified outliers in this column in the 30k price range'
                                    }
                                ],
                                'description': 'The data is ok, we have identified one variability warnings'
                            }

                        }
                    ]

                ]


            }
        }
    ]
}]
