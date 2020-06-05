from mindsdb.libs.constants.mindsdb import DATA_TYPES, DATA_SUBTYPES

test_column_types = {
    'numeric_int': (DATA_TYPES.NUMERIC, DATA_SUBTYPES.INT),
    'numeric_float': (DATA_TYPES.NUMERIC, DATA_SUBTYPES.FLOAT),
    'date_timestamp': (DATA_TYPES.DATE, DATA_SUBTYPES.TIMESTAMP),
    'date_date': (DATA_TYPES.DATE, DATA_SUBTYPES.DATE),
    'categorical_str': (DATA_TYPES.CATEGORICAL, DATA_SUBTYPES.MULTIPLE),
    'categorical_int': (DATA_TYPES.CATEGORICAL, DATA_SUBTYPES.MULTIPLE),
    'categorical_binary': (DATA_TYPES.CATEGORICAL, DATA_SUBTYPES.SINGLE),
    'sequential_array': (DATA_TYPES.SEQUENTIAL, DATA_SUBTYPES.ARRAY),
    'sequential_text': (DATA_TYPES.SEQUENTIAL, DATA_SUBTYPES.TEXT),
}