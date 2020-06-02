from mindsdb.libs.data_sources.file_ds import FileDS
from mindsdb.libs.constants.mindsdb import DATA_TYPES, DATA_SUBTYPES


data_source = FileDS('https://raw.githubusercontent.com/mindsdb/mindsdb-examples/master/benchmarks/german_credit_data/processed_data/test.csv')
data_source.set_subtypes({})

data_source_mod = FileDS('https://raw.githubusercontent.com/mindsdb/mindsdb-examples/master/benchmarks/german_credit_data/processed_data/test.csv')
data_source_mod.set_subtypes({'credit_usage': 'Int', 'Average_Credit_Balance': 'Text','existing_credits': 'Binary Category'})

import mindsdb
analysis = mindsdb.Predictor('analyzer1').analyse_dataset(data_source)
analysis_mod = mindsdb.Predictor('analyzer2').analyse_dataset(data_source_mod)

a1 = analysis['data_analysis_v2']
a2 = analysis_mod['data_analysis_v2']
assert(len(a1) == len(a2))
assert(a1['over_draft']['typing']['data_type'] == a2['over_draft']['typing']['data_type'])

assert(a1['credit_usage']['typing']['data_type'] == a2['credit_usage']['typing']['data_type'])
assert(a1['credit_usage']['typing']['data_subtype'] != a2['credit_usage']['typing']['data_subtype'])
assert(a2['credit_usage']['typing']['data_subtype'] == DATA_SUBTYPES.INT)

assert(a1['Average_Credit_Balance']['typing']['data_type'] != a2['Average_Credit_Balance']['typing']['data_type'])
assert(a1['Average_Credit_Balance']['typing']['data_subtype'] != a2['Average_Credit_Balance']['typing']['data_subtype'])
assert(a2['Average_Credit_Balance']['typing']['data_subtype'] == DATA_SUBTYPES.Text)
assert(a2['Average_Credit_Balance']['typing']['data_type'] == DATA_TYPES.SEQUENTIAL)

assert(a1['existing_credits']['typing']['data_type'] == a2['existing_credits']['typing']['data_type'])
assert(a1['existing_credits']['typing']['data_subtype'] != a2['existing_credits']['typing']['data_subtype'])
assert(a2['existing_credits']['typing']['data_subtype'] == DATA_SUBTYPES.SINGLE)
