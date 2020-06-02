from mindsdb.libs.data_sources.file_ds import FileDS



data_source = FileDS('https://raw.githubusercontent.com/mindsdb/mindsdb-examples/master/benchmarks/german_credit_data/processed_data/test.csv')
data_source.set_subtypes({})

import mindsdb
analysis = mindsdb.analyse_dataset(data_source)
print(analysis['data_analysis_v2'])
