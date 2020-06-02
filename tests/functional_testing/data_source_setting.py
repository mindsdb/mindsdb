from mindsdb.libs.data_sources.file_ds import FileDS
import mindsdb


data_source = FileDS('https://github.com/mindsdb/mindsdb-examples/blob/master/benchmarks/german_credit_data/processed_data/test.csv', data_subtypes={})

analysis = mindsdb.analyse_dataset(data_source)
print(analysis['data_analysis_v2'])
