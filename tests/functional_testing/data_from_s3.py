from mindsdb import Predictor, S3DS


mdb = Predictor(name='analyse_dataset_test_predictor')
mdb.analyse_dataset(from_data=S3DS(bucket_name='mindsdb-example-data', file_path='home_rentals.csv'))
