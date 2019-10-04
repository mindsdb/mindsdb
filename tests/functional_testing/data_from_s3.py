from mindsdb import Predictor


mdb = Predictor(name='analyse_dataset_test_predictor')
mdb.analyse_dataset(from_data="https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv")
