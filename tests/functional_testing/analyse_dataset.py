from mindsdb import Predictor


mdb = Predictor(name='metapredictor')
mdb.analyse_dataset(from_data="https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv")
