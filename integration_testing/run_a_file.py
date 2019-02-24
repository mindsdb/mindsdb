from mindsdb import Predictor


mdb = Predictor(name='titanic_model')
mdb.learn(
    from_data="train.csv",
    to_predict='Survived',
)
