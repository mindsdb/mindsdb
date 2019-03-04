from mindsdb import Predictor


mdb = Predictor(name='photo_score_model2')

#mdb.learn(from_data="train.csv", to_predict=['Score'])
print('------------------------------------------------------------Done training------------------------------------------------------------')

predicted = mdb.predict(when_data="predict.csv")
print('------------------------------------------------------------Preidiction output------------------------------------------------------------')
for val in predicted:
    print(val)
