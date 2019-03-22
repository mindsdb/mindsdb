from mindsdb import Predictor


mdb = Predictor(name='photo_score_model12')

mdb.learn(from_data="~/mindsdb/integration_testing/image_testing/train.csv", to_predict=['Score'])
print('------------------------------------------------------------Done training------------------------------------------------------------')

predicted = mdb.predict(when_data="~/mindsdb/integration_testing/image_testing/predict.csv")
print('------------------------------------------------------------Preidiction output------------------------------------------------------------')
for val in predicted:
    print(val)
