from mindsdb import Predictor


mdb = Predictor(name='titanic_model')

#mdb.learn(from_data="integration_testing/train.csv", to_predict=['Survived','Pclass'])

print('------------------------------------------------------------Done training------------------------------------------------------------')

#mdb = Predictor(name='titanic_model')
predicted = mdb.predict(when_data="integration_testing/train.csv")
print('------------------------------------------------------------Preidiction output------------------------------------------------------------')
for val in predicted:
    print(val)
exit()
print(predicted.predicted_values)
