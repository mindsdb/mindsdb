from mindsdb import Predictor

mdb = Predictor(name='wine_data_model')

mdb.learn(from_data="wine_data_train.tsv", to_predict=['Cultivar'])

#mdb = Predictor(name='titanic_model')
predicted = mdb.predict(when_data="wine_data_predict.tsv")
for index, prediction in enumerate(predicted):
    cultivar = int(prediction['Cultivar'])
    print(f'Predicted cultivar nr {cultivar} for row number {index} !')
