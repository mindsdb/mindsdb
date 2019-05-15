import mindsdb

model = mindsdb.Predictor(name='wine_model')
predictions = model.predict(when_data='wine_data_predict.tsv')

for index, prediction in enumerate(predictions):
    Cultivar = prediction['Cultivar']
    Cultivar_confidence = prediction['Cultivar_confidence']
    print(f'Predicted cultivar {Cultivar} for row with index {index}')
