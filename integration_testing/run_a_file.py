from mindsdb import Predictor


mdb = Predictor(name='suicide_model')
mdb.learn(from_data="integration_testing/suicide.csv", to_predict='suicides_no')

# use the model to make predictions
result = Predictor(name='suicide_rates').predict(when={'country':'Greece','year':1981,'sex':'male','age':'35-54','population':300000})

# you can now print the results
print(result)
