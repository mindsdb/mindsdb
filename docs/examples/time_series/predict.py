"""

"""

from mindsdb import Predictor

# Here we use the model to make predictions (NOTE: You need to run train.py first)
result = Predictor(name='fuel').predict(when_data = 'fuel_predict.csv')

# you can now print the results
print('The predicted main engine fuel consumption')
for row in result:
  print(row)
