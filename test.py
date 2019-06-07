from mindsdb import Predictor
import sys

if len(sys.argv) > 1:
    backend = sys.argv[1]
else:
    backend = 'ludwig'

mdb = Predictor(name='home_rentals_price')

mdb.learn(
    to_predict='rental_price'
    ,from_data="https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv"
    ,disable_optional_analysis=False
    ,backend=backend
)

prediction = mdb.predict(when={'sqft':300})
print(prediction[0])
amd = mdb.get_model_data('home_rentals_price')
#print(amd)
