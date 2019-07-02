from mindsdb import Predictor
import sys

if len(sys.argv) > 1:
    backend = sys.argv[1]
else:
    backend = 'ludwig'

mdb = Predictor(name='home_rentals_price')


mdb.learn(to_predict='rental_price',from_data="https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv",backend=backend)
#mdb.learn(to_predict='rental_price',from_data="docs/examples/basic/home_rentals.csv",backend=backend)

prediction = mdb.predict(when={'sqft':300})
print(prediction[0])
print(list(map(lambda x: int(x['rental_price']), prediction)))
amd = mdb.get_model_data('home_rentals_price')

with open('out.txt','w') as f:
    f.write(str(amd))
