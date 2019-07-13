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

# Test all different forms of output
print(prediction)
print(prediction[0])

print(type(list(prediction.evaluations.values())[0][0]))
assert('ProbabilityEvaluation' in str(type(list(prediction.evaluations.values())[0][0])))

for p in prediction:
    print(p)
print(prediction[0].as_dict())
print(prediction[0].as_list())
print(prediction[0]['rental_price_confidence'])
print(type(prediction[0]['rental_price_confidence']))

print('\n\n========================\n\n')
print(prediction[0].explain())
print('\n\n')

amd = mdb.get_model_data('home_rentals_price')

with open('out.txt','w') as f:
    f.write(str(amd))
