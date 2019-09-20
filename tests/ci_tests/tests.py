from mindsdb import Predictor
import sys
import os


def basic_test(backend='ludwig',use_gpu=True,ignore_columns=[], run_extra=False):
    if run_extra:
        for py_file in [x for x in os.listdir('../functional_testing') if '.py' in x]:
            os.system(f'python3 ../functional_testing/{py_file}')

    # Create & Learn
    mdb = Predictor(name='home_rentals_price')
    mdb.learn(to_predict='rental_price',from_data="https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv",backend=backend, stop_training_in_x_seconds=20,use_gpu=use_gpu)

    # Reload & Predict
    mdb = Predictor(name='home_rentals_price')
    prediction = mdb.predict(when={'sqft':300}, use_gpu=use_gpu)

    # Test all different forms of output
    # No need to print them, we're just doing so for debugging purposes, we just want to see if the interface will crash or not

    print(prediction)
    print(prediction[0])

    for item in prediction:
        print(item)


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

    # See if we can get the adapted metadata
    amd = mdb.get_model_data('home_rentals_price')
    # Make some simple assertions about it
    assert(5 < len(list(amd.keys())))
