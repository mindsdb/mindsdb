import mindsdb
import sys
import os


def basic_test(backend='ludwig',use_gpu=True,ignore_columns=[], run_extra=False, IS_CI_TEST=False):
    mindsdb.CONFIG.IS_CI_TEST = IS_CI_TEST
    if run_extra:
        for py_file in [x for x in os.listdir('../functional_testing') if '.py' in x]:
            os.system(f'python3 ../functional_testing/{py_file}')

    # Create & Learn
    to_predict = 'rental_price'
    mdb = mindsdb.Predictor(name='home_rentals_price')
    #mdb.learn(to_predict=to_predict,from_data="https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv",backend=backend, stop_training_in_x_seconds=20,use_gpu=use_gpu)

    # Reload & Predict
    model_name = 'home_rentals_price'
    if run_extra:
        mdb.rename_model('home_rentals_price', 'home_rentals_price_renamed')
        model_name = 'home_rentals_price_renamed'

    mdb = mindsdb.Predictor(name=model_name)
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
    amd = mdb.get_model_data(model_name)
    # Make some simple assertions about it

    # @TODO: Sometimes are None, not sure why: [, validation_set_accuracy, accuracy]
    for k in ['status', 'name', 'version', 'data_source', 'current_phase', 'updated_at', 'created_at', 'train_end_at']:
        assert(type(amd[k]) == str)
    assert(type(amd['predict']) == list or type(amd['predict']) == str)
    assert(type(amd['is_active']) == bool)

    for k in amd['data_preparation']:
        assert(type(amd['data_preparation'][k]) == int or type(amd['data_preparation'][k]) == float)

    assert(type(amd['validation_set_accuracy']) == float)
    assert(type(amd['accuracy']) == float)

    for k in amd['data_analysis']:
        assert(len(amd['data_analysis'][k]) > 0)
        assert(type(amd['data_analysis'][k][0]) == dict)

    assert(len(amd['model_analysis']) > 0)
    assert(type(amd['model_analysis'][0]) == dict)

    for k in amd['force_vectors'][to_predict]['normal_data_distribution']:
        assert(len(amd['force_vectors'][to_predict]['normal_data_distribution'][k]) > 0)

    for k in amd['force_vectors'][to_predict]['missing_data_distribution']:
        for sk in amd['force_vectors'][to_predict]['missing_data_distribution'][k]:
            assert(len(amd['force_vectors'][to_predict]['missing_data_distribution'][k][sk]) > 0)
