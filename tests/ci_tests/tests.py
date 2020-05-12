import mindsdb
import sys
import os
from sklearn.metrics import r2_score

def test_data_analysis(amd, to_predict):
    data_analysis = amd['data_analysis']
    for k in data_analysis:
        assert (len(data_analysis[k]) > 0)
        assert isinstance(data_analysis[k][0], dict)

def test_model_analysis(amd, to_predict):
    model_analysis = amd['model_analysis']
    assert (len(model_analysis) > 0)
    assert isinstance(model_analysis[0], dict)
    input_importance = model_analysis[0]["overall_input_importance"]
    assert (len(input_importance) > 0)
    assert isinstance(input_importance, dict)

    for column, importance in zip(input_importance["x"], input_importance["y"]):
        assert isinstance(column, str)
        assert (len(column) > 0)

        assert isinstance(importance, (float, int))
        assert (importance > 0)

def test_force_vectors(amd, to_predict):
    force_vectors = amd['force_vectors']
    for k in force_vectors[to_predict]['normal_data_distribution']:
        assert (len(force_vectors[to_predict]['normal_data_distribution'][k]) > 0)

    for k in force_vectors[to_predict]['missing_data_distribution']:
        for sk in force_vectors[to_predict]['missing_data_distribution'][k]:
            assert (len(force_vectors[to_predict]['missing_data_distribution'][k][sk]) > 0)

def test_adapted_model_data(amd, to_predict):
    amd = amd
    for k in ['status', 'name', 'version', 'data_source', 'current_phase', 'updated_at', 'created_at',
              'train_end_at']:
        assert isinstance(amd[k], str)

    assert isinstance(amd['predict'], (list, str))
    assert isinstance(amd['is_active'], bool)

    for k in ['validation_set_accuracy', 'accuracy']:
        assert isinstance(amd[k], float)

    for k in amd['data_preparation']:
        assert isinstance(amd['data_preparation'][k], (int, float))
    test_data_analysis(amd, to_predict)
    test_model_analysis(amd, to_predict)
    #test_force_vectors(amd, to_predict)


def basic_test(backend='lightwood',use_gpu=True, run_extra=False, IS_CI_TEST=False):
    mindsdb.CONFIG.IS_CI_TEST = IS_CI_TEST
    if run_extra:
        for py_file in [x for x in os.listdir('../functional_testing') if '.py' in x]:
            # Skip data source tests since installing dependencies is annoying
            # @TODO: Figure out a way to make travis install required dependencies on osx

            ctn = False
            for name in ['all_data_sources', 'custom_model']:
                if name in py_file:
                    ctn = True
            if ctn:
                continue
            
            code = os.system(f'py ../functional_testing/{py_file}')
            if code != 0:
                raise Exception(f'Test failed with status code: {code} !')

    # Create & Learn
    to_predict = 'rental_price'
    mdb = mindsdb.Predictor(name='home_rentals_price')
    mdb.learn(to_predict=to_predict,from_data="https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv",backend=backend, stop_training_in_x_seconds=120,use_gpu=use_gpu)

    # Reload & Predict
    model_name = 'home_rentals_price'
    if run_extra:
        mdb.rename_model('home_rentals_price', 'home_rentals_price_renamed')
        model_name = 'home_rentals_price_renamed'

    mdb = mindsdb.Predictor(name=model_name)
    # Try predicting from a file and from a dictionary
    prediction = mdb.predict(when_data="https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv", use_gpu=use_gpu)

    mdb.test(when_data="https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv",accuracy_score_functions=r2_score,predict_args={'use_gpu': use_gpu})

    prediction = mdb.predict(when={'sqft':300}, use_gpu=use_gpu)

    # Test all different forms of output
    # No need to print them in order to run these checks, we're just doing so for quick-debugging purposes, we just want to see if the interfaces will crash when we call them
    print(prediction)
    print(prediction[0])

    for item in prediction:
        print(item)


    for p in prediction:
        print(p)
    print(prediction[0].as_dict())
    print(prediction[0].as_list())
    print(prediction[0]['rental_price_confidence'])
    print(type(prediction[0]['rental_price_confidence']))

    print('\n\n========================\n\n')
    print(prediction[0].explain())
    print(prediction[0].explanation)
    print(prediction[0].raw_predictions())
    print('\n\n')

    # See if we can get the adapted model data
    amd = mdb.get_model_data(model_name)
    test_adapted_model_data(amd, to_predict)

if __name__ == "__main__":
    basic_test()
