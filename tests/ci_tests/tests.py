import mindsdb
import os
import unittest


class BasicTest(unittest.TestCase):
    backend = 'ludwig'
    use_gpu = True
    use_gpu = True
    ignore_columns = []
    run_extra = False
    IS_CI_TEST = False
    amd = {}
    to_predict = 'rental_price'

    @classmethod
    def setUpClass(cls):
        mindsdb.CONFIG.IS_CI_TEST = cls.IS_CI_TEST
        if cls.run_extra:
            for py_file in [x for x in os.listdir('../functional_testing') if '.py' in x]:
                os.system(f'python3 ../functional_testing/{py_file}')

        # Create & Learn
        model_name = 'home_rentals_price'
        mdb = mindsdb.Predictor(name=model_name)
        mdb.learn(to_predict=cls.to_predict,
                  from_data="https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv",
                  backend=cls.backend, stop_training_in_x_seconds=20, use_gpu=cls.use_gpu)

        # Reload & Predict

        if cls.run_extra:
            mdb.rename_model('home_rentals_price', 'home_rentals_price_renamed')
            model_name = 'home_rentals_price_renamed'

        mdb = mindsdb.Predictor(name=model_name)
        prediction = mdb.predict(when={'sqft': 300}, use_gpu=cls.use_gpu)

        # Test all different forms of output
        # No need to print them, we're just doing so for debugging purposes, we just want to see if the interface will crash or not

        print(prediction)
        print(prediction[0])

        for item in prediction:
            print(item)

        print(type(list(prediction.evaluations.values())[0][0]))
        assert ('ProbabilityEvaluation' in str(type(list(prediction.evaluations.values())[0][0])))

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
        cls.amd = mdb.get_model_data(model_name)

    def test_adapted_meta_data_basic(self):
        amd = self.amd
        # @TODO: Sometimes are None, not sure why: [, validation_set_accuracy, accuracy]
        for k in ['status', 'name', 'version', 'data_source', 'current_phase', 'updated_at', 'created_at',
                  'train_end_at']:
            assert (type(amd[k]) == str)

        assert (type(amd['predict']) == list or type(amd['predict']) == str)
        assert (type(amd['is_active']) == bool)

        for k in ['validation_set_accuracy', 'accuracy']:
            assert (type(amd[k]) == float)

        for k in amd['data_preparation']:
            assert (type(amd['data_preparation'][k]) == int or type(amd['data_preparation'][k]) == float)

    def test_data_analysis(self):
        data_analysis = self.amd['data_analysis']
        for k in data_analysis:
            assert (len(data_analysis[k]) > 0)
            assert (type(data_analysis[k][0]) == dict)

    def test_model_analysis(self):
        model_analysis = self.amd['model_analysis']
        assert (len(model_analysis) > 0)
        assert (type(model_analysis[0]) == dict)

    def test_column_importance(self):
        input_importance = self.amd['model_analysis'][0]["overall_input_importance"]
        assert (len(input_importance) > 0)
        assert (type(input_importance) == dict)

        for column, importance in zip(input_importance["x"], input_importance["y"]):
            assert (type(column) == str)
            assert (len(column) > 0)

            assert (type(importance) == float or type(importance) == int)
            assert (importance > 0)

    def test_force_vectors(self):
        force_vectors = self.amd['force_vectors']
        for k in force_vectors[self.to_predict]['normal_data_distribution']:
            assert (len(force_vectors[self.to_predict]['normal_data_distribution'][k]) > 0)

        for k in force_vectors[self.to_predict]['missing_data_distribution']:
            for sk in force_vectors[self.to_predict]['missing_data_distribution'][k]:
                assert (len(force_vectors[self.to_predict]['missing_data_distribution'][k][sk]) > 0)

    def test_data_preparation(self):
        data_preparation = self.amd['data_preparation']
        assert (data_preparation['total_row_count'] == data_preparation['train_row_count'] +
                data_preparation['test_row_count'] + data_preparation['validation_row_count'])
        for k, count in data_preparation.items():
            assert (type(count) == int or type(count) == float)
            if k != "accepted_margin_of_error":
                assert (count > 0)


def basic_test(backend='ludwig', use_gpu=True, ignore_columns=[], run_extra=False, IS_CI_TEST=False):
    BasicTest.backend = backend
    BasicTest.use_gpu = use_gpu
    BasicTest.ignore_columns = ignore_columns
    BasicTest.run_extra = run_extra
    BasicTest.IS_CI_TEST = IS_CI_TEST
    unittest.main()


if __name__ == "__main__":
    basic_test(use_gpu=False)
