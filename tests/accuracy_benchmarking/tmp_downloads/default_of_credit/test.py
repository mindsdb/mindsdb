from mindsdb import Predictor


test_file = 'test_sample.csv'

def get_real_test_data():
    test_reader = csv.reader(open(test_file, 'r'))
    next(test_reader, None)
    test_rows = [x for x in test_reader]
    return list(map(lambda x: int(x[-1]), test_rows))

mdb = Predictor(name='default_on_credit_dp4')

predictions = mdb.predict(when_data=test_file)

for p in predictions:
    print(p.epitomize())
