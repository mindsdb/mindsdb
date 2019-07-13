from mindsdb import Predictor


mdb = Predictor(name='rr')

mdb.learn(from_data="integration_testing/rr_ts_test/rr_readings_moving_final_train_dep_indep.csv", to_predict=['will_stand_in_next_30'])
print('------------------------------------------------------------Done training------------------------------------------------------------')

predicted = mdb.predict(when_data="integration_testing/rr_ts_test/rr_readings_moving_final_test_indep.csv")
print('------------------------------------------------------------Preidiction output------------------------------------------------------------')
for val in predicted:
    print(val)
