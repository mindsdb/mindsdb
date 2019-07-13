from mindsdb import Predictor

mindsDb = Predictor("InsuranceModel")
mindsDb.learn(
    from_data="integration_testing/ins_train_indep_test/insu_train_indep_dep.csv",
    to_predict='PolicyStatus'
)
