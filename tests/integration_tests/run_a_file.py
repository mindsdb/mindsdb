from mindsdb import Predictor


class Insurance:

   def __init__(self):
       self.mindsDb = Predictor(name='insurance')

   def insurance_train(self):
       self.mindsDb.learn(to_predict='PolicyStatus',
                          from_data="insu_replicate.csv",
                          order_by=['DateRequested',
                                    'DateRqmtLastFollowed1',
                                    'DateRqmtLastFollowed2',
                                    'DateRqmtLastFollowedF',
                                    'DateRqmtLastFollowed3',
                                    'DateSignedOff'],
                                    window_size_samples=4)
if __name__ == "__main__":
   tTest = Insurance()
   tTest.insurance_train()
