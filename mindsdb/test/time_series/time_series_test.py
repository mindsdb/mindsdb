from mindsdb import MindsDB


class TimeSeriesTest:

    def __init__(self):
        self.mindsDb = MindsDB()

    def train(self):
        self.mindsDb.learn(
            predict='Main_Engine_Fuel_Consumption_MT_day',
            from_data='fuel.csv',
            model_name='my_fuel',

            # Time series arguments:

            order_by='Time',
            group_by='id',
            window_size=24, # just 24 hours

            # other arguments
            ignore_columns=['Row_Number']
        )

    def predict(self):
        result = self.mindsDb.predict(predict='Main_Engine_Fuel_Consumption_MT_day', model_name='fuel',
                                   from_data='fuel_predict.csv')

        # you can now print the results
        print('The predicted main engine fuel consumption')
        print(result.predicted_values)


if __name__ == "__main__":
    tTest = TimeSeriesTest()
    tTest.train()
