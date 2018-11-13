from mindsdb import *


class MindsDBTest:

    def __init__(self):
        self.mindsDb = MindsDB()

    def train(self):
        self.mindsDb.learn(
            from_data="home_rentals.csv",  # the path to the file where we can learn from, (note: can be url)
            predict='rental_price',  # the column we want to learn to predict given all the data in the file
            model_name='my_rental'  # the name of this model
        )

        # self.mindsDb.learn(
        #     from_data="home_rentals.csv",  # the path to the file where we can learn from, (note: can be url)
        #     predict='neighborhood',  # the column we want to learn to predict given all the data in the file
        #     model_name='neighbor'  # the name of this model
        # )

    def predict(self):
        result = self.mindsDb.predict(predict = 'rental_price', when = {'number_of_rooms': 4, 'sqft': 863,
                                          'days_on_market': 10}, model_name = 'my_rental')

        print('The predicted price is ${price} with {conf} confidence'.format(
            price=result.predicted_values[0]['rental_price'], conf=result.predicted_values[0]['prediction_confidence']))

        # result = self.mindsDb.predict(predict='sqft', when={'number_of_rooms': 4, 'neighborhood': 'south_side', 'number_of_bathrooms':4},
        #                               model_name='my_area')
        #
        # print('The predicted area is {sqft} with {conf} confidence'.format(
        #     sqft=result.predicted_values[0]['sqft'], conf=result.predicted_values[0]['prediction_confidence']))
        # result = self.mindsDb.predict(predict='neighborhood', when={'rental_price': 10000}, model_name='neighbor')
        #
        # print('The predicted neighborhood is {neighborhood} with {conf} confidence'.format(
        #     neighborhood=result.predicted_values[0]['neighborhood'], conf=result.predicted_values[0]['prediction_confidence']))


if(__name__ == "__main__"):
    mTest = MindsDBTest()
    mTest.train()
    # mTest.predict()
