import csv
import random

from main.mindsdb import MindsDBController


class NewTest:

    def __init__(self):
        self.mindsDb = MindsDBController()

    def train(self):
        # self.mindsDb.learn(
        #     from_data="home_rentals.csv",  # the path to the file where we can learn from, (note: can be url)
        #     predict='rental_price',  # the column we want to learn to predict given all the data in the file
        #     model_name='my_rental'  # the name of this model
        # )

       self.mindsDb.learn(
            from_data="noFile.csv",  # the path to the file where we can learn from, (note: can be url)
            predict='Purchase',  # the column we want to learn to predict given all the data in the file
            model_name='purchase'  # the name of this model
        )

        # self.mindsDb.learn(
        #     from_data="blackFriday_output.csv",  # the path to the file where we can learn from, (note: can be url)
        #     predict='Product_Category_1',  # the column we want to learn to predict given all the data in the file
        #     model_name='ProductC1'  # the name of this model
        # )

    def predict(self):
        # result = self.mindsDb.predict(predict = 'rental_price', when = {'number_of_rooms': 4, 'sqft': 863,
        #                                   'days_on_market': 10}, model_name = 'my_rental')
        #
        # print('The predicted price is ${price} with {conf} confidence'.format(
        #     price=result.predicted_values[0]['rental_price'], conf=result.predicted_values[0]['prediction_confidence']))
        #
        # result = self.mindsDb.predict(predict='sqft', when={'number_of_rooms': 4}, model_name='my_area')
        #
        # print('The predicted area is {sqft} with {conf} confidence'.format(
        #     sqft=result.predicted_values[0]['sqft'], conf=result.predicted_values[0]['prediction_confidence']))
        # print(result.predicted_values)
        result = self.mindsDb.predict(predict='Purchase', when={'Gender': 'F', 'Age': 55, 'Occupation':0}, model_name = 'purchase')

        print('The predicted price capacity is ${price} with {conf} confidence'.format(
            price=result.predicted_values[0]['Purchase'], conf=result.predicted_values[0]['prediction_confidence']))
    def parseCSV(self):
        with open('blackFriday.csv', mode='r') as csv_file, open('blackFriday_output.csv', mode='w') as out_csv_file:
            # csv_reader = csv.DictReader(csv_file)
            line_count = 0
            a = csv_file.readline()
            out_csv_file.write(a+'\n')
            a = csv_file.readline()
            while(a!='\n'):
                print(a)
                if( len(a.split(','))<3):
                    break
                b = a.split(',')[3].split('-')
                if(len(b) > 1):
                    b1 = int(b[0].strip())
                    b2 = int(b[1].strip())
                    bnew = str(random.randint(b1, b2))
                    a = a.replace('-'.join(b), bnew)
                out_csv_file.write(a)
                a = csv_file.readline()

    def reader(self):
        "User_ID,Product_ID,Gender,Age,Occupation,City_Category,Stay_In_Current_City_Years,Marital_Status,Product_Category_1,Product_Category_2,Product_Category_3,Purchase"
        # with open('blackFriday_output.csv', mode='r') as csv_file:
        #     csv_reader = csv.DictReader(csv_file)
        #     line_count = 0
        #     for row in csv_reader:
        #         if line_count == 0:
        #             print(f'Column names are {", ".join(row)}')
        #             line_count += 1
        #         print(
        #             f'\t{row["User_ID"]}, {row["Product_ID"]} {row["Gender"], row["Age"]}, '
        #             f'{row["Occupation"]} {row["City_Category"], row["Stay_In_Current_City_Years"]}, '
        #             f'{row["Marital_Status"]} {row["Product_Category_1"], row["Product_Category_2"]}.'
        #             f'{row["Product_Category_3"]} {row["Purchase"]}.'
        #         )
        #         line_count += 1
        #     print(f'Processed {line_count} lines.')


if __name__ == "__main__":
    mTest = NewTest()
    # mTest.parseCSV()
    # mTest.reader()
    mTest.train()
    # mTest.predict()
