from mindsdb import *

# We tell mindsDB what we want to learn and from what data
<<<<<<< HEAD
MindsDB().learn(
    #from_file="real_estate_description.xlsx", # the path to the file where we can learn from
    from_file="shakes.train.numLines.csv", # the path to the file where we can learn from
    
    predict='行项目数', # the column we want to learn to predict given all the data in the file
=======
MindsDB().read_csv(filepath="shakes.train.numLines.csv" ,delimiter=',' ,encoding='GB18030')\
    .learn(
    #from_file="real_estate_description.xlsx", # the path to the file where we can learn from
    #predict='number_of_rooms', # the column we want to learn to predict given all the data in the file
    predict='行项目数',
>>>>>>> b01aedb6c79e9c23554c264e30235c1a32de4797
    model_name='real_estate_desc' # the name of this model
)

