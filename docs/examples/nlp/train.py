from mindsdb import *

# We tell mindsDB what we want to learn and from what data
MindsDB().learn(
    from_file="real_estate_description.xlsx", # the path to the file where we can learn from
    predict='number_of_rooms', # the column we want to learn to predict given all the data in the file
    model_name='real_estate_desc' # the name of this model
)

