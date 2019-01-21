from mindsdb import *

# We tell mindsDB what we want to learn and from what data
MindsDB().learn(
    #from_file="real_estate_description.xlsx", # the path to the file where we can learn from
    from_file="shakes.train.numLines.csv", # the path to the file where we can learn from
    
    predict='行项目数', # the column we want to learn to predict given all the data in the file
    model_name='real_estate_desc' # the name of this model
)

