from mindsdb import *

mdb = Predictor(name='real_estate_desc')

# We tell mindsDB what we want to learn and from what data
mdb.learn(
    from_data="real_estate_description.xlsx",
    to_predict='行项目数'
)
