
from mindsdb import *


MindsDB().learn(
    predict = 'Main_Engine_Fuel_Consumption_MT_day',
    from_data = 'fuel.csv',
    model_name='fuel',

    # Time series arguments:

    order_by='Time',
    group_by='id',
    window_size=24, # just 24 hours

    # other arguments
    ignore_columns = ['Row_Number']
)



