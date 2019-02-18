from mindsdb import Predictor


Predictor(name='fuel').learn(
    to_predict='Main_Engine_Fuel_Consumption_MT_day',
    from_data = 'fuel.csv',

    # Time series arguments:

    order_by='Time',
    group_by='id',
    window_size=24, # just 24 hours

)