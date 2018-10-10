
from mindsdb import *
import datetime


mdb = MindsDB()

# Load a datasource
data_source = CSVFileDS('fuel.csv')

########
# Perform some transformations to the data
########

# Remove Columns that we dont want to include
data_source.dropColumns(['Shaft Speed (RPM)', 'Shaft Torque (kNm)', 'Shaft Power (kW)'])
# Transform Date Column so that MindsDB can extract learn better
data_source.applyFunctionToColumn('Time', lambda x: datetime.datetime.fromtimestamp(int(x)).isoformat())
# Give it a column that we can group by (MindsDB needs a group by column if we are going to do time series)
data_source['id'] = 1


########
# Start training
########

mdb.learn(
    from_data = data_source,
    predict = 'Main_Engine_Fuel_Consumption_MT_day',
    order_by='Time',
    group_by='id',
    window_size=24, # just 24 hours
    model_name='fuel'
)



