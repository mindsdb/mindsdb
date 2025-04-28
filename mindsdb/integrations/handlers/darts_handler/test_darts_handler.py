import pandas as pd
from darts import TimeSeries
from darts.models import AutoARIMA
from mindsdb.integrations.libs.base import BaseMLEngine
from darts_handler import DartsModel

# Create a DataFrame from the given dataset Using house_sales sample data
df = pd.read_csv("house_sales.csv", parse_dates=["date"])

# Create a DartsModel object
darts_model = DartsModel()

# Split the data into training and testing sets
train_df = df[df["date"] < "2019-09-30"]
test_df = df[df["date"] >= "2019-09-30"]

# Train the model on the training set
darts_model.create(target="price", df=train_df, args={
    "timeseries_settings": {
        "is_timeseries": True,
        "order_by": "date"
    }
})

# Make predictions on the testing set
prediction_df = darts_model.predict(df=test_df, args={
    "timeseries_settings": {
        "is_timeseries": True,
        "order_by": "date"
    }
})

# Evaluate the model performance
print(prediction_df.describe())
