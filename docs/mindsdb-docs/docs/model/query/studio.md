# Query the model using MindsDB Studio

To get predictive analytics from the trained model:

1. From the left navigation menu, open the `Query` dashboard.
2. Click on the `NEW QUERY` button.
3. In the `New Query` modal window:
    1. Select the `From` predictor option(the name of the pre-trained model).
    2. Add the feature values for which you want to get predictions.

## Example: querying the UsedCars model

In this example we want to solve the problem of estimating the right price for a used car that has Diesel as a fuel type and has done around 20 000 miles. To get the price:

1. From the predictors dashboard, click on the `Query` button.
2. Click on the `NEW QUERY` button.
3. In the `New Query` modal window:
    1. Add the feature values for which you want to get predictions, e.g.
        1. Mileage 20 000.
        2. Fuel type Diesel.

![Query from Studio](/assets/predictors/query-scout.gif)

