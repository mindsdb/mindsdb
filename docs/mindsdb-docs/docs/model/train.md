# Train new model using Studio

Before training the new model, you must connect to a Datasource. To learn how to do that, check out:

* [Connect to MySQL](/datasources/mysql)
* [Connect to PostgreSQL](/datasources/mysql)
* [Connect to MariaDB](/datasources/mariadb)
* [Connect to ClickHouse](/datasources/clickhouse)
* [Connect to Microsoft SQL Server](/datasources/mssql)
* [Connect to MongoDB Atlas](/datasources/mongodb)
* [Connect to remote URL](/datasources/remote)
* [Upload local dataset](/datasources/local)
* [Connect to Snowflake](/datasources/snowflake)

### Basic mode

Training a new model from MindsDB Studio is quite easy:

1. From the left navigation menu, open the `Predictors` dashboard.
2. Click on the `TRAIN NEW` button.
3. In the `New Predictor` modal window:
    1. Select the `From` datasource option.
    2. Add the name of the model.
    3. Check the column name (feature) that you want to predict.
    4. Press `TRAIN`.

![Train model basic mode](/assets/predictors/train-basic.gif)

### Additional training options

In the `Advanced mode`, you can find a few additional options when training the Machine Learning model, such as using a GPU for training, excluding columns from training or changing the sample margin of error. To do this, inside the `New Predictor` modal window:

1. Select the `From` datasource option.
2. Add the name of the model.
3. Check the column name (feature) that you want to predict.
4. Press the `ADVANCED MODE` button and:
    1. Check the `USE GPU` checkbox.
    2. Check the columns that you want to exclude from model training.
    3. Add the `sample margin of error` value.
5. Press `TRAIN`.

![Train model advanced mode](/assets/predictors/train-advanced.gif)

#### Timeseries

To build a timeseries model, you need to select the `Yes, it is` timeseries checkbox inside the `Advanced Mode` section:

1. Select the `From` datasource option.
2. Add the name of the model.
3. Check the column name (feature) that you want to predict.
4. Press the `ADVANCED MODE` button and:
    1. Check the `Yes, it is.` checkbox.
    2. Select the `Order by:` value, the column based on which the data should be ordered.
    3. Select the `Group by:` value, the column based on which to group multiple entities in the timeseries data.
    4. Add the `Look Back Window:` value, the number of rows to `look back` into when making a prediction.
5. Press `TRAIN`.

![Train timeseries](/assets/predictors/train-timeseries.gif)


!!! Success "That's it :tada: :trophy:  :computer:"
    You have successfully trained a new Machine Learning model. The next step is to evaluate the [model quality](/model/quality).