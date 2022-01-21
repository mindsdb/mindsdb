# Train time series model example

To train new timeseries model using `SQL` you will need to insert an object to the `training_options` coulmn.

```sql
INSERT INTO mindsdb.predictors 
            (name, 
             predict, 
             select_data_query, 
             training_options) 
VALUES     ('model_name', 
            'target_variable', 
            'SELECT * FROM table', 
            '{ "timeseries_settings": {                
                "order_by": ["order"],                
                "group_by": ["group"],                
                "horizon": 1,                
                "use_previous_target": True,                
                "window": 10            
            }}' 
            ); 
```

You can specify the following `keys` inside the `timeseries_settings`:

* order_by (list of strings) -- The list of columns names on which the data should be ordered.
* group_by (list of strings) -- The list of columns based on which to group multiple unrelated entities present in your timeseries data.
* horizon (int) --
* use_previous_target (Boolean) -- Use the previous values of the target column[s] for making predictions. Defaults to True
* window (int) --  The number of rows to "look back" into when making a prediction, after the rows are ordered by the order_by column and split into groups. Could be used to specify something like "Always use the previous 10 rows". 

Please, check the following example to get more info. The table used for training the model is the [Air Pollution in Seoul](https://www.kaggle.com/bappekim/air-pollution-in-seoul) timeseries dataset.

 ```sql
 INSERT INTO mindsdb.predictors(name, predict, select_data_query,training_options)
 VALUES('airq_model', 'SO2', 'SELECT * FROM default.pollution_measurement', '{"timeseries_settings":{"order_by": ["Measurement date"], "window":20}}');
 ```

 This `INSERT` query will train a new model called `airq_model` that predicts the passenger `SO2` value.
 Since, this is a timeseries dataset, the `timeseries_settings` will order the data by the `Measurement date` column and will set the window for rows to **look back** when making a prediction.


