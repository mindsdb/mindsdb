Manual QA for Nixtla NeuralForecast.

This file contains Manual QA tests for Neuralforecast with House sales data

### Create ML_Engine for NeuralForecast

```sql
CREATE ML_ENGINE neuralforecast
FROM neuralforecast;
```

Result
![1 create_ml](https://github.com/mindsdb/mindsdb/assets/32901682/19639436-9a3c-41e0-abdc-c4b308b8977f)




### Create and train model

```sql
CREATE MODEL neuralforecast_housesales
FROM example_db
  (SELECT * FROM demo_data.house_sales)
PREDICT ma
ORDER BY saledate
GROUP BY bedrooms, type
WINDOW 8
HORIZON 4
USING engine='neuralforecast', 
frequency='Q';
```

Result
![2 create model](https://github.com/mindsdb/mindsdb/assets/32901682/7aaf51ed-ba4a-4a0d-938f-b2fd65b56893)


### Describe model

```sql
Describe mindsdb.neuralforecast;
```

Result
![Screenshot 2023-09-13 at 09-20-18 MindsDB Web](https://github.com/mindsdb/mindsdb/assets/32901682/33049bc2-a8e8-48f6-9a76-ccafbffa1dd5)

Results:
Unable to successfully create a model with Neuralforecast.

Drop a remark based on your observation.
- [ ] Works Great 💚 (This means that all the steps were executed successfully and the expected outputs were returned.)
- [X] There's a Bug 🪲 [Error when creating model with NeuralForecast](https://github.com/mindsdb/mindsdb/issues/7321)


