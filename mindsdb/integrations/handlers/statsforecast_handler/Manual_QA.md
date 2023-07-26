## Testing Nixtla's StatsForecast Handler in Cloud

### First test is with [House Sales dataset.](https://www.kaggle.com/datasets/htagholdings/property-sales)

**1. Testing CREATE ML_ENGINE**

CREATE ML_ENGINE engine_name
FROM statsforecast;

![1 create_engine](https://github.com/mindsdb/mindsdb/assets/32901682/473a7cda-019f-4e4e-a09c-81fe30831340)

**2. Testing CREATE MODEL**

`CREATE MODEL house_sales1
FROM files (select * from house_sales)
PREDICT MA as price
ORDER BY saledate
GROUP BY type
HORIZON 4
USING ENGINE = 'statsforecast';`

![2 create_model](https://github.com/mindsdb/mindsdb/assets/32901682/76083311-3e94-4c0b-86fc-fc7bfdb507dd)


**3. Testing SELECT FROM MODEL**

`SELECT a.saledate as date, a.price as forecast
  FROM mindsdb.house_sales1 as a 
  JOIN files.house_sales as b
  WHERE t.saledate > LATEST AND t.type = 'unit'
  AND t.bedrooms=2
  LIMIT 4;`
  
  Error: `For time series predictor only the following columns are allowed in WHERE: ['saledate', 'type'], found instead: b.bedrooms.`
![select_error](https://github.com/mindsdb/mindsdb/assets/32901682/3a97ec0b-d1b4-4856-8985-544e8e67ec04)

`SELECT a.saledate as date, a.MA as forecast
  FROM mindsdb.house_sales1 as a 
  JOIN files.house_sales as b
  WHERE b.saledate > LATEST AND b.type = 'unit'
  LIMIT 4;`
  
![3 Select](https://github.com/mindsdb/mindsdb/assets/32901682/f4e5cec8-4a6a-45b4-9e08-59139ed6d600)


### Results

Can only query 2 columns in the `WHERE` statement. When the Nixtla engine was created, it produced a message `query successfully completed`, but when I uploaded a dataset as a file and queried the data,the message in the console produced was 'engine for statsforcast already exists` instead of producing results for the file.

### Tests with the [Vegetables and Fruits dataset](https://www.kaggle.com/datasets/ramkrijal/agriculture-vegetables-fruits-time-series-prices)

**1. Testing CREATE MODEL**

`CREATE MODEL veg_fruit
FROM files (select * from fruit_veg)
PREDICT Maximum
ORDER BY Date
GROUP BY Commodity
HORIZON 100
USING ENGINE = 'statsforecast';`

![CREATE_MODEL](Image URL of the screenshot)

Error: `ValueError: could not broadcast input array from shape (41,) into shape (100,), raised at: /usr/local/lib/python3.8/dist-packages/mindsdb/integrations/libs/learn_process.py#104`
![Screenshot 2023-07-25 at 18-06-20 MindsDB Web](https://github.com/mindsdb/mindsdb/assets/32901682/4d9f51c9-511a-4243-9db7-e2b2eff4a87d)

`CREATE MODEL veg_fruit
FROM files (select * from fruit_veg)
PREDICT Maximum
ORDER BY Date
GROUP BY Commodity
HORIZON 20
USING ENGINE = 'statsforecast';
`
![Screenshot 2023-07-25 at 18-04-17 MindsDB Web](https://github.com/mindsdb/mindsdb/assets/32901682/ada0b743-33cf-4268-acf2-1ea83ffc21a0)


`CREATE MODEL veg_fruit
FROM files (select * from fruit_veg)
PREDICT Maximum
ORDER BY Date
GROUP BY Commodity
HORIZON 10
USING ENGINE = 'statsforecast';`

`Describe veg_fruit`

Error: ValueError: could not b![Screenshot 2023-07-25 at 18-10-49 MindsDB Web](https://github.com/mindsdb/mindsdb/assets/32901682/aa0ed80e-a3e7-4cb8-9dce-546c23b24c28)
roadcast input array from shape (2,) into shape (10,), raised at: /usr/local/lib/python3.8/dist-packages/mindsdb/integrations/libs/learn_process.py#104


### Results
Could not create a model. Team to investigate if it's an error with the dataset.

### Test with [Energy consumption dataset](https://www.kaggle.com/datasets/vitthalmadane/energy-consumption-time-series-dataset?select=KwhConsumptionBlower78_1.csv)

**1. Testing CREATE MODEL**

`CREATE MODEL energy_consumption
FROM files (select * from energy)
PREDICT Consumption
ORDER BY TxnDate
GROUP BY TxnTime
HORIZON 20
USING ENGINE = 'statsforecast';`

IndexError: index -21 is out of bounds for axis 0 with size 1, raised at: /usr/local/lib/python3.8/dist-packages/mindsdb/integrations/libs/learn_process.py#104

The date format was just `1 Jan 2022`,so I added double qoutes to the dataset and tried again.


![CREATE_MODEL](Image URL of the screenshot)



`CREATE MODEL energy_consumption8
FROM files (select * from energy8)
PREDICT Consumption
ORDER BY TxnDate
GROUP BY TxnTime
HORIZON 20
USING ENGINE = 'statsforecast';`

`describe model energy_consumption;`

IndexError: index -21 is out of bounds for axis 0 with size 1, raised at: /usr/local/lib/python3.8/dist-packages/mindsdb/integrations/libs/learn_process.py#104
![create model](https://github.com/mindsdb/mindsdb/assets/32901682/74bf151e-b7dd-4ef0-8876-0cac06205c13)


### Results
Unable to create a model with the given dataset.

### Test with [Gold Dataset](https://www.kaggle.com/datasets/arashnic/learn-time-series-forecasting-from-gold-price)

**1. Testing CREATE MODEL**

`CREATE MODEL gold_price
FROM files (select * from gold)
PREDICT Value
ORDER BY Date
GROUP BY Date
HORIZON 30
USING ENGINE = 'statsforecast';`

![1 create_model](https://github.com/mindsdb/mindsdb/assets/32901682/a33f3045-4c60-49d5-83ff-c7b13d532287)

`describe gold_price;`

![create_model_error](https://github.com/mindsdb/mindsdb/assets/32901682/ba8e9d7a-bfca-4462-9d03-bc266060bbf6)

Error: `KeyError: 'ds', raised at: /usr/local/lib/python3.8/dist-packages/mindsdb/integrations/libs/learn_process.py#104`

### Test with [Netflix stock Dataset](https://www.kaggle.com/datasets/jainilcoder/netflix-stock-price-prediction)

**1. Testing CREATE MODEL**

`CREATE MODEL netflix_stock
FROM files (select * from Netflix)
PREDICT High
ORDER BY Date
GROUP BY Volume
HORIZON 30
USING ENGINE = 'statsforecast';`

`describe netflix_stock;`

`IndexError: index -31 is out of bounds for axis 0 with size 1, raised at: /usr/local/lib/python3.8/dist-packages/mindsdb/integrations/libs/learn_process.py#104`
![Screenshot 2023-07-26 at 12-03-53 MindsDB Web](https://github.com/mindsdb/mindsdb/assets/32901682/f63b231b-44d8-4f9d-b057-58fbce829895)

### Results
Model takes a long time to generate, when pressing enter to type the DESCRIBE syntax in the editor the model gives an error(create syntax is not highlighted). Realised afterwards that the previous models created for this QA experienced the same. I let the model train until it's complete without intervening and it still produced the same error.

Drop a remark based on your observation.
- [ ] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
- [X] There's a Bug 🪲

[Nixtla- Error message not displaying correctly](https://github.com/mindsdb/mindsdb/issues/6934)
      
[Nixtla- Error while creating model ](https://github.com/mindsdb/mindsdb/issues/6937)

[Nixtla- WHERE clause error ](https://github.com/mindsdb/mindsdb/issues/6939) 

[Model fails when entering describe syntax into Editor(Cloud)](https://github.com/mindsdb/mindsdb/issues/6948) 


