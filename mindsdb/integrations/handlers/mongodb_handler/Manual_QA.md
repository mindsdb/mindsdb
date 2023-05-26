# Welcome to the MindsDB Manual QA Testing for MongoDB Handler

> **Please submit your PR in the following format after the underline below `Results` section. Don't forget to add an underline after adding your changes i.e., at the end of your `Results` section.**

## Testing MongoDB Handler with [Cars Data](https://www.kaggle.com/datasets/vijayaadithyanvg/car-price-predictionused-cars.)

**1. Testing CREATE DATABASE**

```
db.databases.insertOne({
  name: "cars_db", // database name
  engine: "mongodb", // database engine 
  connection_args: {
    "port": 27017, // default connection port
    "host": "mongodb+srv://<user>:<password>@clusterml.ighwwuf.mongodb.net/test", //connection host
    "database": "cars_db" // database connection          
    }
});
```

![create](https://user-images.githubusercontent.com/52546856/236434178-79d67c6f-5a47-49c0-8d2d-45f26a5b8738.png)


**2. Testing CREATE PREDICTOR**

```
db.predictors.insert({ 
  name: "cars_predictor", 
  predict: "Selling_Price", 
  connection: "cars_db", 
  "select_data_query": "db.cars.find()"
});
```
![m4](https://user-images.githubusercontent.com/52546856/236434316-acf14c52-892d-48d7-83ac-4a2efaa2c2e7.png)


![m3](https://user-images.githubusercontent.com/52546856/236434358-d3282e3b-efea-454d-a2fb-cd3619e13c64.png)


**3. Testing SELECT FROM PREDICTOR**

```
db.cars_predictor.find({Car_Name: "ritz", Fuel_Type: "Petrol"});
```
![m5](https://user-images.githubusercontent.com/52546856/236434412-8cb18d7c-39f9-42cc-95ff-a92c42ba9eb5.png)


### Results

Drop a remark based on your observation.
- [x] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
- [ ] There's a Bug 🪲 [Issue Title](URL To the Issue you created) ( This means you encountered a Bug. Please open an issue with all the relevant details with the Bug Issue Template)

---

## Testing MongoDB Handler with [COVID-19 Vaccination Trends Data](https://www.kaggle.com/datasets/utkarshx27/covid-19-vaccination-trends)

**1. Testing CREATE DATABASE**

```
db.databases.insertOne({
  name: "vaccine_test_db", // database name
  engine: "mongodb", // database engine 
  connection_args: {
    "port": 27017, // default connection port
    "host": "mongodb+srv://<user>:<password>@clusterml.e57k8ji.mongodb.net/Clusterml", //connection host
    "database": "Clusterml" // database connection          
    }
});
```

![create](https://github.com/ya-sh-vardh-an/mindsdb/assets/111492054/253309fd-0b5c-4d2d-b4b9-346346d9ad27)


**2. Testing CREATE PREDICTOR**

```
db.predictors.insertOne({ 
  name: "vaccinated_predict_model", 
  predict: "Series_Complete_Daily", 
  connection: "vaccine_test_db", 
  select_data_query: "db.covid.find({})"
});
```
![Create-model1](https://github.com/ya-sh-vardh-an/mindsdb/assets/111492054/a4f4be8f-36c4-4651-96f9-c179fdf2466a)
![create-model-predict-find-name--error](https://github.com/ya-sh-vardh-an/mindsdb/assets/111492054/dfcffb93-3352-4fc6-924f-cb944b69aa0e)
![create-model-predict-find-name--error(full)](https://github.com/ya-sh-vardh-an/mindsdb/assets/111492054/f2da5eb0-b3ab-4d52-9b35-461d5ce8a1ed)



**3. Testing SELECT FROM PREDICTOR**

```
db.vaccinated_predict_model.find({Location: "OR"});
```
![Predict-value--error](https://github.com/ya-sh-vardh-an/mindsdb/assets/111492054/4e74f84c-acb2-4f67-a361-d9ec21a83b65)
![error-can't-find-view](https://github.com/ya-sh-vardh-an/mindsdb/assets/111492054/4a50f728-3e23-46e3-988b-14cc6668a67a)


### Results

Drop a remark based on your observation.
- [ ] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
- [x] There's a Bug 🪲 [Error during Mongodb manual handling - Predict Target value step](https://github.com/mindsdb/mindsdb/issues/6313) ( This means you encountered a Bug. Please open an issue with all the relevant details with the Bug Issue Template)
---
