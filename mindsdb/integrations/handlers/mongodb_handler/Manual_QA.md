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

![CREATE-DATABASE](https://github.com/ya-sh-vardh-an/mindsdb/assets/111492054/cf9b5f4e-7498-4e94-a515-61872982ddc4)


**2. Testing CREATE PREDICTOR**

```
db.predictors.insertOne({ 
  name: "vaccinated_predict_model", 
  predict: "Series_Complete_Daily", 
  connection: "vaccine_test_db", 
  select_data_query: "db.covid.find({})"
});
```
![CREATE-MODEL](https://github.com/ya-sh-vardh-an/mindsdb/assets/111492054/1b23bc68-6a70-42e7-b259-b88b26ab798c)
![CREATE-MODEL-STATUS](https://github.com/ya-sh-vardh-an/mindsdb/assets/111492054/d939bf0f-ba7f-48ee-80ca-1909784d51ba)


**3. Testing SELECT FROM PREDICTOR**

```
db.vaccinated_predict_model.find({Location: "OR", Administered_Daily: 5506});
```
![PREDICT-VALUE1](https://github.com/ya-sh-vardh-an/mindsdb/assets/111492054/6c6f7171-8c12-465e-b032-bda2ae58dfda)
![PREDICT-VALUE2](https://github.com/ya-sh-vardh-an/mindsdb/assets/111492054/650c6c9a-20e4-43cd-891b-32b7b744e7d2)


### Results

Drop a remark based on your observation.
- [x] Works Great in MindsDB cloud💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
- [x] There's a Bug in MindsDB local🪲 [#6313 Error During MongoDB manual handling - Predict Target value step](https://github.com/mindsdb/mindsdb/issues/6313) ( This means you encountered a Bug. Please open an issue with all the relevant details with the Bug Issue Template)

---
## Testing MongoDB Handler with [Algerian Forest Fires Data Set](https://archive.ics.uci.edu/ml/datasets/Algerian+Forest+Fires+Dataset++)

**1. Testing CREATE DATABASE**

```
db.databases.insertOne({
  name: "fire_test_db", // database name
  engine: "mongodb", // database engine 
  connection_args: {
    "port": 27017, // default connection port
    "host": "mongodb+srv://<user>:<password>@clusterml.e57k8ji.mongodb.net/Clusterml", //connection host
    "database": "AlgerianFire" // database connection          
    }
});
```
![CREATE_DATABASE](https://github.com/ya-sh-vardh-an/mindsdb/assets/111492054/047d7405-74d1-4d98-8000-b34c854bc607)


**2. Testing CREATE PREDICTOR**

```
db.predictors.insertOne({ 
  name: "fire_predict_model", 
  predict: "Temperature", 
  connection: "fire_test_db", 
  select_data_query: "db.forestfire.find({})"
});
```
![CREATE_MODEL](https://github.com/ya-sh-vardh-an/mindsdb/assets/111492054/7d49609b-b775-4a2b-b3c5-201233950d8c)
![CREATE_MODEL-STATUS](https://github.com/ya-sh-vardh-an/mindsdb/assets/111492054/6ac055fc-9507-409d-9793-3f8d20eeaf9c)


**3. Testing SELECT FROM PREDICTOR**

```
db.fire_predict_model.find({Rain: 0, month: 6});
```
![PREDICT-VALUE](https://github.com/ya-sh-vardh-an/mindsdb/assets/111492054/104d7946-7daf-4732-983d-c9ab80fe87a2)


### Results

Drop a remark based on your observation.
- [x] Works Great in MindsDB cloud💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
- [x] There's a Bug in MindsDB local🪲 [#6313 Error During MongoDB manual handling - Predict Target value step](https://github.com/mindsdb/mindsdb/issues/6313) ( This means you encountered a Bug. Please open an issue with all the relevant details with the Bug Issue Template)

---
## Testing MongoDB Handler with [Wine Quality Data Set](https://archive.ics.uci.edu/ml/datasets/Wine+Quality)

**1. Testing CREATE DATABASE**

```
db.databases.insertOne({
  name: "sample_test", // database name
  engine: "mongodb", // database engine 
  connection_args: {
    "port": 27017, // default connection port
    "host": "mongodb+srv://<user>:<password>@clusterml.e57k8ji.mongodb.net/Clusterml", //connection host
    "database": "Clusterml" // database connection          
    }
});
```
![CREATE-DATABASE](https://github.com/ya-sh-vardh-an/mindsdb/assets/111492054/42599887-5560-4de4-9199-08132cc2dc22)


**2. Testing CREATE PREDICTOR**

```
db.predictors.insertOne({ 
  name: "wine_quality_predict_model", 
  predict: "quality", 
  connection: "vaccine_test_db", 
  select_data_query: "db.wine.find({})"
});
```
![CREATE-MODEL](https://github.com/ya-sh-vardh-an/mindsdb/assets/111492054/1c7262f7-67c9-401d-85e9-ac3d7198f98e)
![CREATE-MODEL-STATUS](https://github.com/ya-sh-vardh-an/mindsdb/assets/111492054/341e48ac-c62c-492a-a7cf-cd24fef35089)



**3. Testing SELECT FROM PREDICTOR**

```
db.wine_quality_predict_model.find({"residual sugar": 1.9, "pH": 3.51});
```
![PREDICT-VALUE](https://github.com/ya-sh-vardh-an/mindsdb/assets/111492054/26f8b6b4-2979-4ae2-9488-c64e6ac46ee6)
![PREDICT-VALUE-extension](https://github.com/ya-sh-vardh-an/mindsdb/assets/111492054/dc86fcd8-093f-4616-887b-62143c017fcb)



### Results

Drop a remark based on your observation.
- [x] Works Great in MindsDB cloud💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
- [x] There's a Bug in MindsDB local🪲 [#6313 Error During MongoDB manual handling - Predict Target value step](https://github.com/mindsdb/mindsdb/issues/6313) ( This means you encountered a Bug. Please open an issue with all the relevant details with the Bug Issue Template)

---
