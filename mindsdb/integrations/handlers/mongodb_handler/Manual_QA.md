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
