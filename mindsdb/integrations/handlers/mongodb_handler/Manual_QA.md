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

![CREATE_DATABASE](Image URL of the screenshot)

**2. Testing CREATE PREDICTOR**

```
db.predictors.insert({ 
  name: "cars_predictor", 
  predict: "Selling_Price", 
  connection: "cars_db", 
  "select_data_query": "db.cars.find()"
});
```

![CREATE_PREDICTOR](Image URL of the screenshot)

**3. Testing SELECT FROM PREDICTOR**

```
db.cars_predictor.find({Car_Name: "ritz", Fuel_Type: "Petrol"});
```

![SELECT_FROM](Image URL of the screenshot)

### Results

Drop a remark based on your observation.
- [x] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
- [ ] There's a Bug 🪲 [Issue Title](URL To the Issue you created) ( This means you encountered a Bug. Please open an issue with all the relevant details with the Bug Issue Template)

---