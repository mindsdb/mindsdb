# Welcome to the MindsDB Manual QA Testing for DocumentDB Handler

> **Please submit your PR in the following format after the underline below `Results` section. Don't forget to add an underline after adding your changes i.e., at the end of your `Results` section.**

## Testing DocumentDB Handler with [home_rentals.csv](https://github.com/mindsdb/mindsdb/files/12832142/home_rentals.csv)

<img width="1512" alt="Screenshot 2023-10-06 at 7 27 13 PM" src="https://github.com/mindsdb/mindsdb/assets/22766405/419bd94b-068f-43ee-88e6-e2fe84cb7232">

**1. Starting a ssh tunnel to DocumentDB in AWS**

```
ssh -i "ec2docDBpf.pem" -L 27017:docdb-demo.cluster-c2cuphiyp2vn.ap-south-1.docdb.amazonaws.com:27017 ec2-user@ec2-3-110-104-64.ap-south-1.compute.amazonaws.com -N
```

**1. Testing CREATE DATABASE**

```
db.databases.insertOne({
    name: "example_documentdb",
    engine: "documentdb",
    connection_args: {
        "username": "username",
        "password": "password",
        "host": "127.0.0.1",
        "port": "27017",
        "database": "sample_database",
        "kwargs": {
            "directConnection": true,
            "serverSelectionTimeoutMS": 2000,
            "tls": true,
            "tlsAllowInvalidHostnames": true,
            "tlsAllowInvalidCertificates": true,
            "retryWrites": false,
            "tlsCAFile": "/home/global-bundle.pem"
        }
    }
});
```

<img width="519" alt="create database" src="https://github.com/mindsdb/mindsdb/assets/22766405/7fd4d11e-172f-45eb-878b-7e68eb791e14">

<img width="394" alt="MONGO DBS" src="https://github.com/mindsdb/mindsdb/assets/22766405/295faa18-0185-424f-b424-08f9476c2622">

**2. Testing CREATE PREDICTOR**

```
db.predictors.insertOne({
     name: "home_rentals_docdb_model",
     predict: "rental_price",
     connection: "example_documentdb",
     select_data_query: "db.home_rentals.find({})"
});
```

<img width="506" alt="CREATE_PREDICTOR" src="https://github.com/mindsdb/mindsdb/assets/22766405/26eb32b2-7efe-4654-9873-650dd1c91160">

**3. Testing FIND STATUS OF PREDICTOR**

```
db.predictors.find({name: "home_rentals_docdb_model"});
```

<img width="825" alt="PREDICTOR_STATUS" src="https://github.com/mindsdb/mindsdb/assets/22766405/f5a877b5-761e-4cdd-b11b-fb421c4d3586">

**4. Testing SELECT FROM PREDICTOR**

```
db.home_rentals_docdb_model.find({sqft: "823", location: "good", neighborhood: "downtown", days_on_market: "10"});
```

<img width="1411" alt="SCORING" src="https://github.com/mindsdb/mindsdb/assets/22766405/451fc327-2135-4fe5-85fe-da1a73c866a5">

**4. Testing DROP PREDICTOR**

```
db.predictors.deleteOne({name: "home_rentals_docdb_model"});
```

<img width="679" alt="DROP" src="https://github.com/mindsdb/mindsdb/assets/22766405/2d3e5a69-a8d7-404c-854e-a53a6e9c050e">

Drop a remark based on your observation.

-   [x] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
-   [ ] There's a Bug 🪲 [Issue Title](URL To the Issue you created) ( This means you encountered a Bug. Please open an issue with all the relevant details with the Bug Issue Template)

---
