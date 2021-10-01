# AI-Tables in MariaDB

Database users are the best to know what data is relevant for ML models. Virtual AI tables in MariaDB allows users to run Automated Machine Learning models directly from inside the database. This tutorial is an overview of this integration capability.

Anyone that has dealt with Machine Learning (ML) understands that data is a fundamental ingredient to it. Given that a great deal of the world’s organized data already exists inside databases, doesn’t it make sense to bring machine learning capabilities straight to the database itself? To do so, we have developed a concept called AITables. In this article we want to present to you what AITables are, how you can use them in MariaDB. Invite you to try it out, get involved and to join us in the journey of ML meets MariaDB.

## AiTables

AITables differ from normal tables in that they can generate predictions upon being queried and returning such predictions like if it was data that existed on the table. Simply put, an AI-Table allows you to use machine learning models as if they were normal  database tables, in something that in plain SQL looks like this;

```sql
SELECT <predicted_variable> FROM <ML_model> WHERE <conditions>
```
To really sink in this idea, let us expand the concept through an example.

### The used car price example

Imagine that you want to solve the problem of estimating the right price for a car on your website that has been selling used cars over the past 2 years.

Also, you use MariaDB and in your database there is a table called `used_cars_data` where you keep records of every car you have sold so far, storing information such as: price, transmission, mileage, fuel_type, road_tax, mpg (Miles Per Gallon) and engine_size.

Since you have historical data, you know that you could use Machine Learning to solve this problem. Wouldn’t it be nice if you could simply tell your MariaDB server to do and manage the Machine Learning parts for you?

At MindsDB we think so too! And AI-Tables baked into MariaDB are here to do exactly that. Although further down in this article we will guide you step by step on how to run this example yourself, let us introduce you to what you can do and how it looks in standard SQL.

You can for instance with a single INSERT statement, create a machine learning model/predictor trained to predict ‘price’ using the data that lives in the table `sold_cars` and publish it as an AI-Table called ‘used_cars_model’.

```sql
INSERT INTO mindsdb.predictors(name, predict, select_data_query) 
VALUES ('used_cars_model', 'price', 'SELECT * FROM used_cars_data);
```

After that you can get price predictions by querying the generated ‘used_cars_model’ AI-Table, as follows:

```sql
SELECT price, 
       confidence 
FROM   mindsdb.used_cars_model 
WHERE  model = "a6" 
       AND mileage = 36203 
       AND transmission = "automatic" 
       AND fueltype = "diesel" 
       AND mpg = "64.2" 
       AND enginesize = 2 
       AND year = 2016 
       AND tax = 20; 
```

As you can see with AI-Tables, we are aiming to simplify Machine Learning mechanics to simple SQL queries, so that you can focus on the important part; which is to think about what predictions are key for your business and what data you want your ML to learn from to make such predictions.

By now, you might be thinking, how do I get started?, what do I need? How can I run this example in my environment?

As promised, in the rest of this article we will show you step-by-step instructions on how to integrate MindsDB into your MariaDB server, how to build, test and use Machine Learning Models as AI-Tables all without the need for specific machine learning skills and how to evaluate prediction results in an “Explainable AI” way. If you want to preview this tutorial visually go ahead to our youtube channel and follow up the Machine Learning in MariaDB with AI Tables video.

<iframe width="100%" height="415" src="https://www.youtube.com/embed/fkHNln3GFnc" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>

### MindsDB AITables in MariaDB

With MindsDB any MariaDB user can train and test neural-networks based Machine Learning models with the same knowledge they have of SQL.

MindsDB is an open-source ML framework that enables running Machine Learning Models as AI-Tables. On top of that it has an exciting “Explainable AI” feature that allows users to get insights into their Machine Prediction accuracy score and evaluate its dependencies. For example, users can estimate how adding or removing certain data would impact on the effectiveness of the prediction.

The whole integration consists of two important parts:
* The Machine Learning models are exposed as database tables (AI-Tables) that can be queried with the SELECT statements.
* The ML model generation and training is done through a simple INSERT statement.
This is possible thanks to MariaDBs CONNECT engine, which enables us to publish tables that live outside MariaDB. Since MindsDB supports the MySQL tcp-ip protocol, AI-Tables can be plugged as if they are external MariaDB tables. The following diagram illustrates this process.

![MindsDB MariaDB](/assets/tutorials/aitables-mariadb/mdb-maria.png)

The resource intensive Machine Learning tasks like model training happen on a separate MindsDB server instance or in the cloud, so that the Database performance is not affected.

In the following step-by-step tutorial you will learn how to install a MindsDB server with MariaDB and connect to the data, how to train the model and get predictions using an example dataset. So let’s get started.

### How to install MariaDB?

If you already have MariaDB installed you can skip this section. MariaDB is one of the most popular database servers in the world and works on the most widely used operating systems. You can find the installation binaries and packages on the [mariadb download](https://mariadb.com/downloads/) site. To check the full list of distributions which include MariaDB head over to [list of distributions](https://mariadb.com/kb/en/distributions-which-include-mariadb/).

### How to install MindsDB?

Before you install MindsDB you need a Python version greater than 3.6 and pip >=19.3 which comes pre-installed with newer Python versions. Also, you will need to have around 1GB free space on your machine for installing the MindsDB’s dependencies. Other than that the installation is quite simple. Inside your virtual environment just run:

```
pip install mindsdb
```

To check if the installation was successful run:

```
pip show mindsdb
```

And you should be able to see the MindsDB information as name, version, summary, license etc:

![MindsDB version](/assets/tutorials/aitables-mariadb/mdb-ver.png)

That’s all. Let’s set up the required configuration and start MindsDB.

### Example Dataset

If you are following this tutorial with your own data, you can skip to the next section. For this example we will use the [Used Car Price](https://www.kaggle.com/adityadesai13/used-car-dataset-ford-and-mercedes) dataset from the 100k used cars scraped data. The dataset contains information on price, transmission, mileage, fuel type, road tax, miles per gallon (mpg), and engine size of the used cars in the UK. The idea is to predict the price depending on the above features.

### Add data to MariaDB

The first thing we need to do is to create the table. Execute the below query:

```sql
CREATE TABLE `used_cars_data` 
  ( 
     `model`        VARCHAR(100) DEFAULT NULL, 
     `year`         INT(11) DEFAULT NULL, 
     `price`        INT(11) DEFAULT NULL, 
     `transmission` VARCHAR(100) DEFAULT NULL, 
     `mileage`      INT(11) DEFAULT NULL, 
     `fueltype`     VARCHAR(100) DEFAULT NULL, 
     `tax`          INT(11) DEFAULT NULL, 
     `mpg`          FLOAT DEFAULT NULL, 
     `enginesize`   FLOAT DEFAULT NULL 
  ) 
engine=innodb 
DEFAULT charset=latin1 
```
The InnoDB is a general storage engine and the one offered as the best choice in most cases from the MariaDB team.To see the list with all available ENGINEs and for advice on which one to choose check out the [MariaDB engine docs](https://mariadb.com/kb/en/choosing-the-right-storage-engine/#:~:text=InnoDB%20is%20a%20good%20general,InnoDB%20and%20is%20usually%20preferred.). After creating the table there are a few options that you can do to add the data inside MariaDB.

* If you are using graphical clients such as dbForge, DBeaver or another SQL client use the import option from the menu.

* Use the LOAD DATA statement that reads the local file from the location provided and sends the content to the MariaDB Server:

```
LOAD DATA LOCAL INFILE 'data.csv' INTO TABLE used_cars_data FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n';

```
The TERMINATED BY specifies the separator in the data. The escape characters and new lines are manageable with the ENCLOSED BY and LINES TERMINATED BY clause.

* Using [mysqlimport](https://mariadb.com/kb/en/importing-data-into-mariadb/#mysqlimport). This is the cli for the above, LOAD DATA statement. The arguments sent here correspond to the clauses of the LOAD DATA example:

```
mysqlimport --local --fields-terminated-by="," used_cars_data data.csv 
```
Let’s select the data from used_cars_data table to make sure it is successfully imported:

```sql
SELECT * FROM  test.used_cars_data LIMIT 5;
```

![SELECT data](/assets/tutorials/aitables-mariadb/select-data.png)

The data is inside MariaDB so the next step is to add the required configuration.

### Required Configuration
The first thing we need to set up is the required configuration for MindsDB Server. Let’s create a new file called config.json and add the following example:

```json
{
    "api": {
        "http": {
            "host": "127.0.0.1",
            "port": "47334"
        },
        "mysql": {
            "host": "127.0.0.1",
            "password": "password",
            "port": "47335",
            "user": "root"
        }
    },
    "config_version": "1.4",
    "integrations": {
        "default_mariadb": {
            "publish": true,
            "host": "localhost",
            "password": "password",
            "port": 3306,
            "type": "mariadb",
            "user": "root"
        }
    },
    "storage_dir": "/storage"
}
```
This looks like a big configuration file but it is quite simple. What we have included are:

* api – The keys here are specifying the host and port for MindsDB REST Apis.
* mysql – All of the information for connecting to mysql and using the mysql protocol as host, user, password and port. Also the log level information and the path to your local SSL certificate. If certificate_path is left empty, MindsDB will automatically create one.
* integrations – Here, we are specifying the type of integration that we will use, default_mariadb. Also the required parameters for connecting to it as host, port, user, password. Other supported databases for integrations are ClickHouse, PostgreSQL, MySQL and Microsoft SQL Server.
* interface – The required keys added here are datastore and mindsdb_native, that contains the path to the storage location, which will be used by MindsDB to save some of the configuration files.
And the last thing we need to do is to install the CONNECT storage engine plugin that we have added in the plugin-load-add variable. To check how to download it for your OS check installing docs. It should be quite simple as using the package manager, e.g:

```sql
sudo apt-get install mariadb-plugin-connect
```

That’s pretty much everything related to the configuration required for successful integration with MariaDB. Let’s jump to the interesting part where we will train the machine learning model and query it.

### AutoML inside MariaDB

First, we need to start MindsDB:

```
python3 -m mindsdb --api=mysql --config=config.json
```

The flags added here are:

* –api – This specifies the type of API we will use with MindsDB, in this case mysql.
* –config – The path to the config.json file we have created before.
If MindsDB was successfully started there should be a new database automatically created in MariaDB called mindsdb with two tables (commands and predictors).

![Database](/assets/tutorials/aitables-mariadb/database.png)
In these tables, MindsDB will keep information about the models, model accuracy, training status, target variable that we will predict and additional options used for model training.

### Create new predictor
The main motto when we first started MindsDB was with one line of code, so now we will try to stick to it and present that in the databases with just one query. The Predictor in MindsDB’s words means Machine Learning model, so creating one could be done with an INSERT statement inside mindsdb.predictors table. Execute the following query:

```sql
INSERT INTO mindsdb.predictors(name, predict, select_data_query) 
VALUES ('used_cars_model', 'price', 'SELECT * FROM used_cars_data);
```
What this query does is it creates a new model called ‘used_cars_model’ , specifies the column that we will try to predict as ‘price’ from the used_cars_data  table. The required columns(parameters) for training the predictor are:

* name (string) – the name of the predictor.
* predict (string) –  the feature you want to predict, in this example it will be price.
* select_data_query (string) – the SELECT query that will get the data from MariaDB.
* training_options (dictionary) – optional value that contains additional training parameters. For a full list of the parameters check the mindsdb.docs.
You should see the message about the successful execution of the query and if you open up the console, the MindsDB logger shall display messages while training the model.

![Database](/assets/tutorials/aitables-mariadb/training.png)

Training could take some time depending on the data used, columns types, size etc. In our example not more than 2-3 min. To check that model was successfully trained run:

```sql
SELECT * FROM mindsdb.predictors WHERE name='used_cars_model'
```
![Database](/assets/tutorials/aitables-mariadb/training-run.png)
The column status shall be complete and the model accuracy will be saved when the training finishes.

![Database](/assets/tutorials/aitables-mariadb/training-finish.png)

The model has been trained successfully. It was quite simple because we didn’t do any hyperparameters tuning or features engineering and leave that out to MindsDB as an AutoML framework to try and fit the best model. With the INSERT query, we just provided labeled data as an input. The next step is to query the trained model with SELECT and get the output from it (predict the price of the car).

### Query the predictor

To get the prediction from the model is as easy as executing the SELECT statement where we will select the price and price confidence. The main idea is to predict the used car’s price depending on the different features.

The first thing that comes to mind when looking for the used car is the car model, fuel type, mileage, the year when the car was produced etc. We should include in the WHERE clause all of this informations and leave it to MindsDB to make the predictions for them e.g:

```sql
SELECT price            AS predicted, 
       price_confidence AS confidence 
FROM   mindsdb.used_cars_model 
WHERE  model = "a6" 
       AND mileage = 36203 
       AND transmission = "automatic" 
       AND fueltype = "diesel" 
       AND mpg = "64.2" 
       AND enginesize = 2 
       AND year = 2016 
       AND tax = 20; 
```

![Predicted value](/assets/tutorials/aitables-mariadb/predicted.png)

You should see that MindsDB is quite confident that the car with all of the above characteristics as included in the WHERE clause shall cost around 13,111. To get additional information about the prediction include the explain column in the SELECT e.g:

```sql
SELECT price            AS predicted, 
       price_confidence AS confidence, 
       price_explain    AS info 
FROM   mindsdb.used_cars_model 
WHERE  model = "a6" 
       AND mileage = 36203 
       AND transmission = "automatic" 
       AND fueltype = "diesel" 
       AND mpg = "64.2" 
       AND enginesize = 2 
       AND year = 2016 
       AND tax = 20; 
```

![Predicted info](/assets/tutorials/aitables-mariadb/predicted-info.png)

Note that to beautify the resultFormat you can add command line option format for particular session.
Now MindsDB will display additional information in the info column as prediction quality, confidence interval, missing information for improving the prediction etc.

```json
{
    "predicted_value": 13772,
    "confidence": 0.9922,
    "prediction_quality": "very confident",
    "confidence_interval": [10795, 31666],
    "important_missing_information": [],
    "confidence_composition": {
        "Model": 0.008,
        "year": 0.018,
        "transmission": 0.001,
        "mpg": 0.001
    },
    "extra_insights": {
        "if_missing": [{
            "Model": 13661
        }, {
            "year": 17136
        }, {
            "transmission": 3405
        }, {
            "mileage": 15281
        }, {
            "fuelType": 7877
        }, {
            "tax": 13908
        }, {
            "mpg": 38858
        }, {
            "engineSize": 13772
        }]
    }
}
```

The confidence_interval specifies the probability that the value of a price lies within the range of 10k to 30k. The important_missing_information in this case is empty, but if we omit some of the important values in the WHERE clause e.g price, year or mpg, MindsDB shall warn us that that column is important for the model. The if_missing in the extra_insights shows the price value if some of the mentioned columns are missing.

Now, let’s try and get the price prediction for different car models with different fuel type, mileage, engine size, transmission:

```sql
SELECT price            AS predicted, 
       price_confidence AS confidence, 
       price_explain    AS info 
FROM   mindsdb.used_cars_model 
WHERE  model = "a1" 
       AND mileage = 122946 
       AND transmission = "manual" 
       AND fueltype = "petrol" 
       AND mpg = "35.4" 
       AND enginesize = 1.4 
       AND year = 2014 
       AND tax = 30; 
```

![Predicted value](/assets/tutorials/aitables-mariadb/predicted1.png)

Now, MindsDB thinks that this type of car would cost around 12k and price ranges to 23k.

Let’s sum up what we did and the simple steps we take to get the predictions:

* Install MariaDB and MindsDB.
* Setup the configuration.
* Train the model with an INSERT query.
* Get predictions from the model with a SELECT query.

Quite simple right?

This is a brand new feature that we have developed so we are happy to hear your opinions on it. You can play around and query the model with different values or train and query the model with different datasets.

If you have some interesting results or you found some issues we are happy to help and talk with you. Join our [community forum](https://community.mindsdb.com/) or reach out to us on [GitHub](https://github.com/mindsdb/mindsdb).