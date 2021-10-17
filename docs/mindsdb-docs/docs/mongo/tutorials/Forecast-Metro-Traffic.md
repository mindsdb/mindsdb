# Forecast Metro Traffic using MindsDB Cloud and MongoDB Atlas

In this tutorial, we will be learning to:

:point_right: Connect a MongoDB database to MindsDB.

:point_right: Train a model to predict metro traffic.

:point_right: Get a prediction from the model given certain input parameters.

All of that **without writing a single line of code and in less than 15 minutes**. Yes, you read that right! :100:

We will be using the Metro traffic dataset :metro: that can be downloaded from [here](https://github.com/mindsdb/benchmarks/blob/main/benchmarks/datasets/metro_traffic_ts/data.csv). You are also free to use your own dataset and follow along the tutorial.

## :hash: Pre-requisites
1. This tutorial is primarily going to be about MindsDB so the reader is expected to have some level of familiarity with [MongoDB Atlas](https://www.mongodb.com/cloud/atlas). In short, MongoDB Atlas is a Database as a Service(DaaS), which we will be using to spin up a MongoDB database cluster and load our dataset.
2. Download a copy of the Metro Traffic dataset from [here](https://github.com/mindsdb/benchmarks/blob/main/benchmarks/datasets/metro_traffic_ts/data.csv).
3. You are also expected to have an account on MindsDB Cloud. If not, head over to [https://cloud.mindsdb.com/](https://cloud.mindsdb.com/) and create an account. It hardly takes a few minutes. :zap:

Finally, No! **You are not required to have any background in programming or machine learning**. As mentioned before you wont be writing a single line of code!

## :hash: About MindsDB
> MindsDB is a predictive platform that makes databases intelligent and machine learning easy to use. It allows data analysts to build and visualize forecasts in BI dashboards without going through the complexity of ML pipelines, all through SQL. It also helps data scientists to streamline MLOps by providing advanced instruments for in-database machine learning and optimize ML workflows through a declarative JSON-AI syntax.

Although only SQL is mentioned, MongoDB is also supported.

## :hash: Dataset overview
The dataset contains information about the: *Hourly Interstate 94 Westbound traffic volume for MN DoT ATR station 301, roughly midway between Minneapolis and St Paul, MN*. More details can be found [here](https://archive.ics.uci.edu/ml/datasets/Metro+Interstate+Traffic+Volume).

The dataset is a `.csv` file that contains 9 columns:
1. `holiday`: Categorical US National holidays plus regional holiday, Minnesota State Fair
2. `temp`: Numeric Average temp in *kelvin*
3. `rain_1h`: Numeric Amount in *mm of rain* that occurred in the hour
4. `snow_1h`: Numeric Amount in *mm of snow* that occurred in the hour
5. `clouds_all`: Numeric *Percentage* of cloud cover
6. `weather_main`: Categorical Short textual description of the current weather
7. `weather_description`: Categorical Longer textual description of the current weather
8. `date_time`: *DateTime* Hour of the data collected in local CST time
9. `traffic_volume`: Numeric Hourly I-94 ATR 301 reported westbound traffic volume

Phew! With all of that out of the way, we can finally get started! :rocket: 

## :hash: Setting up a Cluster on MongoDB Atlas
1. Head over to [https://cloud.mongodb.com/](https://cloud.mongodb.com/) and create a new project named `mindsdb` and within it a new database cluster named `mindsDB`. Typically, it takes a minute or two to provision a cluster. Once it is done, you should have something like this:
    ![Create cluster](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/n7otmph2uyb4cpu3u1ar.png)
2. Click on the "Connect" button. In the popup modal, you will be asked to add a connection IP address. Although not recommended, for the sake of this tutorial, choose "Allow access from anywhere" and then "Add IP Address".
    ![Firewall setup](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/xbc1y1uhxsbghd4j12ae.png) 
3. Next, you will be asked to create a new database user. After providing a username and password, click on the "Create Database User" button.
    ![Database user creation](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/o1siazilrkubaiqum5z5.png)
4. In the next step, select "Connect using MongoDB Compass". Copy the connection string which should look like this:
```
mongodb+srv://<username>:<password>@mindsdb.htuqc.mongodb.net/
```
We will now use this connection string to connect to our database from MongoDB Compass and load our dataset.

## :hash: Loading the dataset with MongoDB Compass
1. Open MongoDB Compass. Paste the connection string and click on "Connect". On successful authentication, you will be welcomed by this screen.
    ![Welcome screen - MongoDB Compass](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/13u778mwf1jdxufzwhv1.png)
2. Click on "Create Database" and create a database named `mindsDB` and a collection named `data`.
    ![Create database](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/vmbdso723t7dlxmpc8uc.png)
3. You will now see `mindsDB` listed. Click on it and you will see that it contains a collection named `data`. We will be loading data from the `.csv` file into this collection. Open the `data` collection by clicking on it.
    ![Import data](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/ozb0ni92whdeu98zvfnv.png) 
4. Click on the "Import data" button and load your `.csv` file. You will now be able to preview your dataset and also assign the data types as shown below. Then, "import" the dataset and wait for a few seconds for the import to finish.
    ![Set data types](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/xa4z5jnmmjzrdrlp55un.png)

## :hash: Connecting MindsDB to MongoDB Database
1. Head over to [https://cloud.mindsdb.com/](https://cloud.mindsdb.com/) and click on "Add Database".
    ![MindsDB landing page](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/8j7mmi39jgzi38lp7i37.png)
2. Enter the required details as shown below. The connection string must be similar to:
    ```
    mongodb+srv://<username>:<password>@mindsdb.htuqc.mongodb.net/mindsDB
    ```
    ![Add DB](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/xbsqgmp3bie7774str70.png)
3. Click on "Connect" and that's it! We have successfully linked our Database to MindsDB.
    ![database linked](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/ru1d28cgse997fw8g6mr.png)
4. Next, head over to the Datasets tab and click on "From database".
5. Enter the details as shown below. In the *Find* field, we can specify a Mongo query using which MindsDB will include only the results of this query in the data source. By specifying `{}`, we are telling MindsDB to include every single document in the `data` collection in the data source.

    ![create dataset](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/t0nrk7krs1ab7os9rb2x.png)
6. Click on "Create" and now we will see that our data source named "Metro Traffic Dataset" has been added. One can check for the quality and also preview the data source.
    ![Data source created](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/19ee37xe3q412um9ku36.png)
    We are now ready to train an ML model to predict the `traffic_volume` using MindsDB.

## :hash: Training the ML Model
1. Head over to the Predictors Tab and click on "Train New".
2. In the popup modal, give a name to the predictor and select the column that needs to be predicted, which in our case is `traffic_volume`.
    ![Predictor](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/l81z2wpp5c8jykmn9t0u.png)
3. After entering the details, click on "Generate". That's how simple training an ML model is with MindsDB. Now all you have to do is wait for a few minutes for the model to get trained after which you will be able to run queries and get predictions on the `traffic_volume`.
    ![Training](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/x8gnel2891sgdgycd51o.png)  

## :hash: Running Queries to get predictions
1. Once the status changes to COMPLETE, it means that our model is now ready and we can start getting predictions.
    ![Training complete](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/jq4jc96x90wz3f77u5om.png)
    We can see that the model has an accuracy of 98.6%, which is impressive!
2. To start getting predictions, click on the "Query" button and then "New Query".
3. Let's say we wanted to know the `traffic_volume` for some day and all we know is the following:
    ```bash
    {
        temp: 300, # temperature of 300 Kelvin
        clouds_all: 10, # 10% cloud cover
        weather_main: "Clouds",
        weather_description: "few clouds",
        holiday: "None"
    }
    ```
    Enter the above details and "Run Query".
    ![Run query](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/6r7y0n30zhoetqtunc40.png)
4. We can see that the model predicted with 99% confidence that on such a day, the traffic volume would be 832.
    ![Prediction result](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/3us0ret3ewf253i1w695.png)
 
You can play with the inputs and run a few more queries and observe the results.

## :hash: What Next?
This tutorial can be extended to perform lots of awesome things. For example, it would be interesting to see the dependence between the weather and the traffic volume. Some interesting questions that can be asked are:
1. Given a certain `traffic_volume` what is the probability the sky is clear :sunny:? What is the probability that it is raining? 🌧️
2. Given a certain `traffic_volume`, how certain can we be that the day is a holiday? 🏖️
3. Can we predict more parameters instead of only the `traffic_volume`? :thinking:

Apart from these, you can also install MindsDB on your machine and connect with your local databases to get predictions. You can also use a BI tool to visualize these predictions. Apart from SQL and MongoDB, you can explore other data source integrations that MindsDB supports like Oracle and Kafka.

If you have made it this far, thank you for your time and hope you found this useful. If you like the article, like it and share it with others.

Happy Querying! :chart_with_downwards_trend:
