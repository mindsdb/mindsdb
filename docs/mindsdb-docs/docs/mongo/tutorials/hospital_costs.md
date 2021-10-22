# Forecast Hospital Costs using MindsDB Cloud and MongoDB Atlas :fireworks:

In this tutorial, we all will  learn to the following:

:point_right: Connecting the  MongoDB database to MindsDB.

:point_right: How Train a model to predict Hospital Costs ?

:point_right: Make a prediction from predictor trained by you with help of mindsdb.

Amazing things what i want to tells you all this is done **without writing a SINGLE LINE CODE !!** :open_mouth: Yes Its is True Belive Mindsdb's Power💪 .

We will be using the  Hospital Costs dataset 🏥 that can be downloaded from [here](https://github.com/mindsdb/benchmarks/blob/main/benchmarks/datasets/hospital_costs/data.csv). You are also free to use your own dataset and follow along the tutorial.

## :hash: Pre-requisites
1. This tutorial is going to  [MongoDB Atlas](https://www.mongodb.com/cloud/atlas) so it is good if you know some basics of MongoDB Atlas . *If Not Dont Worry Follow Along me.* In short, MongoDB Atlas is a Database as a Service(DaaS), which we will be using to spin up a MongoDB database cluster and load our dataset.
2. Download a copy of the  Hospital Costs dataset from [here](https://github.com/mindsdb/benchmarks/blob/main/benchmarks/datasets/hospital_costs/data.csv).
3. You are also expected to have an account on MindsDB Cloud. If not, head over to [https://cloud.mindsdb.com/](https://cloud.mindsdb.com/) and create an account. It hardly takes a few minutes. :zap:
4. We also need MongoDB Compass to load the dataset into the collection. It can be downloaded from [here](https://www.mongodb.com/try/download/compass).

Finally, No! **You are not required to have any background in programming or machine learning**. As mentioned before you wont be writing a single line of code!

## :hash: MindsDB In Short
> **_MindsDB is a predictive platform that makes databases intelligent and machine learning easy to use. It allows data analysts to build and visualize forecasts in BI dashboards without going through the complexity of ML pipelines, all through SQL. It also helps data scientists to streamline MLOps by providing advanced instruments for in-database machine learning and optimize ML workflows through a declarative JSON-AI syntax.
Although only SQL is mentioned, MongoDB is also supported._**

## :hash: Dataset overview
A nationwide survey of hospital costs conducted by the US Agency for Healthcare
consists of hospital records of inpatient samples. The given data is restricted to
the city of Wisconsin and relates to patients in the age group 0-17 years. The
agency wants to analyze the data to research on the healthcare costs and their
utilization. . More details can be found [here](https://www.kaggle.com/vik2012kvs/analyze-the-healthcare-cost-in-wisconsin-hospitals).

The dataset is a `.csv` file that contains 6 columns:

* `AGE` : Age of the patient discharged .
* `Sex` : Binary variable that indicates if the patient is female . (True Means Female)
* `LOS` : Length of stay, in days .
* `Demographic` : Race of the patient (specified numerically) .
* `TOTCHG` : Hospital discharge costs .
* `APRDRG` : All Patient Refined Diagnosis Related Groups .

Phew! With all of that out of the way, we can finally get started! :rocket: 

# :video_game: Let the Game Begin :basketball:

## :hash: Setting up a Cluster on MongoDB Atlas
1. GO to [https://cloud.mongodb.com/](https://cloud.mongodb.com/) and create a new project named `mindsdb` and within it a new database cluster named `mindsDB`. Typically, it takes a some _**Short Time( 1 to 2 minutes)**_ to provision a cluster. Once it is done, you should see something like this on you screen:
    ![Create cluster](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/n7otmph2uyb4cpu3u1ar.png)
    
2. Now Click on the "Connect" button. In the popup, you have to add a connection IP address. Although not recommended, but for the sake of this tutorial, select "Allow access from anywhere" and then "Add IP Address".
    ![Firewall setup](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/xbc1y1uhxsbghd4j12ae.png) 
    
3. Next, you will be asked to create a new database user. After providing a username and password, click on the "Create Database User" button.**(Remember this username And Password)**
    ![Database user creation](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/o1siazilrkubaiqum5z5.png)
    
4. In the next step, choose "Connect using MongoDB Compass". Copy the connection string which should look like this:(Or Enter Your username and password At <> place)
```
mongodb+srv://<username>:<password>@mindsdb.htuqc.mongodb.net/test
```

We will now use this connection string to connect to our database from MongoDB Compass and load our dataset.
Using this connection string we will connect our database to MongoDB compass  and load our dataset.


## :hash: Loading the dataset with MongoDB Compass
1. Open MongoDB Compass. Paste the connection string and click connect button to get connected. On successful authentication, you will be welcomed by this screen.
    ![Welcome screen - MongoDB Compass](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/13u778mwf1jdxufzwhv1.png)
2. Click on "Create Database" to create a database named `mindsDB` and a collection having name as `data`.
    ![Create database](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/vmbdso723t7dlxmpc8uc.png)
3. You will now see that  `mindsDB` is  listed. Click on it and you will see that it contains a collection named `data`. We will be loading data from the `.csv` file into this collection. Open the `data` collection by clicking on it.
    ![Import data](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/ozb0ni92whdeu98zvfnv.png) 
4. Click on the "Import data" button and now load your `.csv` file. You will now be able to preview your dataset and also assign the data types as shown below. Then, "import" the dataset and wait for a few seconds for the import to finish.
    ![Set data types](https://images.unsplash.com/photo-1634706966017-69d97886febd?ixid=MnwxMjA3fDB8MHxwcm9maWxlLXBhZ2V8MXx8fGVufDB8fHx8&ixlib=rb-1.2.1&auto=format&fit=crop&w=3000&q=60)

## :hash: Connecting MindsDB to MongoDB Database
1. Head over to [MindsDB CLoud](https://cloud.mindsdb.com/) and click on "Add Database".
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

    ![create dataset](https://images.unsplash.com/photo-1634675894412-87e53fb553ff?ixid=MnwxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8&ixlib=rb-1.2.1&auto=format&fit=crop&w=700&q=80)
6. Click on "Create" and now we will see that our data source named "**Hosptial Costs**" has been added. One can check for the quality and also preview the data source.
    ![Data source created](https://images.unsplash.com/photo-1634675897672-7fb5df7d4eb8?ixlib=rb-1.2.1&ixid=MnwxMjA3fDB8MHxwcm9maWxlLXBhZ2V8NXx8fGVufDB8fHx8&auto=format&fit=crop&w=1000&q=60)
    
    We are now ready to train an ML model to predict the `TOTCHG` using MindsDB.

## :hash: Training the ML Model
1. Navigate to the Predictors Tab and click on "Train New".
2. In the popup , give a name to the predictor what ever you wnt to give and select the column that needs to be predicted, which in our case is `TOTCHG`.
    ![Predictor](https://images.unsplash.com/photo-1634704128381-6394ee413e05?ixid=MnwxMjA3fDB8MHxwcm9maWxlLXBhZ2V8MXx8fGVufDB8fHx8&ixlib=rb-1.2.1&auto=format&fit=crop&w=1000&q=90)
3. After entering the details, click on "Train". That's how simple training an ML model is with MindsDB. Now all you have to do  take a cup of water/tea/milk/coffee if dataset is very heavy or something you want , after training is completed you can predict `TOTCHG`


## :hash: Running Queries to get predictions
1. When You see status COMPLETED of predictor , You are ready to get prediction from predictor.
    ![Training complete](https://images.unsplash.com/photo-1634675897672-7fb5df7d4eb8?ixlib=rb-1.2.1&ixid=MnwxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8&auto=format&fit=crop&w=2085&q=80)
    We can see that the model has an accuracy of 47-49%, which is impressive!
2. To start getting predictions, navigate to the "Query" button and  then click on "New Query".
3. Let's say we wanted to know the `TOTCHG(Total Charge)` for some person and all we know(data about person) is the following:
    ```json
    {
        "AGE" : 17,
        "APRDRG": 33,
        "LOS" : 20,
        "DEMOGRAPHIC": 1.0 ,
        "SEX": false,

    }
    ```
    >:ballot_box_with_check: **Sex** has value **false** means **Female**
    
    Enter the above details(**Don Just Copy Free to change value of input**) and "Run Query".
    
    ![Run query](https://images.unsplash.com/photo-1634704822405-e81f23b5bebf?ixid=MnwxMjA3fDB8MHxwcm9maWxlLXBhZ2V8MXx8fGVufDB8fHx8&ixlib=rb-1.2.1&auto=format&fit=crop&w=900&q=60)
4. We can see that the model predicted with 99% confidence that on such a day, the traffic volume would be 832.
    ![Prediction result](https://images.unsplash.com/photo-1634675897727-de9f0f98da98?ixlib=rb-1.2.1&ixid=MnwxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8&auto=format&fit=crop&w=1225&q=80)
 
Feel Free to use inputs according you Mind ! 
That All !
### _Now Just Enjoy And Work !_

## :hash: What Next?
This tutorial can be extended to perform lots of awesome things. For example, it would be interesting to see the dependence between the AGE and the TOTCHG. Some interesting questions that can be asked are:
1. Given a certain `TOTCHG` what is the probability the person is Male ? What is the probability that it is Female? 
2. Given a certain `TOTCHG`, how certain can we be that person is of **Adult AGE Group**? 
3. Can we predict more parameters instead of only the `TOTCHG`? :thinking:

Apart from these, you can also install MindsDB on your machine and connect with your local databases to get predictions. You can also use a BI tool to visualize these predictions. Apart from SQL and MongoDB, you can explore other data source integrations that MindsDB supports like Oracle and Kafka.


