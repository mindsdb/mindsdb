## Predicting house price
In this tutorial you will learn how to predict house price on basis of different parameter like condition,bedrooms,bathrooms,area etc. We will be using mysql database for this purpose and kc_house_data as dataset.
link to dataset is https://www.kaggle.com/harlfoxem/housesalesprediction you can download whole dataset from here.
The process will be like:

 1. Cleaning data and deleting unnecessary columns.
 2. Starting MySQL main server to create databases and tables.
 3. Starting mindsdb server to access gui and its database.
 5. Starting another MySQL server on port 47735 with username mindsdb
 7. Predicting using our predictor.

These are our 5 step to add ML prediction model layer on any mysql database. 
So let get started.

## Cleaning data:
Simply install database and clean it using excel ,your data should look like this:
![excle_img](https://user-images.githubusercontent.com/59201258/141510855-b8e2325a-361c-44c4-9fd8-8adfb6729602.PNG)


## Starting mysql main server
So now time to start root server, open cmd and type:

    mysql -h 127.0.0.1 --port 3306 -u root -p
   
   Now create our database:
   

    CREATE DATABASE housedata;
Now create the table:

    USE housedata
    CREATE TABLE house_pricing_data;
Now time to import our CSV data:

    LOAD DATA INFILE "I:/kc_house_data.csv"
     INTO TABLE house_pricing_data
     FIELDS TERMINATED BY ','
     ENCLOSED BY '"'
     LINES TERMINATED BY '\r\n'
     IGNORE 1 LINES
     (id,grade,price,bedrooms,bathrooms,sqft_living,floors,condition_of);

This will import all csv data to your table. Now our mysql database setup is done!
don't quit this server,open another cmd.

## Starting mindsdb 
To start mindsdb locally run:

    python3 -m mindsdb --api=http,mysql

You don't need config file in this case, now go to gui interface, and Add database:
![ADD_DB](https://user-images.githubusercontent.com/59201258/141510954-1ad712de-563a-477b-904c-c0c7a60e57e9.PNG)

Now type all details like below:

![fill_form](https://user-images.githubusercontent.com/59201258/141511055-0c7cedc7-d610-47a1-915c-2729e166bf91.PNG)


 - ***Intergration name:*** It define the name through which we will access database in mindsdb's mysql server.
 - ***database name:*** It is same database name that we created in second step,it is name of database that we have to access.
 - ***host and port:*** host and port will be localhost since we are running everything locally.
 - ***Username and password:*** they should be same as your main MySQL server that is running(we run it in second step).

Now click on connect , it eventually create an instance and you are now ready to create your predictor, your gui should look like this:

![gui_updated](https://user-images.githubusercontent.com/59201258/141511216-a316b3f2-ea11-4088-9706-892f7a405400.PNG)

## Creating predictor

Open your third cmd window and run:

    mysql -h 127.0.0.1 --port 47335 -u mindsdb -p 

*Note:Password for mindsdb is usually blank*

Now on running `show databases;  `you will see:

![database_cmd](https://user-images.githubusercontent.com/59201258/139728508-dff29fa4-ead8-463c-af2b-49172f7339ae.PNG)

Now create the predictor:
    create predictor housing_predictor
         FROM house_kc_data
         (select * from house_pricing_data) predict price as pricing
Here housing_predictor is name of predictor model we are giving. house_kc_data is name of integration that we created in third step, housing_pricing_data is name of table inside housedata database.

**NOTE:** *we use integration name to access database and inside bracket we run query to retrieve data from database linked to integration.*
price is name of column we are going to predict and pricing is alias name we gave to price column.

## Predicting data:
Now when you created predictor ,you can see status of your predictor:

    use mindsdb;
    SELECT  *  FROM  mindsdb.predictors  WHERE  name='housing_predictor';

You will see something like this:

![prediction_tb](https://user-images.githubusercontent.com/59201258/141511308-19d16dee-40ed-495a-94b6-becf26ae36ee.PNG)

Now for predicting on any data:
 

    SELECT price
    from mindsdb.housing_predictor
    WHERE when_data='{"grade":5,"bedrooms":4,"bathrooms":2,"sqft_living":890,"floors":1,"condition_of":3}';

Now you will see result like that:

![answer](https://user-images.githubusercontent.com/59201258/141511536-f18ad990-84c3-4c1b-97b6-2001bc5f0ddc.PNG)


You successfully completed this tutorial congrats,now you can run mindsdb locally on you SQL databases!
