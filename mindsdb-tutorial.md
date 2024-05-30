# MindsDB House Price Prediction Tutorial

## Introduction
In this tutorial, we will walk through the steps to create and use a predictor with MindsDB to estimate house prices based on various features such as the number of rooms, square footage, and location. We will use the "California Housing Prices" dataset from the UC Irvine Machine Learning Repository.

## Data Setup
First, download the California Housing Prices dataset from [here](https://www.dcc.fc.up.pt/~ltorgo/Regression/cal_housing.tgz). The dataset contains the following features:
- `longitude`: Longitude coordinate of the house.
- `latitude`: Latitude coordinate of the house.
- `housing_median_age`: Median age of the house.
- `total_rooms`: Total number of rooms in the house.
- `total_bedrooms`: Total number of bedrooms in the house.
- `population`: Population in the block.
- `households`: Number of households in the block.
- `median_income`: Median income of the households in the block.
- `median_house_value`: Median house value (target variable).

## Connecting the Data
Upload the dataset to a location accessible by MindsDB, such as an S3 bucket, Google Drive, or directly to MindsDB's storage if supported. In this example, we'll assume you have the CSV file locally.

1. **Upload the dataset**:
    ```sql
    CREATE DATABASE california_housing
    WITH ENGINE = 'file'
    SETTINGS = {
        "source": "/path/to/cal_housing.csv",
        "format": "csv"
    };
    ```

2. **Verify the connection**:
    ```sql
    SHOW TABLES FROM california_housing;
    ```

## Understanding the Data
Let's take a look at the first few rows of the dataset:
```sql
SELECT *
FROM california_housing.california_housing
LIMIT 10;
```
##  Training a Predictor
Create a predictor to estimate the `median_house_value` using the other features:
```sql
CREATE PREDICTOR house_price_predictor
FROM california_housing
(PREDICT median_house_value
USING longitude, latitude, housing_median_age, total_rooms, total_bedrooms, population, households, median_income);

```
## Status of a Predictor
Check the status of the predictor:
```sql
SELECT status FROM mindsdb.predictors WHERE name =  'house_price_predictor';

Wait until the status is `complete` before proceeding.

```
## Status of a Predictor
Query for prediction results:
```sql
SELECT median_house_value, house_price_predictor.median_house_value AS predicted_value
FROM california_housing.california_housing
JOIN house_price_predictor
ON california_housing.california_housing.id = house_price_predictor.id
LIMIT 10;
```
## Making a Single Prediction
To make a single prediction:
```sql
SELECT house_price_predictor.median_house_value
FROM house_price_predictor
WHERE longitude = -122.23 AND latitude = 37.88 AND housing_median_age = 41
  AND total_rooms = 880 AND total_bedrooms = 129 AND population = 322
  AND households = 126 AND median_income = 8.3252;

```
##  Making Batch Predictions
For batch predictions, use the `JOIN` clause:
```sql
SELECT california_housing.*, house_price_predictor.median_house_value AS predicted_value
FROM california_housing.california_housing
JOIN house_price_predictor
ON california_housing.california_housing.id = house_price_predictor.id;

```

## Using a Sentiment Classifier Model

You can also join your data with a sentiment classifier model to analyze sentiment. Here’s an example:

```sql

`SELECT some_text_column, sentiments.sentiment
FROM your_data_table
JOIN sentiment_classifier_model AS sentiments;` 
```
Run this in the MindsDB editor to find out the output.
##
#### What's Next?
Want to learn more about MindsDB? Check out these resources:

-   [MindsDB](https://mindsdb.com/)
-   [MindsDB Documentation](https://docs.mindsdb.com/)
-   [Slack](https://mindsdb.com/joincommunity)
-   [GitHub](https://github.com/mindsdb/mindsdb/)


