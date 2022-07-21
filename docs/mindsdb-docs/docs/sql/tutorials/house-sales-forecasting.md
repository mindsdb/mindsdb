# Forecast quarterly house sales using MindsDB

## Introduction

In this short example we will produce forecasts for a multivariate time series.

## Data and setup

The dataset we will use is the pre-processed version of the "House Property Sales" [Kaggle competition](https://www.kaggle.com/datasets/htagholdings/property-sales), in particular, the "ma_lga_12345.csv" file, which tracks quarterly moving averages of house sales aggregated by type and amount of bedrooms in each listing.

Make sure you have access to a working MindsDB installation (either local or via cloud.mindsdb.com), and either load it into a table in your database of choice or upload the file directly to the special `FILES` datasource (via SQL or GUI). Once you've done this, proceed to the next step. For the rest of the tutorial we'll assume you've opted for the latter option and uploaded the file with the name `HR_MA`:

```
SHOW TABLES FROM files;

SELECT * FROM files.HR_MA LIMIT 10;
```

## Create a time series predictor

Now, we can specify that we want to forecast the `MA` column, which is a moving average of the historical median price for house sales. However, looking at the data you can see several entries for the same date, depending on two factors: how many bedrooms the properties had, and whether properties were "houses" or "units". This means that we can have up to ten different groupings here (although, if you do some digging, you will find we only actually have seven of the possible ten combinations in practice).

MindsDB makes it simple so that we don't need to repeat the predictor creation process for every group there is. Instead, we can just group for both columns and the predictor will learn from all series and enable forecasts for all of them! The command for this is:

```
CREATE PREDICTOR 
  mindsdb.home_sales_model
FROM files
  (SELECT * FROM HR_MA)
PREDICT MA
ORDER BY saledate
GROUP BY bedrooms, type
-- as the target is quarterly, we will look back two years to forecast the next one
WINDOW 8
HORIZON 4;  
```

You can check the status of the predictor:

```
SELECT * FROM mindsdb.predictors where name='home_sales_model';
```

## Generating some forecasts

Once the predictor has been successfully trained, you can query it to get forecasts for a given period of time. Usually, you'll want to know what happens right after the latest training data point that was fed, for which we have a special bit of syntax, the "LATEST" key word:

```
SELECT m.saledate as date,
       m.MA as forecast
FROM mindsdb.home_sales_model as m 
JOIN files.HR_MA as t
WHERE t.saledate > LATEST AND t.type = 'house' AND t.bedrooms = 2
LIMIT 4;
```

Now, try changing `type` to unit or `bedrooms` to any number between 1 to 5, and check how the forecast varies. This is because MindsDB recognizes each grouping as being its own different time series.

## What's Next?

Have fun while trying it out yourself!

* Bookmark [MindsDB repository on GitHub](https://github.com/mindsdb/mindsdb).
* Sign up for a free [MindsDB account](https://cloud.mindsdb.com/register).
* Engage with the MindsDB community on [Slack](https://mindsdb.com/joincommunity) or [GitHub](https://github.com/mindsdb/mindsdb/discussions) to ask questions and share your ideas and thoughts.

If this tutorial was helpful, please give us a GitHub star [here](https://github.com/mindsdb/mindsdb).
