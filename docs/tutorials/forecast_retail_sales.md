---
title: "Forecast Retail Sales with MindsDB using CSV Data"
description: "Learn how to use MindsDB to forecast retail sales from a CSV file using SQL commands."
tags: [tutorial, forecasting, csv, sql, timeseries, mindsdb]
---

# Forecast Retail Sales with MindsDB using CSV Data

## 📝 Introduction

In this tutorial, you’ll learn how to use **MindsDB** to build a machine learning model that predicts future retail sales using time-series forecasting.  
We’ll walk through loading a CSV dataset, connecting it to MindsDB, training a predictor model, and making forecasts — all using simple SQL commands.

This guide is perfect for anyone getting started with MindsDB and time-series analysis.

---

## ⚙️ Prerequisites

Before you begin, make sure you have:

- **Python 3.10+** installed  
- **MindsDB** installed (`pip install mindsdb`) or running via Docker  
- A **CSV file** containing sales data. You can download a sample dataset:  
  👉 [retail_sales_sample.csv](https://raw.githubusercontent.com/mindsdb/mindsdb-examples/main/data/retail_sales_sample.csv)

Each row in this dataset contains:
| Date | Store | Product | Sales |
|------|--------|----------|--------|
| 2023-01-01 | Store_A | Shoes | 120 |
| 2023-01-02 | Store_A | Shoes | 135 |
| ... | ... | ... | ... |

---

## 🪄 Step 1 — Start MindsDB

Start a local MindsDB instance:

```bash
python -m mindsdb

or via Docker:

docker run -p 47334:47334 -p 47335:47335 mindsdb/mindsdb

Once running, you can connect using MindsDB Studio (browser UI) or any SQL client like DBeaver or MySQL Workbench.

Default connection:

Host: 127.0.0.1
Port: 47334
User: mindsdb
Password: mindsdb

📁 Step 2 — Create a Datasource for the CSV File

We’ll connect our CSV file to MindsDB so it can query it like a normal table.

CREATE DATABASE retail_sales
WITH ENGINE = 'csv',
PARAMETERS = {
  "path": "/path/to/retail_sales_sample.csv"
};


✅ Tip: Use the absolute path to your CSV file. On Windows, escape backslashes like:

"path": "C:\\Users\\Uday\\Documents\\retail_sales_sample.csv"


Now check your data:

SELECT * FROM retail_sales LIMIT 5;


You should see the first few rows from your dataset.

🤖 Step 3 — Train a Forecasting Model

Now, create a model that predicts future sales based on previous data.

CREATE MODEL retail_sales_forecaster
PREDICT sales
USING
  engine = 'mindsdb',
  order_by = 'date',
  group_by = 'store, product',
  window = 7,
  horizon = 7,
  max_epochs = 20,
  data = (SELECT * FROM retail_sales);


Let’s unpack that:

order_by = 'date' tells MindsDB it’s a time-series.

group_by allows separate forecasts per product/store.

window defines how many past days to consider.

horizon defines how many days ahead to forecast.

You can monitor the training process:

SELECT * FROM mindsdb.models WHERE name='retail_sales_forecaster';

🔮 Step 4 — Generate Forecasts

Once the model is trained, you can make predictions directly via SQL.

To forecast the next 7 days for Store_A - Shoes:

SELECT date, sales
FROM retail_sales_forecaster
WHERE store = 'Store_A' AND product = 'Shoes'
ORDER BY date DESC;


MindsDB automatically extrapolates the future values based on your horizon.

📊 Step 5 — Visualize Results (Optional)

You can export the results to CSV and visualize in Python, Excel, or Tableau.

For example, using Python + Pandas:

import mysql.connector
import pandas as pd

conn = mysql.connector.connect(
    host='127.0.0.1', port=47334,
    user='mindsdb', password='mindsdb'
)

query = """
SELECT date, sales
FROM retail_sales_forecaster
WHERE store='Store_A' AND product='Shoes'
"""
df = pd.read_sql(query, conn)
df.plot(x='date', y='sales', title='Predicted Sales Forecast')

🧩 Step 6 — (Optional) Improve Accuracy

You can enhance your model by:

Adding weather or promotion data as extra columns

Increasing max_epochs for more training cycles

Changing the window or horizon parameters

Trying a different ML engine (like Lightwood or Prophet)

Example:

ALTER MODEL retail_sales_forecaster
SET engine = 'prophet';

✅ Conclusion

You’ve just created a complete sales forecasting pipeline using MindsDB — from raw CSV data to predictive insights — all through SQL!

In this tutorial, you learned:

How to connect data sources in MindsDB

How to train a time-series forecasting model

How to query your model for future predictions

Now you can apply these steps to your own business data or explore MindsDB’s integrations with PostgreSQL, Snowflake, MongoDB, and more.