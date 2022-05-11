## What is MindsDB?

Data is your single most important asset and your data lives in a database. By bringing machine learning to the database, MindsDB accelerates the speed of machine learning development.

With MindsDB, you will easily build, train, optimize and deploy your models. Your predictions will be available via the queries you already use.


![Machine Learning in Database using SQL](/assets/what_is_mindsdb.png)

## What are AI Tables?

There is an ongoing transformational shift within the modern business world from the “what happened and why” based on historical data analysis to the “what will we predict can happen and how can we make it happen” based on machine learning predictive modeling.

![Analytics](/assets/sql/tutorials/snowflake-superset/1-ML_audience.png)

The success of your predictions depends both on the data you have available and the models you train this data on. Data Scientists and Data Engineers need best-in-class tools to prepare the data for feature engineering, the best training models, and the best way of deploying, monitoring, and managing these implementations for optimal prediction confidence.

### Machine Learning (ML) Lifecycle

The ML lifecycle can be represented as a process that consists of the data preparation phase, modeling phase, and deployment phase. The diagram below presents all the steps included in each of the stages.

![ML Workflow](/assets/sql/tutorials/snowflake-superset/2-ML_workflow.png)

Companies looking to implement machine learning have found their current solutions require substantial amounts of data preparation, cleaning, and labeling, plus hard to find machine learning/AI data scientists to conduct feature engineering; build, train, and optimize models; assemble, verify, and deploy into production; and then monitor in real-time, improve, and refine. Machine learning models require multiple iterations with existing data to train. Additionally, extracting, transforming, and loading (ETL) data from one system to another is complicated, leads to multiple copies of information, and is a compliance and tracking nightmare.

A recent study has shown it takes 64% of companies a month, to over a year, to deploy a machine learning model into production¹. Leveraging existing databases and automating the feature engineering, building, training, and optimization of models, assembling them, and deploying them into production is called AutoML and has been gaining traction within enterprises for enabling non-experts to use machine learning models for practical applications.

![Classical ML](/assets/sql/tutorials/snowflake-superset/3-AI_Tables-income-debt.jpg)

MindsDB brings machine learning to existing SQL databases with a concept called AI Tables. AI Tables integrate the machine learning models as virtual tables inside a database, create predictions, and can be queried with simple SQL statements. Almost instantly, time series, regression, and classification predictions can be done directly in your database.

### Deep Dive into the AI Tables

Let’s consider the following income table that stores the income and debt values.

```sql
SELECT income, debt FROM income_table;
```

![AI_Tables-income_table](/assets/sql/tutorials/snowflake-superset/3-AI_Tables-income_table.jpg)

A simple visualization of the data present in the income table is as follows.

![Income vs Debt](/assets/sql/tutorials/snowflake-superset/4-AI_Tables-income-debt-query.jpg)

Querying the income table to get the debt value for a particular income value results in the following.

```sql
SELECT income, debt FROM income
WHERE income = 80000;
```

![Income vs Debt table](/assets/sql/tutorials/snowflake-superset/5-debt-income-query-table.jpg)

![Income vs Debt chart](/assets/sql/tutorials/snowflake-superset/5-debt-income-query.jpg)

But what happens when we query the table for income value that is not present?

```sql
SELECT income, debt FROM income WHERE income = 90000;
```

![Income vs Debt table](/assets/sql/tutorials/snowflake-superset/6-debt-income-query-null-table.jpg)

![Income vs Debt query](/assets/sql/tutorials/snowflake-superset/6-debt-income-query-null.jpg)

When a table doesn’t have an exact match the query will return a null value. This is where the AI Tables come into play!

Let’s create a debt model that allows us to approximate the debt value for any income value. We’ll train this debt model using the income table’s data.

```sql
CREATE PREDICTOR mindsdb.debt_model FROM income_table PREDICT debt;
```

MindsDB provides the **CREATE PREDICTOR** statement. When we execute this statement, the predictive model works in the background, automatically creating a vector representation of the data that can be visualized as follows.

![Income vs Debt model](/assets/sql/tutorials/snowflake-superset/7-debt-income-query-ml.jpg)

Let’s now look for the debt value of some random income value. To get the approximated debt value, we query the debt_model and not the income table.

```sql
SELECT income, debt FROM debt_model WHERE income = 90120;
```

![Income vs Debt model](/assets/sql/tutorials/snowflake-superset/7-debt-income-query-ml-table.jpg)


## How can I help?

You can help in the following ways:

 * Trying MindsDB and [reporting issues](https://github.com/mindsdb/mindsdb/issues/new/choose).
 * If you know python, you can also help us debug open issues. Issues labels with the `good first issue` tag should be [the easiest to start with](https://github.com/mindsdb/mindsdb/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22).
 * You can help us with documentation and examples.
 * Tell your friends, write a blog post about MindsDB.
 * Join our team, we are growing fast so [we should have a few open positions](https://career.mindsdb.com/).

## Why is it called MindsDB?

Well, as most names, we needed one, we like science fiction and the [culture series](https://en.wikipedia.org/wiki/The_Culture_(series)), where there are these AI super smart entities called Minds.

How about the DB part? Although in the future we will support all kinds of data, currently our objective is to add intelligence to existing data stores/databases, hence the term DB.
As to becoming a **Mind** to your **DB**.

Why the bear? Who *doesn't* like bears! Anyway, a bear for UC Berkeley where this all was initially coded.