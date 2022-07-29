# What is MindsDB?

Data is your single most important asset and your data lives in a database. By bringing machine learning to the database, MindsDB accelerates the speed of machine learning development.

With MindsDB, you will easily build, train, optimize and deploy your models. Your predictions will be available via the queries you already use.

![Machine Learning in Database using SQL](/assets/what_is_mindsdb.png)

## What are AI Tables?

MindsDB brings machine learning to existing SQL databases with a concept called AI Tables. AI Tables integrate the machine learning models as virtual tables inside a database, create predictions, and can be queried with simple SQL statements. Almost instantly, time series, regression, and classification predictions can be done directly in your database.

## Deep Dive into the AI Tables

### The problem

Let’s consider the following income table that stores the income and debt values.

```sql
SELECT income, debt 
FROM income_table;
```

```sql
+------+-----+
|income|debt |
+------+-----+
|60000 |20000|
|80000 |25100|
|100000|30040|
|120000|36010|
+------+-----+
```

A simple visualization of the data present in the income table is as follows:

<figure markdown> 
    ![Income vs Debt](/assets/sql/income_vs_debt.png){ width="800", loading=lazy  }
    <figcaption></figcaption>
</figure>



Querying the income table to get the debt value for a particular income value results in the following:

```sql
SELECT income, debt 
FROM income_table
WHERE income = 80000;
```

```sql
+------+-----+
|income|debt |
+------+-----+
|80000 |25100|
+------+-----+
```

<figure markdown> 
    ![Income vs Debt chart](/assets/sql/income_vs_debt_known_value.png){ width="800", loading=lazy  }
    <figcaption>Green dot and dashed line as query result</figcaption>
</figure>

But what happens when we query the table for income value that is not present?

```sql
SELECT income, debt
FROM income
WHERE income = 90000;
```

```sql
Empty set (0.00 sec)
```

In other words, Nothing! no valuable information at all.

<figure markdown> 
    ![Income vs Debt query](/assets/sql/income_vs_debt_unknown_value.png){ width="800", loading=lazy  }
    <figcaption>Dashed red line describing the absense of a value (blue dot) for the query</figcaption>
</figure>

When a table doesn’t have an exact match the query will return an empty set or null value. This is where the AI Tables come into play!

### The solution

Let’s create a debt model that allows us to approximate the debt value for any income value. We’ll train this debt model using the income table’s data.

```sql
CREATE PREDICTOR mindsdb.debt_model
FROM income_table 
PREDICT debt;
```

On execution, we get:

```sql
Query OK, 0 rows affected (x.xxx sec)
```

MindsDB provides the [`#!sql CREATE PREDICTOR`](/sql/create/predictor/) statement. When we execute this statement, the predictive model works in the background, automatically creating a vector representation of the data that can be visualized as follows:

<figure markdown> 
    ![Income vs Debt model](/assets/sql/income_vs_debt_predictor.png){ width="800", loading=lazy  }
    <figcaption> Green line describing the Predictor (model) created</figcaption>
</figure>

Let’s now look for the debt value of some random income value. To get the approximated debt value, we query the `#!sql mindsdb.debt_model` instead of the `#!sql income_table`.

```sql
SELECT income, debt
FROM mindsdb.debt_model 
WHERE income = 90000;
```

```sql
+------+-----+
|income|debt |
+------+-----+
|90000 |27820|
+------+-----+
```

<figure markdown> 
    ![Income vs Debt model](/assets/sql/income_vs_debt_prediction.png){ width="800", loading=lazy  }
    <figcaption> Dashed blue line describing the query of the model with the predicted value (dark blue dot) </figcaption>
</figure>

## What is MindsDB, why is MindsDB important?
### Shift on Data Analysis Paradigm

There is an ongoing transformational shift within the modern business world from the “what happened and why” based on historical data analysis to the “what will we predict can happen and how can we make it happen” based on machine learning predictive modeling.

<figure markdown> 
    ![Analytics](/assets/sql/analytics_shift.png){ width="600", loading=lazy  }
    <figcaption></figcaption>
</figure>

The success of your predictions depends both on the data you have available and the models you train this data on. Data Scientists and Data Engineers need best-in-class tools to prepare the data for feature engineering, the best training models, and the best way of deploying, monitoring, and managing these implementations for optimal prediction confidence.

### The Machine Learning (ML) Lifecycle

The ML lifecycle can be represented as a process that consists of the data preparation phase, modeling phase, and deployment phase. The diagram below presents all the steps included in each of the stages.


<figure markdown> 
    ![ML Workflow](/assets/sql/machine_learning_lifecycle.png){ width="600", loading=lazy  }
    <figcaption></figcaption>
</figure>

Companies looking to implement machine learning have found their current solutions require substantial amounts of data preparation, cleaning, and labeling, plus hard to find machine learning/AI data scientists to conduct feature engineering; build, train, and optimize models; assemble, verify, and deploy into production; and then monitor in real-time, improve, and refine. Machine learning models require multiple iterations with existing data to train. Additionally, extracting, transforming, and loading (ETL) data from one system to another is complicated, leads to multiple copies of information, and is a compliance and tracking nightmare.

A recent study has shown it takes 64% of companies a month, to over a year, to deploy a machine learning model into production. Leveraging existing databases and automating the feature engineering, building, training, and optimization of models, assembling them, and deploying them into production is called AutoML and has been gaining traction within enterprises for enabling non-experts to use machine learning models for practical applications.

### Why is it called MindsDB?

Well, as most names, we needed one, we like science fiction, and the [culture series](https://en.wikipedia.org/wiki/The_Culture_(series)), where there are these AI super-smart entities called Minds.

How about the DB part? Although in the future we will support all kinds of data, currently our objective is to add intelligence to existing data stores/databases, hence the term DB.
As to becoming a **Mind** to your **DB**.

Why the bear? We wanted to honor the open source tradition of animals related to projects! We went for a bear because of UC Berkeley where this all was initially coded. But we liked a cooler bear so went for a Polar Bear.

### How Can You Help Democratize Machine Learning?

You can help in the following ways:

- [X] Trying MindsDB and [reporting issues](https://github.com/mindsdb/mindsdb/issues/new/choose).

- [X] If you know python, you can also help us debug open issues. Issues labels with the `good first issue` tag should be [the easiest to start with](https://github.com/mindsdb/mindsdb/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22).

- [X] You can help us with documentation, tutorial and examples.

- [X] Tell your friends, to spread the word, i.e write a blog post about MindsDB.

- [X] Join our team, we are growing fast so [we should have a few open positions](https://mindsdb.com/careers/).