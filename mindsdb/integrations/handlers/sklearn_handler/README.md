# Sklearn Handler

Scikit-learn-based handler for [MindsDB](https://github.com/mindsdb/mindsdb) that enables using logistic regression or linear regression models for classification and regression tasks.

---

## Table of Contents

- [Sklearn Handler](#sklearn-handler)
  - [About scikit-learn](#about-scikit-learn)
  - [Handler Implementation](#handler-implementation)
  - [Handler Initialization](#handler-initialization)
  - [Implemented Features](#implemented-features)
  - [Example Usage](#example-usage)

---

## About scikit-learn

[scikit-learn](https://scikit-learn.org/stable/) is an open-source machine learning library for Python. It features various classification, regression, and clustering algorithms and is built on top of SciPy, NumPy, and matplotlib.

---

## Handler Implementation

The handler is implemented using the `Pipeline` module from scikit-learn. It supports automatic model selection between:

- `LogisticRegression` for classification tasks
- `LinearRegression` for regression tasks

It also performs automatic one-hot encoding for categorical features and leverages the `type_infer` module for target type detection.

---

## Handler Initialization

This ML handler is triggered via a `CREATE MODEL` SQL query. The following parameters are supported:

- `engine`: Must be set to `'sklearn'`
- `target`: Column name to predict
- Input table: Must be available in a supported file-based or table-based database

Example:

```sql
CREATE MODEL mindsdb.sklearn_model
FROM files
  (SELECT * FROM train)
PREDICT target
USING engine = 'sklearn';