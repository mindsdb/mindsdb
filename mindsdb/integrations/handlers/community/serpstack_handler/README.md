# Serpstack Handler

Serpstack handler for MindsDB connects to the Serpstack API and pulls data into MindsDB.

---

## Table of Contents

- [Serpstack Handler](#serpstack-handler)
  - [Table of Contents](#table-of-contents)
  - [About Serpstack](#about-serpstack)
  - [Serpstack Handler Implementation](#serpstack-handler-implementation)
  - [Serpstack Handler Initialisation](#serpstack-handler-initialisation)
  - [How to Get Your Serpstack Access Key](#how-to-get-your-serpstack-access-key)
  - [Implemented Features](#implemented-features)
  - [TODO](#todo)
  - [Example Usage](#example-usage)

---

## About Serpstack

Serpstack is a real-time search engine results API that provides accurate and reliable search data. It allows users to perform keyword searches and retrieve various types of search results including organic results, images, videos, news, and shopping results etc.

## Serpstack Handler Implementation

This handler was implemented using the Serpstack API. The API provides a simple and efficient way to access search engine results programmatically.

## Serpstack Handler Initialisation

The Serpstack handler is initialised with the following parameter:

- `access_key`: a required API access key for the Serpstack API

## How to Get Your Serpstack Access Key

1. Sign up for an account on [Serpstack](https://serpstack.com).
2. Choose the plan that suits your needs.
3. Once the account is created, an access key will be generated for you. This key is required to authenticate API requests.

## Implemented Features

- Fetch organic search results based on a query.
- Fetch image search results based on a query.
- Fetch video search results based on a query.
- Fetch news search results based on a query.
- Fetch shopping search results based on a query.

## TODO

- Potentially create more tables for:
  - Knowledge graph
  - Inline tweets
  - Related searches
  - Questions

## Example Usage

### Create the Serpstack Database

```sql
CREATE DATABASE my_serpstack
WITH 
    ENGINE = 'serpstack',
    PARAMETERS = {
     "access_key": "YOUR_ACCESS_KEY"
    };
```

After setting up the Serpstack handler, the user can use SQL queries to fetch data from search engines using Serpstack. Note that the `query` parameter is required in order to make searches. If no results are found for a given query, a table filled with 'No results found' will be returned instead.

### Fetch Organic Search Results

```sql
SELECT * 
FROM my_serpstack.organic_results
WHERE query = 'KFC';
```

### Fetch Image Search Results

```sql
SELECT * 
FROM my_serpstack.image_results
WHERE query = 'Dinosaurs';
```

### Fetch Video Search Results

```sql
SELECT * 
FROM my_serpstack.video_results
WHERE query = 'NBA Finals';
```

### Fetch News Search Results

```sql
SELECT * 
FROM my_serpstack.news_results
WHERE query = 'Euros 2024';
```

### Fetch Shopping Search Results

```sql
SELECT * 
FROM my_serpstack.shopping_results
WHERE query = 'Nvidia 4090';
```

### Example with Additional Parameters

You can include additional request parameters in your SQL queries. Refer to the [Serpstack documentation](https://serpstack.com/documentation) under **HTTP GET Request Parameters** for a full list of available parameters.

```sql
SELECT * 
FROM my_serpstack.organic_results
WHERE query = 'McDonalds' AND gl = 'no' AND page = '2';
```