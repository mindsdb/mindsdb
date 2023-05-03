# Google Content API for Shopping Integration

This handler integrates with
the [Google Content API for Shopping](https://developers.google.com/shopping-content/guides/quickstart)
to allow you to use Google Search data in your SQL queries.

## Example: Predicting price of product based on Google Content API for Shopping activity

To see how the Google Content API for Shopping handler is used, let's walk through a simple example to create a model to predict
the price of a product based on Shopping activity.

## Connect to the Content API for Shopping

We start by creating a database to connect to the Google Content API for Shopping. Currently, there is no need for an
API key:

However, you will need to have a Google account and have enabled the Google Content API for Shopping.
Also, you will need to have the credentials in a json file.
You can find more information on how to do
this [here](https://developers.google.com/shopping-content/guides/quickstart/setting-up-a-client-library).

**Optional:**  The credentials file can be stored in the google_content_shopping handler folder in
the [mindsdb/integrations/google_content_shopping_handler](mindsdb/integrations/handlers/google_content_shopping_handler)
directory.

~~~~sql
CREATE
DATABASE my_content
WITH  ENGINE = 'google_content_shopping',
parameters = {
    'credentials': 'C:\\Users\\panagiotis\\Desktop\\GitHub\\mindsdb\\mindsdb\\integrations\\handlers\\google_content_shopping_handler\\credentials.json',
    'merchant_id': '1234567890'
};    
~~~~

This creates a database called my_content. This database ships with a table called AccountsTable, OrderTable and with a
table called ProductsTable that we can use to search for

## Get the list of accounts

Let's get an account.

~~~~sql
SELECT *
FROM my_content.Accounts
WHERE accountId = 123456789
~~~~

## Remove an account

Now let's remove an account.

~~~~sql
DELETE
FROM my_content.Accounts
WHERE accountId = 123456789
~~~~

## Get the list of orders

Let's test by getting the list of orders.

~~~~sql
SELECT *
FROM my_content.Orders
WHERE placedDateStart = '2020-10-01'
  AND placedDateEnd = '2020-10-31'
~~~~

## Delete some orders

Now let's delete some orders.

~~~~sql
DELETE
FROM my_content.Orders
WHERE orderId > 123
  AND orderId < 456
~~~~

## Get the list of products

Let's get the list of products.

~~~~sql
SELECT *
FROM my_content.Products
WHERE productId > 123456789
~~~~

## Update a product

Now let's update some products.

~~~~sql
UPDATE my_content.Products
SET title = 'New Title'
WHERE productId > 123456789
  AND updateMask = 'title'
~~~~

## Delete a product

Now let's delete some products.

~~~~sql
DELETE
FROM my_content.Products
WHERE productId > 123456789
~~~~

### Note 

If you have specified only one aspect of the comparison (`>` or `<`), then the `start_id` will be `end_id` - 10 (
if `start_id` is
not defined) and the `end_id` will be `start_id` + 10 (if `end_id` is defined).

## Creating a model to predict future product prices

Now we can use machine learning for sales predictions, inventory management,
product recommendations, and other automations.

~~~~sql
CREATE
PREDICTOR future_product_prices
FROM my_content.Products
PREDICT price
~~~~