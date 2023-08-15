# Google Books API Integration

This handler integrates with the [Google Books API](https://developers.google.com/books/docs/overview) to allow you to
make book and bookshelf data available to use for model training and predictions.

## Example: Automate your book recommendations

To see how the Google Books handler is used, let's walk through a simple example to create a model to predict
your future book recommendations.

## Connect to the Google Books API

We start by creating a database to connect to the Google Books API. Currently, there is no need for an API key:

However, you will need to have a Google account and have enabled the Google Books API.
Also, you will need to have the credentials
in a json file. You can find more information on how to do
this [here](https://developers.google.com/identity/protocols/oauth2/service-account).

**Optional:**  The credentials file can be stored in the google_books handler folder in
the [mindsdb/integrations/google_books_handler](mindsdb/integrations/handlers/google_books_handler) directory.

~~~~sql
CREATE
DATABASE my_books
WITH  ENGINE = 'google_books',
parameters = {
    'credentials': 'C:\\Users\\panagiotis\\Desktop\\GitHub\\mindsdb\\mindsdb\\integrations\\handlers\\google_books_handler\\credentials.json'
};    
~~~~

This creates a database called my_books. This database ships with a table called bookshelves and a table called volumes
that we can use to search for
info related to the users bookshelves and volumes.

## Searching for bookshelves in SQL

Let's get a list of bookshelves in our account.

~~~~sql
SELECT id,
       title,
       description
FROM my_books.bookshelves
WHERE userId = '1001'
  AND title = 'My Bookshelf'
~~~~

or

~~~~sql
SELECT id,
       title,
       description
FROM my_books.bookshelves
WHERE shelf > 10
  AND shelf < 20
~~~~

**Note**: If you have specified only one aspect of the comparison (`>` or `<`), then the `minShelf` will be `maxShelf` -
10 (
if `minShelf` is
not defined) and the `maxShelf` will be `minShelf` + 10 (if `maxShelf` is not defined).

## Searching for volumes in SQL

Let's get a list of volumes in our account.

~~~~sql
SELECT id,
       title,
       description
FROM my_books.volumes
WHERE q = 'Harry Potter'
~~~~

## Creating a model to predict future book recommendations

Now we can use ML for book recommendations,
reading history analysis, and other automations based on our Google Books activity.

~~~~sql
CREATE
PREDICTOR recommend_books
FROM my_books.volumes
PREDICT 
    title,
    description
~~~~