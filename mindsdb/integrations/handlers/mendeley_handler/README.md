# Mendeley API Handler
This handler integrates with the Mendeley API.

### Connect to the Mendeley API
We start by creating a database to connect to the Mendeley API. In order to do that we need the client id and client secret that are created after registering an application at https://dev.mendeley.com/myapps.html . More information on the matter can be found at https://dev.mendeley.com/reference/topics/application_registration.html .

```
CREATE DATABASE my_mendeley
WITH
  ENGINE = 'mendeley'
  PARAMETERS = {
    "client_id" : "the client id",
     "client_secret" : "the client secret"
  };
```

### Search for documents
Using the Mendeley Handler you can find information about documents of your interest such as a document's id, title, type, source, year, identifiers, keywords, link and authors.
In order to conduct your search you can choose from a number of supported parameters. Those parameters are:

### First category of parameters

* title – Title.
* author – Author.
* source – Source.
* abstract – Abstract.
* min_year – Minimum year for documents to return.
* max_year – Maximum year for documents to return.
* open_access – If ‘true’, only returns open access documents.

### Second category of parameters

* arxiv – ArXiV ID.
* doi – DOI.
* isbn – ISBN.
* issn – ISSN.
* pmid – PubMed ID.
* scopus – Scopus ID (EID).
* filehash – SHA-1 filehash.

### Third category of parameters

* id – the ID of the document to get


The first category of parameters is not considered very specific compared to the other two, so the use of parameters from only that category may result in a document catalog and not a single document. If one is in need of information about a specific document and can use parameters from many categories, it is in his best interest to use those of the second or third category, since they return the most specific result, a single document (provided the validity of the parameters and the existence of the document in mendeley catalogs)

### Fields returned

Through the use of the handler one has access to a document's:

  * title
  * type
  * source
  * year
  * pmid
  * sgr
  * issn
  * scopus
  * doi
  * pui
  * authors
  * keywords
  * link
  * id

Of course through the use of SELECT, one can choose what fields to display.

### Examples

If using parameters from the first category, since the result may not be a single document but more, one can use LIMIT to specify the number of documents to return. The default number of documents returned is 30.

```
SELECT *
FROM my_mendeley.catalog_search_data
WHERE title = "COVID-19 diagnosis and management: a comprehensive review"
LIMIT 10;
```

```
SELECT *
FROM my_mendeley.catalog_search_data
WHERE doi = "10.1111/joim.13091"
```

```
SELECT *
FROM my_mendeley.catalog_search_data
WHERE id = "c3503ef8-26eb-3666-87db-03ccc422293a"
```