# Strapi Handler

Strapi handler is a MindsDB handler for Strapi. It allows you to query Strapi collections using SQL.

## What is Strapi ?

Strapi is the leading open-source Headless CMS. Strapi gives developers the freedom to use their favorite tools and frameworks while allowing editors to easily manage their content and distribute it anywhere.

## Strapi Handler Initialization

The Strapi handler is initialized with the following parameters:

- `host` - the host of the Strapi server
- `port` - the port of the Strapi server
- `api_token` - the api token of the Strapi server
- `plural_api_ids` - the list of plural api ids of the collections

## Implemented Features

- `SELECT` - select data from a collection
- `WHERE` - filter data from a collection
- `LIMIT` - limit data from a collection
- `INSERT` - insert data into a collection
- `UPDATE` - update data from a collection

Note: We can use collection name as table name in SQL.

## Example Usage

The first step is to create a database with the new `Strapi` engine.

```sql
CREATE DATABASE myshop --- display name for database.
WITH ENGINE = 'strapi', --- name of the mindsdb handler
PARAMETERS = {
  "host" : "<strapi-host>", --- host, it can be an ip or an url.
  "port" : "<strapi-port>",  --- common port is 1337.
  "api_token": "<your-strapi-api-token>", --- api token of the strapi server.
  "plural_api_ids" : ["<plural-api-id>"] --- plural api ids of the collections.
};
```

Example:

```sql
CREATE DATABASE myshop
WITH ENGINE = 'strapi',
PARAMETERS = {
  "host" : "localhost",
  "port" : "1337",
  "api_token": "c56c000d867e95848c",
  "plural_api_ids" : ["products", "sellers"]
};
```

---

### SELECT

```sql
SELECT *
FROM myshop.<collection-name>;
```

Example:

```sql
SELECT *
FROM myshop.products;
```

---

### WHERE

```sql
SELECT *
FROM myshop.<collection-name>
WHERE <field-name> = <value>;
```

Example:

```sql
SELECT description, price
FROM myshop.products
WHERE id = 1;
```

---

### LIMIT

```sql
SELECT *
FROM myshop.<collection-name>
LIMIT <limit>;
```

Example:

```sql
SELECT *
FROM myshop.products
LIMIT 10;
```

---

### INSERT

```sql
INSERT INTO myshop.<collection-name> (<field-name-1>, <field-name-2>, ...)
VALUES (<value-1>, <value-2>, ...);
```

Example:

```sql
INSERT INTO myshop.sellers (name, email, sellerid)
VALUES ('Ram', 'ram@gmail.com', 'ram');
```

Note: You only able to insert data into the collection which has `create` permission.

---

### UPDATE

```sql
UPDATE myshop.<collection-name>
SET <field-name-1> = <value-1>, <field-name-2> = <value-2>, ...
WHERE <field-name> = <value>;
```

Example

```sql

UPDATE myshop.products
SET price = 299,
avaiablity = false
WHERE id = 1;
```

Note: You only able to update data into the collection which has `update` permission.
