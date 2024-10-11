# Shopify Handler

Shopify handler for MindsDB provides interfaces to connect to Shopify via APIs and pull store data into MindsDB.

---

## Table of Contents

- [Shopify Handler](#shopify-handler)
  - [Table of Contents](#table-of-contents)
  - [About Shopify](#about-shopify)
  - [Shopify Handler Implementation](#shopify-handler-implementation)
  - [Shopify Handler Initialization](#shopify-handler-initialization)
  - [Implemented Features](#implemented-features)
  - [TODO](#todo)
  - [Example Usage](#example-usage)

---

## About Shopify

Shopify is a complete commerce platform that lets you start, grow, and manage a business.
<br>
https://www.shopify.com/blog/what-is-shopify

## Shopify Handler Implementation

This handler was implemented using [shopify_python_api](https://github.com/Shopify/shopify_python_api), the Python SDK for Shopify.

## Shopify Handler Initialization

The Shopify handler is initialized with the following parameters:

- `shop_url`: a required URL to your Shopify store.
- `access_token`: a required access token to use for authentication.

These are the optional parameters:

- `yotpo_app_key`: token needed to access customer reviews via the Yotpo Product Reviews app.
- `yotpo_access_token`: token needed to access customer reviews via the Yotpo Product Reviews app.

If you want to query customer reviews, use the [Yotpo Product Reviews](https://apps.shopify.com/yotpo-social-reviews) app available in Shopify. Here are the steps to follow:
1. Install the [Yotpo Product Reviews](https://apps.shopify.com/yotpo-social-reviews) app for your Shopify store.
2. Generate `yotpo_app_key` following [this instruction](https://support.yotpo.com/docs/finding-your-yotpo-app-key-and-secret-key) for retrieving your app key. Learn more about [Yotpo authentication here](https://apidocs.yotpo.com/reference/yotpo-authentication).
3. Generate `yotpo_access_token` following [this instruction](https://develop.yotpo.com/reference/generate-a-token).
Watch this video on creating a Shopify access token [here](https://www.youtube.com/watch?v=4f_aiC5oTNc&t=302s).

## Implemented Features

- [x] Shopify Products Table for a given Store
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
  - [x] Support UPDATE
  - [x] Support INSERT
  - [x] Support DELETE
- [x] Shopify Customers Table for a given Store
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
  - [x] Support UPDATE
  - [x] Support INSERT
  - [x] Support DELETE
  - [x] Support WHERE
- [x] Shopify Orders Table for a given Store
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
  - [x] Support UPDATE
  - [x] Support INSERT
  - [x] Support DELETE
- [x] Shopify Customer Reviews Table for a given Store
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
- [x] Shopify Inventory Level Table for a given Store
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
- [x] Shopify Carrier Service Table for a given Store
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
- [x] Shopify Shipping Zone Table for a given Store
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
- [x] Shopify Sales Channel for a given Store
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection

## TODO

- [ ] Support UPDATE and DELETE for Customers table
- [ ] Support INSERT, UPDATE and DELETE for Product table
- [ ] Shopify Payments table
- [ ] Shopify Inventory table
- [ ] Shopify Discounts table
- [ ] Many more

## Example Usage

The first step is to create a database with the new `shopify` engine by passing in the required `shop_url` and `access_token` parameters. Optionally, you can provide additional keys, `yotpo_app_key` and `yotpo_access_token`, to access customer reviews.

~~~~sql
CREATE DATABASE shopify_datasource
WITH ENGINE = 'shopify',
PARAMETERS = {
  "shop_url": "your-shop-name.myshopify.com",
  "access_token": "shppa_...",
  "yotpo_app_key": "...",
  "yotpo_access_token": "..."
};
~~~~

Use the established connection to query your database:

~~~~sql
SELECT * FROM shopify_datasource.products
~~~~

~~~~sql
SELECT * FROM shopify_datasource.carrier_service;
~~~~

~~~~sql
SELECT * FROM shopify_datasource.shipping_zone;
~~~~

Run more advanced SELECT queries:

~~~~sql
SELECT  id, title
FROM shopify_datasource.products
WHERE status = 'active'
ORDER BY id
LIMIT 5
~~~~

It is also possible to INSERT data into your Shopify store. At the moment, only the `customers`, `products`, and `orders` tables support INSERT:

~~~~sql
INSERT INTO shopify_datasource.customers(first_name, last_name, email)
VALUES 
('John', 'Doe', 'john.doe@example.com')
~~~~

~~~~sql
INSERT INTO shopify_datasource.products(title, vendor, tags)
VALUES 
('Product Name', 'Vendor Name', 'new, sale, winter')
~~~~

~~~~sql
INSERT INTO shopify_datasource.orders(title_li, price_li, quantity, test)
VALUES 
("Product Name", 25.00, 3, true)
~~~~

A limited number of columns are supported for INSERT for each table: 

The `products` table supports the following columns: 'title', 'body_html', 'vendor', 'product_type', 'tags', and 'status'. Of these, 'title' must be provided.

The `customers` table supports the following columns: 'first_name', 'last_name', 'email', 'phone', 'tags' and 'currency'. Of these, either 'first_name', 'last_name', 'email' or 'phone' must be provided. 

The `orders` table supports the following columns: 'billing_address', 'discount_codes', 'buyer_accepts_marketing', 'currency', 'email', 'financial_status', 'fulfillment_status', 'line_items', 'note', 'note_attributes', 'phone', 'po_number', 'processed_at', 'referring_site', 'shipping_address', 'shipping_lines', 'source_name', 'source_identifier', 'source_url', 'tags', 'taxes_included', 'tax_lines', 'test', 'total_tax', 'total_weight'. 

Of these columns, 'billing_address', 'discount_codes', 'line_items', 'note_attributes', 'shipping_address', 'shipping_lines', and 'tax_lines' are comprised of sub-components as follows:

'billing_address': 'address1_ba', 'address2_ba', 'city_ba', 'company_ba', 'country_ba', 'country_code_ba', 'first_name_ba', 'last_name_ba', 'latitude_ba', 'longitude_ba', 'name_ba', 'phone_ba', 'province_ba', 'province_code_ba', 'zip_ba'

'discount_codes': 'amount_dc', 'code_dc', 'type_dc'

'line_items': 'gift_card_li', 'grams_li', 'price_li', 'quantity_li', 'title_li', 'vendor_li', 'fulfillment_status_li', 'sku_li', 'variant_title_li'

'note_attributes': 'name_na', 'value_na'

'shipping_address': 'address1_sa', 'address2_sa', 'city_sa', 'company_sa', 'country_sa', 'country_code_sa', 'first_name_sa', 'last_name_sa', 'latitude_sa', 'longitude_sa', 'name_sa', 'phone_sa', 'province_sa', 'province_code_sa', 'zip_sa'

'shipping_lines': 'code_sl', 'price_sl', 'discounted_price_sl', 'source_sl', 'title_sl', 'carrier_identifier_sl', 'requested_fulfillment_service_id_sl', 'is_removed_sl'

'tax_lines': 'price_tl', 'rate_tl', 'title_tl', 'channel_liable_tl'

The value fields 'price_li' and 'title_li' must be provided.

It is also possible to DELETE data from your Shopify store. At the moment, only the `customers`, `products`, and `orders` tables support DELETE:

~~~~sql
DELETE FROM shopify_datasource.customers
WHERE first_name = 'John'
AND last_name = 'Doe'
AND email = 'john.doe@example.com';
~~~~

~~~~sql
DELETE FROM shopify_datasource.orders
WHERE id=5632671580477;
~~~~


Inventory details for the products can be queried as follows:

~~~~sql
SELECT  *
FROM shopify_datasource.inventory_level
WHERE inventory_item_ids="id1,id2" AND location_ids="id1,id2"
ORDER BY available
LIMIT 5
~~~~

`inventory_item_ids` or `location_ids` have to be specified in the `where` clause of the query. 

For querying locations, you can use the `locations` table:

~~~~sql
SELECT  id, name, address
FROM shopify_datasource.locations
ORDER BY id
LIMIT 5
~~~~

For the `customer_reviews` table, only SELECT is supported.

~~~~sql
SELECT  *
FROM shopify_datasource.customer_reviews
WHERE score=5
ORDER BY id
LIMIT 5
~~~~

For the `customers` table, DELETE is supported too. You can delete the customers as follows:

~~~~sql
DELETE FROM shopify_datasource.customers
WHERE verified_email = false;
~~~~

For the `orders` table, UPDATE is supported. You can update the orders as follows:
~~~~sql
UPDATE shopify_datasource.orders
SET email="abc@your_domain.com"
WHERE id=5632671580477;
~~~~
