# Cryptopanic Handler

 Cryptopanic Handler, a custom data handler for MindsDB that enables fetching data from Cryptopanic, a social news website focused on cryptocurrency
---

## Table of Contents

- [About Cryptopanic](#about-Cryptopanic)
  - [Cryptopanic Handler Implementation](#Cryptopanic-handler-implementation)
  - [Implemented Features](#implemented-features)
  - [Example Usage](#example-usage)
---
## About Cryptopanic

Cryptopanic allows users to submit and discuss news articles and other content related to cryptocurrency. It provides a valuable source of information for those interested in staying updated on the latest happenings in the crypto world.


## Cryptopanic Handler Implementation

This handler was implemented using the official Cryptopanic API. It provides a simple and easy-to-use interface to access the Cryptopanic API.


## Implemented Features

- Fetch news from a website based following inputted param.
filters:
curiencies:
regions:
kind:



## Example Usage
```
CREATE DATABASE crypto_news
WITH 
ENGINE = 'cryptopanic'
USING
api_token='********';
```

After setting up the Cryptopanic Handler, you can use SQL queries to fetch data from Cryptopanic:

```sql
SELECT *
FROM crypto_news.news
LIMIT 250;
```
