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

- **filters**:
  - Allows filtering content based on various criteria.
  - Available options: `rising`, `hot`, `bullish`, `bearish`, `important`, `saved`, `lol`.
  - Example usage: `filter='rising'`.

- **currencies**:
  - Filters content based on specified currency codes.
  - Maximum of 50 currency codes can be provided.
  - Example usage: `currencies='BTC,ETH'`.

- **regions**:
  - Filters content based on specified region codes.
  - Default region is `en` (English).
  - Available regions: `en`, `de`, `nl`, `es`, `fr`, `it`, `pt`, `ru`, `tr`, `ar`, `cn`, `jp`, `ko`.
  - Example usage: `regions=fr,es`.

- **kind**:
  - Filters content based on the type of content.
  - Default is to include all types of content.
  - Available values: `news` or `media`.
  - Example usage: `kind=news`.

- **following**:
  - Filters content to show only feeds that the user is following.
  - Accepts a boolean value (`true` or `false`).
  - Example usage: `following=true`.

NOTE: Returns 20 news by default but can be customize by LIMIT clause

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
