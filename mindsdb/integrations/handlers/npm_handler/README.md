# NPM Handler

This handler allows you to interact with the data that [npms.io](https://npms.io) stores stats about various NPM packages.

## About NPM

The free npm Registry has become the center of JavaScript code sharing, and with more than two million packages, the largest software registry in the world.

## NPM Handler Implementation

This implementation is based on the API service provided at [api.npms.io](https://api.npms.io/).

## NPM Handler Initialization

There is nothing needed to be passed in the database initialization process. You can create the database via the following flow.

```sql
CREATE DATABASE npm_datasource
WITH ENGINE = 'npm';
```
