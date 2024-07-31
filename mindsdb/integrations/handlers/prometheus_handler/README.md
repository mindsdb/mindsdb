---
title: Prometheus
sidebarTitle: Prometheus Handler
---

This documentation describes the intragration of MindsDB with [Prometheus](https://prometheus.io/), an open-source solution that allows you to collect different types of metrics from your host fleet in order to monitor their health, perfomance, usage, etc. 

## Data Models 
The data model used by Prometheus to store data is quite different that the relational model of SQL queries. Prometheus uses a timeseries model and exposes the stored data via PromQL. Translating any query from SQL to PromQL would be quite complex and no library was found that could serve that need. For the sake of providing some basic capability, we made some assumptions on how to map Prometheus' timeseries model to a relational model and we support specific types of queries which are simple to translate:

1. Each Prometheus metric is mapped to a SQL table.
2. Each Prometheus label is mapped to a SQL column. 
2. We support SELECT queries with WHERE filters and AND conditions. 

For example, consider the following PromQL query and its SQL mapping: 

```
http_requests_total{environment="development"}
```

```sql
SELECT
    *
FROM http_requests_total
WHERE environment = "development"
```

While quite restrictive, this simplification would make posible a large amount of simple use-cases. 

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](/setup/self-hosted/docker) or [Docker Desktop](/setup/self-hosted/docker-desktop).
2. Install ```prometheus-api-client``` the only requirement of the Prometheus handler: 

```bash 
pip install prometheus-api-client
```

## Connection
To setup a connection to Prometheus host, run the following SQL in the MindsDB interface: 

```sql
CREATE DATABASE prometheus_sourxe
WITH
  engine = 'prometheus',
  parameters = {
    "host": "https://127.0.0.1:9090",
    "user": "user",
    "password": "password"
  };
```

The only required paremeter is the `host`, either IP or hostname of the host running the Prometheus server. The hostname must be in the format of the example. If you are looking to test the handler locally, you can download Prometheus from their official [website](https://prometheus.io/download/)


Optional connection parameters include the following:

- `user`: The username to connect to the Prometheus server with.
- `password`: The password to authenticate the user with the Prometheus server.


## Usage

After setting up the connection, you can retrieve data from a specified index using the following:

```sql
SELECT 
    *
FROM prometheus_source.http_requests_total
LIMIT 10;
```

If you want to add filters, use SQL where:

```sql
SELECT 
    *
FROM prometheus_source.http_requests_total
WHERE code="200" AND handler="/"
LIMIT 10;
```

## Tests

To run the unittests, you must first have a locally running Prometheus server, then run the following: 

```bash
env PYTHONPATH=./ pytest mindsdb/integrations/handlers/prometheus_handler/tests/test_prometheus_handler.py
```

## Limitations & TODOs

Currently, we only support the types of SQL queries mentioned above and we do not fully validate the input query (TBD). 
Also, we could utilize the orderby returned by the SELECTQueryParser to support ```ORDERBY col``` statements. 
