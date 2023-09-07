# Welcome to the MindsDB Manual QA Testing for Couchbase Handler

## Testing Couchbase Handler with [travel-sample data](https://docs.couchbase.com/server/current/manage/manage-settings/install-sample-buckets.html#install-sample-buckets-with-the-ui)

**1. Testing CREATE DATABASE**

```
CREATE DATABASE couchbase_datasource
WITH
    engine = 'couchbase',
    parameters = {
        "host": "localhost",
        "bucket": "test-bucker",
        "user": "admin",
        "password": "password"
    };

```

[![couchbase-create-database.png](https://i.postimg.cc/MKwHdRCB/couchbase-create-database.png)](https://postimg.cc/RqPv03VC)


**2. Testing SELECT FROM DATABASE**

```
SELECT * FROM couchbase_datasource.airport;
```

[![Screenshot-from-2023-09-05-23-26-33.png](https://i.postimg.cc/WpXCHcK7/Screenshot-from-2023-09-05-23-26-33.png)](https://postimg.cc/XXBsrmnZ)

### Results

Drop a remark based on your observation.
- [X] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
- [] There's a Bug 🪲 [Issue Title](URL To the Issue you created) ( This means you encountered a Bug. Please open an issue with all the relevant details with the Bug Issue Template)
