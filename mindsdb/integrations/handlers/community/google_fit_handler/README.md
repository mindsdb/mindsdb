# Google Fit API Handler

To start connecting to your Google Fit app, visit [Google Fit Authorization](https://developers.google.com/fit/rest/v1/get-started) to obtain an authorization from Google and use the credentials.json file for the following steps.

To create a database connected to Google Fit, you can either specify a path to the credentials file or manually input the credentials fields

To connect using a path to the credentials file, run:
```
CREATE DATABASE my_google_fit
With 
    ENGINE = 'google_fit',
    PARAMETERS = {
      "service_account_file": "Absolute path to the credentials file"
    };
```
To connect using manually typed credentials, run:
```
CREATE DATABASE my_google_fit
With 
    ENGINE = 'google_fit',
    PARAMETERS = {
      "service_account_json": {
        "client_id": "cient id from the credentials file",
        "project_id": "project id from the credentials file",
        "auth_uri": "auth_uri from the credentials file",
        "token_uri": "token uri from the credentials file",
        "auth_provider_x509_cert_url": "auth_provider_x509_cert_url from the credentials file",
        "client_secret": "client secret from the credentials file"
      }
    };
```

This creates a database called my_google_fit. This database contains a table called aggregated_data that we can use to obtain our Google Fit data such as the step count.


## Searching for step count in SQL

To search your step count data based on a time

```
SELECT *
FROM my_google_fit 
WHERE 
   date > 'year-month-day'
LIMIT 20;
```

Note that in the WHERE clause, '>' means that the date is the start date and the end date is the current date, '<' means that the date is the end date and the start date is about one month ago.
If WHERE clause is not supplied, the data is of the last month by default.

Once you have the data, you can utilize MindsDB's AI features to manipulate and extract information from it.

## Special Notice
This is still a draft handler, and we will keep on adding features such as the options to search for other types of data based on the query.
