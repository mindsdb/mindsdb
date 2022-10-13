# Google Sheets Handler

This is the implementation of the Google Sheets handler for MindsDB.

## Google Sheets
Google Sheets is a spreadsheet program included as part of the free, web-based Google Docs Editors suite offered by Google.
https://en.wikipedia.org/wiki/Google_Sheets

## Implementation
This handler was implemented using `duckdb`, a library that allows SQL queries to be executed on `pandas` DataFrames.

In essence, when querying a particular sheet, the entire sheet will first be pulled into a `pandas` DataFrame using the Google Visualization API. Once this is done, SQL queries can be run on the DataFrame using `duckdb`.

Note: Since the entire sheet needs to be pulled into memory first (DataFrame), it is recommended to be somewhat careful when querying large datasets so as not to overload your machine.

The documentation for the Google Visualization API is available here,
<br>
https://developers.google.com/chart/interactive/docs/reference

The required arguments to establish a connection are,
* `spreadsheet_id`: the unique ID of the Google Sheet.
* `sheet_name`: the name of the sheet within the Google Sheet.

## Usage
In order to make use of this handler and connect to a Google Sheet in MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE sheets_datasource
WITH
engine='sheets',
parameters={
    "spreadsheet_id": "12wgS-1KJ9ymUM-6VYzQ0nJYGitONxay7cMKLnEE2_d0",
    "sheet_name": "iris"
};
~~~~

Now, you can use this established connection to query your table as follows,
~~~~sql
SELECT * FROM sheets_datasource.example_tbl
~~~~

At the moment, only `SELECT` queries are allowed to be executed through `duckdb`. This, however, has no restriction on running machine learning algorithms against your data in Airtable using `CREATE PREDICTOR` statements.