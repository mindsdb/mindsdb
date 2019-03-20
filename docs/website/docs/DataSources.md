[<Back to Table of Contents](../README.md)

Our goal is to make it very simple to ingest and prepare data that we can feed into MindsDB we call these **DataSources**, here are the basics:

## Data Sources

MindsDB.learn **from_data** argument can be any of the follows:
 * **file**: can be a path in the localsystem, stringio object or a url to a file, supported types are (csv, json, xlsx, xls).
  * **data frame**: A [pandas DataFrame](), pandas is one of the most use data preparation libraries out there, so it makes sense that we support this.
 * **MindsDB data source**: MindsDB has a special class for datasources which lends itself for simple data ingestion and preparation, as well as to combine various datasources to learn from. 


### MindsDB data source:

MindsDB datasource is an enriched version of a pandas dataframe so all methods in the pandas dataframe also apply to MindsDB, 
However, the DataSource class provides a way to implement data loading transformations and cleaning of data.

Some special implementations of datasources that already do cleaning and various tasks:

You can learn about them in [here](https://github.com/mindsdb/mindsdb/tree/master/mindsdb/libs/data_sources):

#### FileDS

An important one is the one MindsDB uses to work with files.

```python
from mindsdb import FileDS

ds = FileDS(file, clean_header = True, clean_rows = True, custom_parser = None)


# Now you can pass this DataSource to MindsDB
mdb.learn(
    from_data=ds,
    predict='<what column you want to predict>', # the column we want to learn to predict given all the data in the file
    model_name='<the name you want to give to this model>' # the name of this model
)

```
FileDS arguments are:

* ***file*** can be a path in the localsystem, stringio object or a url to a file, supported types are (csv, json, xlsx, xls).
* ***clean_header*** (default=True) clean column names, so that they don't have special characters or white spaces.
* ***clean_rows*** (default=True) Goes row by row making sure that nulls are nulls and that no corrupt data exists in them, it also cleans empty rows
* ***custom_parser*** (default=None) special function to extract the actual table structure from the file in case you need special transformations.


