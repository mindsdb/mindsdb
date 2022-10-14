# Welcome to the MindsDB Manual QA Testing for Databricks Handler

> **Please submit your PR in the following format after the underline below `Results` section. Don't forget to add an underline after adding your changes i.e., at the end of your `Results` section.**

## Testing Databricks Handler with [BankNotesDataset](https://archive.ics.uci.edu/ml/machine-learning-databases/00267/data_banknote_authentication.txt)

**1. Testing CREATE DATABASE**

```
CREATE DATABASE databricks_datasource
WITH ENGINE='databricks',
PARAMETERS={
  "server_hostname": "adb-2739211854327427.7.azuredatabricks.net",
  "http_path": "sql/protocolv1/o/2739211854327427/1008-130317-9mq6rcp6",
  "access_token": "dapib9c127dc406d9ddb9d42a4f170f398df-3"
};
```

![CREATE_DATABASE](https://github.com/saldanhad/mindsdb/blob/tests/databrickstestimages/createdb.jpg)


### Results

Drop a remark based on your observation.
- [x] There's a Bug 🪲 [Issue](https://github.com/mindsdb/mindsdb/issues/3387)

---
