# Welcome to the MindsDB Manual QA Testing for Access Handler

> **Please submit your PR in the following format after the underline below `Results` section. Don't forget to add an underline after adding your changes i.e., at the end of your `Results` section.**

## Testing Access Handler with [Dataset Name](https://raw.githubusercontent.com/mindsdb/mindsdb-examples/a43f66f0250c460c0c4a0793baa941307b09c9f2/others/diabetes_example/dataset/diabetes-train.csv)

**1. Testing CREATE DATABASE**

``` Tested locally on Windows 10 with both 2013 and 2016 Access Runtimes installed (ran python -m mindsdb --install-handlers access) before running python -m mindsdb
CREATE DATABASE access_test
WITH
engine='access',
parameters={
    "db_file":"C:\\Work\\HacktoberFest\\ManualQA\\Diabetes.accdb"
};
```

![CREATE_DATABASE](https://user-images.githubusercontent.com/241893/199141975-e817adb0-55da-468e-9766-ca15f41aa388.png)

**2. Testing CREATE PREDICTOR**

```
N/A
```

![CREATE_PREDICTOR](Image URL of the screenshot)

**3. Testing SELECT FROM PREDICTOR**

```
N/A
```

![SELECT_FROM](Image URL of the screenshot)

### Results

Drop a remark based on your observation.
- [ ] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
- [X] There's a Bug 🪲 [Access Handler Not Behaving As Expected]((https://github.com/mindsdb/mindsdb/issues/3947) ( This means you encountered a Bug. Please open an issue with all the relevant details with the Bug Issue Template)

---
