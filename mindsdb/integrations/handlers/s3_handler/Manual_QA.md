# Welcome to the MindsDB Manual QA Testing for AWS S3 Handler

> **Please submit your PR in the following format after the underline below `Results` section. Don't forget to add an underline after adding your changes i.e., at the end of your `Results` section.**

## Testing AWS S3 Handler with [Job Placement Prediciton](https://www.kaggle.com/datasets/ahsan81/job-placement-dataset)

**1. Testing CREATE DATABASE**

```
CREATE DATABASE test_manual_job_placement_data       --- display name for database.
WITH ENGINE = 's3',
PARAMETERS = {
    "aws_access_key_id": "AKIAQY4UBNVN7GGXHEHG",      --- the AWS access key
    "aws_secret_access_key": "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",  --- the AWS secret access key
    "region_name": "ap-south-1",             --- the AWS region
    "bucket": "mindsdb-tutorial",                 --- the name of the S3 bucket
    "key": "Job_Placement_Data.csv",                    --- the key of the object to be queried
    "input_serialization": "{'CSV': {'FileHeaderInfo': 'NONE'}}"    --- the format of the data in the object that is to be queried
};
```

![CREATE_DATABASE](https://user-images.githubusercontent.com/81156510/220292479-f185114d-c3da-42ef-a847-2e061f465a6d.png)

**2. Testing CREATE PREDICTOR**

```
CREATE PREDICTOR mindsdb.test_manual_placement_status
FROM test_manual_job_placement_data (
select *
FROM S3Object
) PREDICT status;
```

![CREATE_PREDICTOR](https://user-images.githubusercontent.com/81156510/220292725-d725790e-d423-4fc6-99dc-084896552a23.png)

**3. Testing SELECT FROM PREDICTOR**

```
SELECT *
FROM mindsdb.models
WHERE name='test_manual_placement_status';
```

![SELECT_FROM](https://user-images.githubusercontent.com/81156510/220292283-152340fd-0533-4d54-8b59-1ddf1f5cc3f2.png)

### Results

Drop a remark based on your observation.
- [x] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
- [ ] There's a Bug 🪲 [Issue Title](URL To the Issue you created) ( This means you encountered a Bug. Please open an issue with all the relevant details with the Bug Issue Template)

---