# Welcome to the MindsDB Manual QA Testing for Bigquery Handler

> **Please submit your PR in the following format after the underline below `Results` section. Don't forget to add an underline after adding your changes i.e., at the end of your `Results` section.**

## Testing Bigquery Handler with [Dataset Name](URL to the Dataset)

**1. Testing CREATE DATABASE**

```
CREATE DATABASE bqdataset
WITH ENGINE = "bigquery",
PARAMETERS = {
   "project_id": "my-project-493512",
   "dataset": "mytestdataset",
   "service_account_json": {
        "type": "service_account",
  "project_id": "my-project-493512",
  "private_key_id": "842ghtyf45fty45sdf4545s7dfs245gsgsgf5752sdgrg",
  "private_key": "-----BEGIN PRIVATE KEY-----\asdhfdlkfaldkfADFADF4564dfadf\n-----END PRIVATE KEY-----\n",
  "client_email": "my-project-493512@my-project-493512.iam.gserviceaccount.com",
  "client_id": "15165432156542316542",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/my-project-493512%40my-project-493512.iam.gserviceaccount.com"
        }
   }
```

![Capture10](https://github.com/fkamau1/mindsdb/assets/70659811/3605e978-f666-45db-b968-78736015eedc)


**2. Testing CREATE MODEL**

```
COMMAND THAT YOU RAN TO CREATE MODEL.

CREATE MODEL winningNumbers
FROM bqdataset
    (SELECT Draw_Date, Winning_Numbers FROM winningNumbers)
PREDICT Winning_Numbers;

```

![Capture7](https://github.com/fkamau1/mindsdb/assets/70659811/5226759e-23d3-4bfc-bc9f-264e35562618)

**3. Testing SELECT FROM MODEL**

```
SELECT t.Winning_Numbers AS actual_number,
       p.Winning_Numbers AS predicted_number
FROM bqdataset.winningNumbers AS t
JOIN mindsdb.winningnumbers AS p
WHERE p.Draw_Date = "2010-09-14"
AND p.Mega_Ball = "1";
```

![Capture9](https://github.com/fkamau1/mindsdb/assets/70659811/29c13e63-4e32-4ba0-92fe-4026365e8c4b)


### Results

Drop a remark based on your observation.
- [x] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
- [ ] There's a Bug 🪲 [Issue Title](URL To the Issue you created) ( This means you encountered a Bug. Please open an issue with all the relevant details with the Bug Issue Template)

---
