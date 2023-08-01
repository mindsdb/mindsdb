# Welcome to the MindsDB Manual QA Testing for Bigquery Handler

> **Please submit your PR in the following format after the underline below `Results` section. Don't forget to add an underline after adding your changes i.e., at the end of your `Results` section.**

## Testing Bigquery Handler with [Dataset Name](URL to the Dataset)

**1. Testing CREATE DATABASE**

```
CREATE DATABASE bqdataset
WITH ENGINE = "bigquery",
PARAMETERS = {
   "project_id": "my-project-394522",
   "dataset": "mytestdataset",
   "service_account_json": {
        "type": "service_account",
  "project_id": "my-project-394522",
  "private_key_id": "7d03ba8c664cf9f459662eab3c84d18348e043af",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDBYL/4Tm0ESbSK\nG7VcC22U32o2FE4OTWaut/y4Dw5iwmnzP6coasM1mxVl+ahaG0vnMOtq6juaiJhf\n4Q483bkgemV0YZ2QSShMdGi8fgZPnWrrGWkpPC1dr+00OpNPgOb4kYupwQtaGO07\nxH1xLBv2GNDAZDrGNJ4qrPil1ZTRYlh7IzRG5dCMOTfD8WXGQfmFl7EeFt0M+Ji6\npc2NrOvsKsVkogXEOKTUmJSlWXgiSJl/YZGOn5q2klfwu7C9qe9+qO34Xgeg36fR\nn6nJg77iRHRXOVQv6BxS8hOFsg3nHz6EjXnUlb7UMRTTkFokKdX3Kj/AUzsUorr6\nVqoL323NAgMBAAECggEABvcNowtsGMnw00KhOyH8Pe74G9+KWjCGgMeGtjhwm7UF\n7Ol/g1CpHE6sLCKcYjZ2eurDp/Os1umRl257PUz9JILUEaeMbcx8w5gzx4pAfvCc\nD8C4DXCp76RtBkMP2D670vsnDSOzXXjlXpA6tm9QyIHnTu6tQUNW6RGW2R5CwHHa\nxEryDUXWXDM65tr+T+IOryPPMC+EOwJnAha82YIjUr43+sWvpvfC4sSaL0nnCXwv\nEagbcY1qPPiHt5kbRJqh7Z+Tm+FvvytAl21/0DDY6IVJAs1QZsCDztLqKYmHMIdE\n3Fe6POQPMs1FVZFxCq5NYWVtnR9XzGcGx4VGIu8v0QKBgQD1TsvcT2j1x7QalpIz\nVyBixm3nBmXNWLpTDw+Wmk5kVEJVtpps6IrNLnJW/B9AHxGn9sTk540EkuozfjG4\nn2zYywp9NLxMHCQ3AJspd9tFvlWCKDUTV4w17YlKaBuZbniOmM4cTeFOOpNDWktU\nq6WN9Iey9fKPL3NxyAGRbjDL6QKBgQDJzoHtf3GdiuwhxvNgwZ9wjOWJrD+FiK9+\nZDLvEyWcX/FAO7Qm29njvPrPphKnIidSEqbbpiqF1GdgWl+UQ/2WNoT7v+KggKpU\n0QH7EMlgpQYmuBBumsR+u+AVeID0+vXQjX5XAZo9NYTcELE+gMalowBcCTrieVk8\nqlQMgEG4RQKBgGzD7TvXsdAYJdEMJfLBlMh29s/SVF0CFEsziFPcQxnVCjx65GmZ\nicGD8IqE605A+FEi5xYfXLVEdrcyItWbSmWtUQ9GzJ5qc9w3VpTCYeTAiaVWMoJK\n9Q4MLi1hj6suAiInumtuVJGdAyJ/7Jq0KImSfIBq8ItwHJ28coWK9/PRAoGAbELi\npNxXwSKD5uLqMZ/tvt13TlIfia14KB/syyDEbo1xyPm+fZI349q/2qHHI+5Ildj3\ntqH/8eeuxZv15n5LDkiVMtEP7PbZacLugxfQUI53LFJWTl+sxihX4GzTEZmIQaeG\nfXqYmfmaGszBqTxAVR+K09UHx/M9kjP1/vbXStkCgYEAqFGNrzI7kqZOcOE5ZKQa\nCiybiPgMDBfvIfEefYD0tB61QIc7P4gbuyXYLt/i3w/MVH9eZ0LCRxwWFf6B8QE2\nWXrzdWnGRjaGCb0DLJjlEo5K1fRB3valuWJJrSU+XlPe88G979tkLyKM0psbHVmT\nlX00ZHsJSB/Ld2Ntn6WStY8=\n-----END PRIVATE KEY-----\n",
  "client_email": "my-project-394522@my-project-394522.iam.gserviceaccount.com",
  "client_id": "102198119996236088169",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/my-project-394522%40my-project-394522.iam.gserviceaccount.com"
        }
   }
```
![Capture1](https://github.com/fkamau1/mindsdb/assets/70659811/32a42724-adeb-4399-a698-ad7808c83948)

![CREATE_DATABASE](Image URL of the screenshot)

**2. Testing CREATE MODEL**

```
COMMAND THAT YOU RAN TO CREATE MODEL.

CREATE MODEL winningNumbers
FROM bqdataset
    (SELECT Draw_Date, Winning_Numbers FROM winningNumbers)
PREDICT Winning_Numbers;

```

![CREATE_MODEL](Image URL of the screenshot)

![Capture7](https://github.com/fkamau1/mindsdb/assets/70659811/5226759e-23d3-4bfc-bc9f-264e35562618)

**3. Testing SELECT FROM MODEL**

```
SELECT Winning_Numbers
FROM mindsdb.winningnumbers
WHERE Draw_Date = "2023-08-01"
AND Mega_Ball = "1";
```


![SELECT_FROM](Image URL of the screenshot)
![Capture8](https://github.com/fkamau1/mindsdb/assets/70659811/cac7e89d-f483-44f7-80ff-485f46ff86ab)


### Results

Drop a remark based on your observation.
- [x] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
- [ ] There's a Bug 🪲 [Issue Title](URL To the Issue you created) ( This means you encountered a Bug. Please open an issue with all the relevant details with the Bug Issue Template)

---
