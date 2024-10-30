# IBM Cloud Object Storage

```sql
CREATE DATABASE ibm_datasource
WITH ENGINE = 'ibm_cos',
    PARAMETERS = {
        'cos_hmac_access_key_id': 'your-access-key-id',
        'cos_hmac_secret_access_key': 'your-secret-access-key',
        'cos_endpoint_url': 'https://s3.eu-gb.cloud-object-storage.appdomain.cloud',
        'bucket': 'your-bucket-name'
    };
```
