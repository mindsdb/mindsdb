
## Examples 

Create ml engine. service_account is content of service_account.json
```
create ml_engine vertex from vertex
using 
project_id="mindsdb-401709",
location="us-central1",
staging_bucket="gs://my_staging_bucket",
experiment="my-experiment",
experiment_description="my experiment description",
service_account = {
  ...
};
```

Create model:
```sql
CREATE MODEL mindsdb.vertex_anomaly_detection_model
PREDICT cut
USING 
    engine = 'vertex',
    model_name = 'diamonds_anomaly_detection',
    custom_model = True;
```

Call prediction
```sql
SELECT t.cut, m.cut as anomaly
FROM files.vertex_anomaly_detection as t
JOIN mindsdb.vertex_anomaly_detection_model as m;
```
