# Google Cloud Storage Handler
A Handler for handling connections and interacting with buckets in [Google Cloud Storage](https://cloud.google.com/storage//?gad=1) using Google Cloud Storage API.

## Connecting to the Google Cloud Storage API
In order for this to work you will need:
1. to have a Google Account and created project in [Google Cloud Storage](https://cloud.google.com/storage//?gad=1)
2. to create a service account for that project with Storage Administrator privileges
3. to generate JSON "keyfile" with credentials for connecting to the cloud and put it into the [Google Cloud Storage Handler folder](.) (mindsdb/integrations/handlers/google_cloud_storage_handler)

~~~sql
CREATE
DATABASE my_storage
WITH  ENGINE = 'google_cloud_storage',
parameters = {
    'keyfile': 'keyfile.json'
};    
~~~

## Selecting buckets from storage (SQL)
~~~sql
SELECT id, name, timeCreated, location
FROM my_storage.buckets
WHERE timeCreated > '2023-11-11';
~~~

## Creating a bucket in storage (SQL)
~~~sql
INSERT INTO my_storage.buckets (name, user_project, location, storageClass)
VALUES ('test-bucket', 'test-project', 'us', 'STANDARD_STORAGE_CLASS')
~~~

## Updating a bucket in storage (SQL)
~~~sql
UPDATE my_storage.buckets 
SET storageClass = 'STANDARD_STORAGE_CLASS' 
WHERE name = 'test-bucket';
~~~

## Deleting a bucket in storage (SQL)
~~~sql
DELETE FROM my_storage.buckets 
WHERE name = 'test-bucket';
~~~
