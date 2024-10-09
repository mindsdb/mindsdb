# Firestore Handler

This is the implementation of the Firestore Handler for MindsDB.

## Firestore

Firestore is a NoSQL document database built for automatic scaling, high performance, and ease of application development. It is part of the Firebase ecosystem by Google and allows you to store, sync, and query data for mobile and web apps at global scale.

## Implementation

This handler uses the `google-cloud-firestore` Python library to connect to a Firestore instance. It allows users to interact with Firestore collections and documents seamlessly.

The required arguments to establish a connection are:

- `credentials_file`: Path to the Firebase credentials JSON file that contains your service account information.
- `project_id`: The Google Cloud Project ID associated with the Firestore instance.

## Usage

In order to use this handler and connect to Firestore within MindsDB, the following syntax can be used:

### Connect to Firestore

```sql
CREATE DATABASE firestore_db
WITH ENGINE = "firestore",
PARAMETERS = {
   "credentials_file": "path/to/serviceAccountKey.json",
   "project_id": "your_project_id"
};
```

### Insert data into Firestore

You can insert data into a Firestore collection by specifying the table schema and providing the data source.

```sql
CREATE TABLE firestore_db.users (
    SELECT * FROM mysql_demo_db.users
);
```

### Fetch data from Firestore

Once the data is inserted, you can retrieve data by querying the Firestore collection like so:

```sql
SELECT * FROM firestore_db.users;
```

### Filter data

You can apply conditions to filter records based on document fields.

``` sql
SELECT * FROM firestore_db.users
WHERE age > 25;
```

### Update data in Firestore
To update existing documents in Firestore, simply specify the primary key and the new data.

```sql
UPDATE firestore_db.users
SET city = "Los Angeles"
WHERE name = "Alice";
```

### Delete data from Firestore
To delete records from a Firestore collection, you can use the DELETE query.

```sql
DELETE FROM firestore_db.users
WHERE name = "Charlie";
```

### Example
Here’s a complete example to show how you can connect to Firestore, insert data, and query it:

``` sql
-- Create the database connection
CREATE DATABASE firestore_db
WITH ENGINE = "firestore",
PARAMETERS = {
   "credentials_file": "path/to/serviceAccountKey.json",
   "project_id": "your_project_id"
};

-- Insert data
CREATE TABLE firestore_db.users (
    SELECT name, age, city FROM mysql_demo_db.users
);

-- Fetch all data
SELECT * FROM firestore_db.users;

-- Update a record
UPDATE firestore_db.users
SET city = "Miami"
WHERE name = "Bob";

-- Fetch updated record
SELECT * FROM firestore_db.users WHERE name = "Bob";

-- Delete a record
DELETE FROM firestore_db.users WHERE name = "Charlie";

-- Fetch remaining records
SELECT * FROM firestore_db.users;

```