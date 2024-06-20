
# Using mongodb handler and server 

## Create integration:

Using mysql:

```
CREATE DATABASE mongo_int
WITH ENGINE = "mongodb",
PARAMETERS = {
           "host": "<mongodb_connection_string>",
           "database": "test_data"    
  }
```

Using mongodb:

```
db.databases.insertOne({
    'name': 'mongo_int', 
    'engine': 'mongodb',
    'connection_args': {
            "port": 27017,
            "host": "127.0.0.1",
            "database": "test"            
  }   
})
```

Here **mongo_int** is the name of integration. 
And it contains collection **house_sales**

Parameters or integration:
- host - hostname or ip or connection string
- database 
- port - optional, default is 27017
- user - if authorization is needed
- password 


## Querying data 

### Querying mongo collection from sql:

```
select * from mongo_int.house_sales
where type='house'
limit 10
```

To filter on datetime fields you need use 'cast as date' to declare type of value.
Otherwise, mongo server won't return anything
```
select * from mongo_int.house_sales
where saledate > cast('2018-03-30' as date)
```

### Querying data from mongo shell:

```
// get list integrations
show databases

// switch to mongo_int integration
use mongo_int 

// get list tables or collections
db.getCollectionNames()

```

Querying from mongodb collection

Because mongodb uses strict type comparison and don't do type cast automatically
you need to choose the same type of variable in filter as stored in collection.
```
use mongo_int

db.house_sales.find({ // filters
    'type': 'house',
    'saledate': {'$gt': ISODate("2018-03-30T00:00:00.000Z")}    
},
{'type': 1} // projection, optional
)
.sort({'Species': -1}) // sorting, optional
.limit(10)  // limit, optional
```

Querying from postgres table

Here it is possible to use implicit and explicit type. 
Database server tries to cast value automatically.
```
use postg_int

db.house_sales.find({ 
    '$and': [
        {'saledate': {'$gt': ISODate("2018-03-30T00:00:00.000Z")}},
        {'saledate': {'$lt': "2019-03-30")}}
    ]}      
})
```

Append data to collection:

```
db.house_sales.insertOne({
    'real_price': 10000
})
```

## Creating predictor:

From mysql:
```
CREATE PREDICTOR mindsdb.sales_model
FROM mongo_int (
    db.house_sales.find({ 'type': 'house'})    
) PREDICT sale_price
ORDER BY saledate
GROUP BY type
WINDOW 10
HORIZON 7
USING
    encoders.location.module='CategoricalAutoEncoder'
```
     
From mongo:
```
// working with mindsdb database
use mindsdb

db.models.insertOne(
{
     "name": "sales_model",
     "predict": "sale_price",
     "connection": "mongo_int",
     "select_data_query": "db.house_sales.find({ 'type': 'house'})",
     "training_options": { 
        "timeseries_settings": {                
            "order_by": ["saledate"],               
            "group_by": ["type"],                               
            "window": 10,
            "horizon": 7           
        },
        "encoders.location.module":"CategoricalAutoEncoder",
    }  
}
)
```

Retrain predictor: 
The same syntax as create model, but using "action": "retrain"
```
db.models.insertOne({
     "name": "sales_model",
     "action": "retrain",
})
```

Finetune predictor:
The same syntax as create model, but using "action": "finetune"
```
db.models.insertOne({
     "name": "sales_model",
     "action": "finetune",
})
```


Parameters of USING operator of sql query are located in training_options of mongo query 

If you have nested values in mongo collections. For example key hist_data contents list of objects like 
`{ sale_price: 80.38, saledate: '2020-08-01T00:00:00.000'}`

You can unnest it using aggregate mongo query:
```
"select_data_query": "db.house_sales.aggregate([\
{'$match': {'type': 'house'}},\
{'$unwind': '$hist_data'},\
{'$project': {'sale_price': '$hist_data.sale_price', 'saledate': '$hist_data.saledate', 'type': 1}}\
])"
```

- $unwind - is doing unnesting
- $project - is moving keys to top level

## List of models from mongo:
```
// all models
db.models.find({})

// filter by name
db.models.find({'name': "sales_model"})
```


## Using predictor:

Get prediction from model (one row select) using mongo shell
```
db.sales_model.find(
  {'type': 'house', 'sale_date': ISODate("2018-03-31T00:00:00.000Z")} 
)
```

Using join table and predictor from mongo.

Here you need to use explicit types because mongo don't do automatic type cast 
```
db.sales_model.find(
{
    "collection": "mongo_int.house_sales", 
    "query":  { 
        'type': 'house',
        "sale_date": {"$gt": ISODate("2018-03-31T00:00:00.000Z")}
    }
})
```

Alternative is to use aggregate function:

```
db.sales_model.aggregate([
    {'$match': {
        "collection": "mongo_int.house_sales", 
        "query":  { 
            'type': 'house',
            "sale_date": {"$gt": ISODate("2018-03-31T00:00:00.000Z")}
        }
    }},
    {'$project':  {
       'house_sales.sale_price': 'real_price',
       'sales_model.sale_price': 'predicted_price',
    }},
    {'$limit': 2},    
    {'$out': {'db': 'photorep', 'coll': 'aaa', 'append': true}}
])
```

'$match' step is required. The other steps are optional

'$out' - stores result to table. Possible values:
- {$out: 'photorep.aaa'} - create table aaa in integration photorep
- {$out: {'db': 'photorep', 'coll': 'aaa'}} - the same as above
- {$out: {'db': 'photorep', 'coll': 'aaa', 'append': true}} - append data to table


One more example:

Here to work with LATEST keyword $where operator is used.
It that case query is composed as string and "this" it is a link to record 

Also here is used a projection, that defined output. It should consist of two columns:
- orig_price - alias to house_sales.sale_price
- predicted_price - alias to sales_model.sale_price

```
db.sales_model.find(
{
    "collection": "mongo_int.house_sales", 
    "query":  { 
        "$where": "this.sale_date > latest and this.type = 'house'" 
    },
},
{ // projection block
    'house_sales.sale_price': 'orig_price',
    'sales_model.sale_price': 'predicted_price'
}
)
```

To work with nested values you can add modifiers in mindsdb queries that will pass to query to mongodb database. 
It will prepare data from mongo to pass it to predictor in expected format 

```
db.sales_model.find(
{
    "collection": "mongo_int.house_sales", 
    "query":  { 
        "$where": "this.sale_date > latest and this.type = 'house'" 
    },
    'modifiers': [
        {'$unwind': '$hist_data'},
        {'$project': {'sale_price': '$hist_data.sale_price', 'saledate': '$hist_data.saledate', 'type': 1}}
    ]
},
{ // projection block
    'house_sales.sale_price': 'orig_price',
    'sales_model.sale_price': 'predicted_price'
}
)
```


## Describe predictor

Here is the information about describing predictor:
https://docs.mindsdb.com/sql/api/describe/

How to call it from mongo
```
db.runCommand({describe: "sales_model.features"})
db.runCommand({describe: "sales_model.ensemble"})
db.runCommand({describe: "sales_model.model"})
```

## Delete models 
```
db.models.deleteOne({'name': "sales_model"})
```

## ML Engines

**Create**

`CREATE ml_engine ...` alternative in mongo:

```
db.ml_engines.insertOne({'name': "openai_2", "handler": "openai", "params": {"openai_api_key": "qqq"}})
```

**List**

`SHOW ml_engines` alternative in mongo:

```
db.ml_engines.find()
```

**Delete**

`DROP ml_engine ...` alternative in mongo:

```
db.ml_engines.deleteOne({"name": "openai_2"})
```

## Jobs

**Create**
```
db.jobs.insertOne({
    'name': 'job1',
    'schedule_str': 'every day',
    'start_at': '2023-03-30',    
    'end_at': '2023-03-30 11:11:11',
    'query': "\
        db.home_rentals_model.aggregate([\
            {'$match': {\
                'collection': 'example_db.demo_data.home_rentals',\
                'query':  {\
                    'number_of_rooms': 2\
                }\
            }},\
            {'$project':  {\
            'home_rentals.rental_price': 'real_price',\
            'home_rentals_model.rental_price': 'predicted_price',\
            }},\
            {'$limit': 2},\
            {'$out': {'db': 'photorep', 'coll': 'aaa', 'append': true}}\
        ])\
    "
})
```
schedule_str, start_at, end_at are optional. 

query - can be sql syntax string or mongo syntax string, 
in case of mongo syntax it will be converted to sql

One more example:
```
db.jobs.insertOne({
    'name': 'job2',
    'query': "select * from models"
})
```

**Delete**

```
db.jobs.deleteOne({'name': "job1"})
```


---

# Mongo server: technical docs

It implemented mongodb protocol to decode queries and encode response. 
Request is passed to responder responsible for request method. 
Responders are stored in separate classes each for one method (find, insert, stats, etc).  
Inside of responder mongo logic and mindsdb logic are linked. 
Target behavior is convert queries from mongo to AST-queries commands 
and then run them using executor and existing handlers.
Some parts of mongo server still process queries by themselves without using executor.

At the moment server handles limited set of methods. 
Methods that not implemented, but may be required to be used
- aggregate
- no alternatives for 'create table' and 'insert to' yet

**find**

Main method for performing querying is "find".
It uses MongoToAst class to transform mongo query to AST-query 

It can work in tho modes: normal and join mode

In normal mode all filters from mongo query
are passed to WHERE operator of AST-query. 

In join mode it is possible to emulate join between tables 
using 'collection' and 'query' in filter parameters. 
This mode is used to join table and predictor

For example this query:
```
use integration1
db.collection_name1.find({
    "collection": "integration2.collection_name2", 
    "query":  {
        'type': 'house',
    }
})
```
Is converted to:
```
select * from (
    select * from integration2.collection_name2 
    where type='house'
)
join integration1.collection_name1
```

**MongoToAst class**

Translates mongo query to AST-query

It can handle $where query that is represented as string and looks like:
```
'this.a ==1 and "te" >= latest'
```
'this' is a link to record

Parsing of that kind of string queries is performed using python ast parser

Limitations of MongoToAst:
- at the moment it can handle only 'find' method
- 'group by' is not implemented
- it can produce only single table select because 'find' is query for singular collection. 


### Testing

To run tests:

```
env PYTHONPATH=./ pytest tests/unit/test_mongodb_server.py
```
