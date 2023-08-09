## Manual QA with Monkeylearn Framework on Cloud

This file contains Manual QA tests with Monkey Learn using their E-commerce Support Ticket Classifier which classifies text according to a tag, eg. Missing Item, Product Availability etc.

### Creating a model without a dataset
**Create ML Engine:**

`Create ML_ENGINE monkeylearn
FROM monkeylearn`

![1 create_ml](https://github.com/mindsdb/mindsdb/assets/32901682/f5d8d2f6-5267-4c6b-818a-50aa6b157647)


**Create model:**

`CREATE MODEL mindsdb.ecommerce_ticket_classifier
PREDICT tag
USING
engine = 'monkeylearn',
api_key = 'your_api_key',
model_id = 'monkeylearn_model_id',
input_column = 'text';`

![createmodel1](https://github.com/mindsdb/mindsdb/assets/32901682/40bc9607-3a13-442d-ae23-159fca7bae4c)


**Select model:**

`SELECT * FROM mindsdb.models WHERE name = 'ecommerce_ticket_classifier';`
![3 select_predictor](https://github.com/mindsdb/mindsdb/assets/32901682/409e6fcd-a9e7-44e5-ac6e-0ed1c9e09648)


**Describe model:**

`DESCRIBE ecommerce_ticket_classifier;`
![4 describe](https://github.com/mindsdb/mindsdb/assets/32901682/4c9b110d-3b98-4db4-86bd-5514267fc622)


**Select a prediction:**

`SELECT * FROM ecommerce_ticket_classifier
WHERE text = 'Where is my order? The delivery status shows shipped. When I call the delivery driver there is no response!';`
![5 select_prediction](https://github.com/mindsdb/mindsdb/assets/32901682/f8b8d4fc-ed95-4c87-8909-3e2d54e1c49a)


### Create a model with a dataset

Using dataset:[test1 - e-commerce.csv](https://github.com/mindsdb/mindsdb/files/12292862/test1.-.e-commerce.csv)

File is uploaded directly into the GUI.

**Create model:**

`CREATE MODEL mindsdb.ecommerce_ticket_classifier2
FROM files (select * from queries)
PREDICT tag
USING
engine = 'monkeylearn',
api_key = 'your_api_key',
model_id = 'monkeylearn_model_id',
input_column = 'text';`

![createmodel2](https://github.com/mindsdb/mindsdb/assets/32901682/1c9a849b-f8d8-4ac1-9954-ed977d08cf2f)



**Select a prediction:**

`SELECT * FROM ecommerce_ticket_classifier2
WHERE text = 'I would like to speak to a sales representative';`
![7 select_predict](https://github.com/mindsdb/mindsdb/assets/32901682/160f36d9-d28c-46a0-aa02-585c15d59833)


**Select a Batch Prediction:**

`SELECT a.text,b.tag
FROM mindsdb.ecommerce_ticket_classifier2 as b
JOIN files.queries as a;`
![8 select_batch1](https://github.com/mindsdb/mindsdb/assets/32901682/e0f4cfd3-5f79-4b6f-98d8-71f0838d99c9)


The batch prediction produces the same values for each result. The value produced for the first row is diffeent from the value that was provided for a single prediction.

To ensure that the dataset is not the issue, the text values have been changed  to make it easier to predict the correct tag for the text. 

The above model is dropped to create a new one with the same name.

Using Dataset:[Untitled spreadsheet - e-commerce.csv](https://github.com/mindsdb/mindsdb/files/12292892/Untitled.spreadsheet.-.e-commerce.csv)

The file is uploaded directly into the GUI.

**Create model:**

`CREATE MODEL mindsdb.ecommerce_ticket_classifier2
FROM files (select * from queries2)
PREDICT tag
USING
api_key = 'your_api_key',
model_id = 'monkeylearn_model_id',
input_column = 'text';`

![model3](https://github.com/mindsdb/mindsdb/assets/32901682/0a02cd2a-596a-43d7-abd5-6d517c656781)



**Select prediction:**

`SELECT * FROM ecommerce_ticket_classifier2
WHERE text = 'I ordered 4 units but only received 3';`
![10 select_prediction](https://github.com/mindsdb/mindsdb/assets/32901682/7189ece9-09c5-4f73-95cd-a5f5f34d65fb)


**Select Batch Prediction:**

`Select batch prediction:
SELECT a.text,b.tag FROM mindsdb.ecommerce_ticket_classifier2 as b JOIN files.queries2 as a;`
![11 select_batch_prediction](https://github.com/mindsdb/mindsdb/assets/32901682/d91c4835-2163-459e-9d97-936fb19d130a)


The model is giving the same values for all the rows. 

Test to see if a single prediction will produce a different result from the batch prediction:

**Select a prediction:**

`SELECT * FROM ecommerce_ticket_classifier2 WHERE text = 'When can I expect my order?';`
![12 select_predict](https://github.com/mindsdb/mindsdb/assets/32901682/5d184826-319e-4e4c-827b-163c01c6f5e3)

Model provides a different result than in the batch prediction for the text.

## Results

The model is successfully created and single predictions provide correct results. Unable to do batch predictions as it provides the same values for all the input rows which is incorect.


Drop a remark based on your observation.
- [ ] Works Great 💚 (This means that all the steps were executed successfully and the expected outputs were returned.)
- [X] There's a Bug 🪲 [Monkeylearn batch predictions give the same value #7033](https://github.com/mindsdb/mindsdb/issues/7033) 



