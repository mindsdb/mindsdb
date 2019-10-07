---
id: advanced-mindsdb
title: Examples of Advanced Usecases
---

## Multiple column predictions

### What is a multiple column prediction ?
In some cases, you might want to predict more than one column of your data.

In order for mindsdb to predict multiple columns, you simply need to change the `to_predict` argument from a string (denoting the name of the column) to an array containing the names of the columns you want to predict.

In the following example, we've altered the real estate model to predict the `location` and `neighborhood` both, instead of the `rental_price`.

### Code example
```python
import mindsdb

mdb = mindsdb.Predictor(name='multilabel_real_estate_model')
mdb.learn(
    from_data="https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv",
    to_predict=['location','neighborhood'] # Array with the names of the columns we want to predict
)
```


## Multimedia inputs (images, audio and video)

Currently, we only support images as inputs into models. We are working on support audio, you can check [this issue](https://github.com/mindsdb/mindsdb/issues/124) to track the progress. Video input support is not yet planned.

For any sort of media files, simply provide the full path to the file in the column. For example:

```python
[
  {'img':'/mnt/data/img_1.jpg'
    'is_bicycle': True}
  ,{'img':'/mnt/data/img_2.jpg'
    'is_bicycle': True}
  ,{'img':'/mnt/data/img_3.jpg'
    'is_bicycle': False}
]
```

Please provide the full path to a file on your local machine, not a url or the binary data from an image loaded up in a dataframe.

Currently, the timeline on supporting multimedia output is still undecided, if you need that feature or want to implement, feel free to contact us. That being said, image outputs might actually work, we just haven't tested anything yet.


## Timeseries predictions

### What is a timeseries ?
A timeseries is a document where rows are related to each other in a sequential way, such that the prediction of the value in the present row should take into account a number of previous rows.

To build a timeseries model you need to call `learn` with timeseries specific arguments:

### Mandatory arguments
* order_by  -- The column on which to order the rows being feed into the model (e.g. a timestamp)
* window_size -- The number of rows to "look back" into when making a prediction, after the rows are ordered by the order_by column and split into groups. This is an alias for `window_size_samples`, they are interchangeable. You can use `window_size_seconds` instead if you want to use a number of seconds passing on the order by column, rather than a fixed number (please note, this approach assumes the order by column is or can be converted internally into a unix timestamp)

### Optional arguments
* group_by -- The column based on which to group multiple unrelated entities present in your timeseries data. If your data is coming from multiple sources, this argument can be a differentiator between those sources (e.g a uuid for the source).

### Code example
```python
import mindsdb

mdb = mindsdb.Predictor(name='assembly_machines_model')
mdb.learn(
    from_data='assembly_machines_historical_data.tsv',
    to_predict='chance_of_failure'
    # timeseries specific args
    ,order_by='timestamp' # The time of this reading
    ,window_size_seconds= 12000 # Use the last 1200 seconds of data to train for the prediction of a row, If you want a fixed nr of rows, use window_size_samples instead
    ,group_by = 'machine_id' # Train the model as a timeseries for each individual machine
)

results = mdb.predict(when_data='assembly_machines_data.tsv')
```

### Other approaches to timeseries data

Another way of approaching a timeseries prediction, if you don't want to use mindsdb's interface, is to simply include array's for the non-group-by values in your rows.

For example, to train, you could use the following csv:

```python
X vlas,previous Y vlas, Y now
[1 2 3 4],[1 4 9],16
[2,3,4,5],[4,9,16],25
[3,4,5,6],[9,16,25],36
```

And your model would be fit to predict the `Y now` column for this csv:

```python
X vlas,previous Y vlas
[7,8,9,10][49,64,81]
[33,34,35,36][1089,1156,1225]
```

## Unbalanced dataset

Given a dataset that is "imbalanced", the model being trained might not give the results you expect. For example, let's say you have the following dataset:

```python
data.csv
x,  is_power_of_9
1,  False
2,  False
3,  False
4,  False
5,  False
6,  False
7,  False
8,  False
9,  True
10, False
11, False
.
.
.
81, True
82, False
83, False
84, False
.
.
.
100, False
```

On which we want to predicted the aptly-named column `is_power_of_9`.

We have 2 occurrences of the output `True` and 98 occurrences of the `False` output. So a model could always predict `False` and it would have an accuracy of 98%, which is pretty decent, so unless there's a way for the model to learn those 2 instance of `True` without losing on accuracy, it might just decided `98%` is the best possible model.

However, let's say we really care about those `True` predictions being correct, or at least we want the model to consider them equally important, in that case we would call the `learn` function using the `equal_accuracy_for_all_output_categories` argument set to true.

This essentially means that a model with 50% accuracy, with 2 correct predictions for `True` (100%) and 48 for `False` (49%) is considered better than a model with 98% accuracy that only predictions `False` when mindsdb trains the model.

We could call this as: `predictior.learn(from_data='data.csv', equal_accuracy_for_all_output_categories=True)`
