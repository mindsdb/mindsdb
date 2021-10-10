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
import mindsdb_native

mdb = mindsdb_native.Predictor(name='multilabel_real_estate_model')
mdb.learn(
    from_data="https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv",
    to_predict=['location','neighborhood'] # Array with the names of the columns we want to predict
)
```


## Multimedia inputs (images, audio and video)

[![Watch the video](https://img.youtube.com/vi/2S-x6KfgJwE/maxresdefault.jpg)](https://youtu.be/2S-x6KfgJwE)
<a href="https://youtu.be/2S-x6KfgJwE"><h1>Watch Video</h1></a>

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
