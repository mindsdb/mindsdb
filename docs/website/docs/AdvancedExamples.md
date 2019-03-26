# Examples of advanced usecases

## Multiple column predictions

In order for mindsdb to predict multiple column, you simple need to change the `to_predict` argument from a string (denoting the name of the column) to an array containing the names of the columns you want to predict.

In the following example we've altered the real estate model to predict the `location` and `neighborhood` both, instead of the `rental_price`.

```python
import mindsdb

mdb = mindsdb.Predictor(name='multilabel_real_estate_model')
mdb.learn(
    from_data="https://raw.githubusercontent.com/mindsdb/mindsdb/master/docs/examples/basic/home_rentals.csv",
    to_predict=['location','neighborhood'] # Array with the names of the columns we can to predict
)
```


## Multimedia inputs (images, audio and video)

Currently we only support images as inputs into models. We are working on support audio, you can check [this issue](https://github.com/mindsdb/mindsdb/issues/124) to track the progress. Video input support is not yet planned.

For any sort of media files, simple provide the full path to the file in the column. For example:

```python
[
  {'img':'/mnt/data/img_1.jpg'
    'contains_pandas': True}
  ,{'img':'/mnt/data/img_2.jpg'
    'contains_pandas': True}
  ,{'img':'/mnt/data/img_3.jpg'
    'contains_pandas': False}
]
```

Please provide the full path to a file on your local machine, not a url or the binary data from an image loaded up in a dataframe.


## Timeseries predictions
