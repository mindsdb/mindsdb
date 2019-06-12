---
id: google-colab
title: Google Colab
---

# Using MindsDB with Google Colab

[Google Colab](https://colab.research.google.com) is a free cloud service that supports free GPU!
You can use MindsDB there.

Fortunately, this is really easy.
Inside Google Colab, start a new python 3 notebook and in a cell, insert the following:
```
!pip install mindsdb
```

## Let's Build an Example

First we'll import mindsdb
```python
from mindsdb import Predictor
```
This is where it gets interesting. It's now up to you to install any dataset you want, so long as its a Excel or CSV file (or some other from of separator, doesn't necessarily have to be a ","). We'll be linking it to colab next.
In this example we'll be using a students dataset from kaggle. You can get it [here](https://www.kaggle.com/spscientist/students-performance-in-exams) if you want to follow along.

Once you have your CSV dataset, download it and put it in a new folder on your Google Drive. We'll call ours `Datasets`.
We'll import it into colab using the following lines
```python
from google.colab import drive
drive.mount('/content/drive')
```
Now just follow the instructions and enter your authorization code.

Here, we'll create a file variable that stores the path of our dataset.

```
file = "./drive/My Drive/Datasets/StudentsPerformance.csv"
```


## Training

Now let's create a MindsDB object and initialize it with our data from the file. We'll be prediciting the reading_score and we'll call our model 'reading_predictor'.
Remember that depending on your dataset, these variables might change. Just remember that `predict` is the column you want to make your prediction on and that mindsdb will automatically rename all your columns to snake case.
```python
mdb = Predictor(name='reading_score_predictor')

mdb.learn(print(predictions)
  from_data=file, # call file from google drive
  to_predict='reading_score'
)
```

## Testing

`mdb.predict` needs 1 of 2 arguments to run a prediction:
* `when` is a dictionary of values for the columns we want to use for the prediction (i.e. we want to predict the reading score of a student who got a writing score of 80, a math score of 40, and has a standard lunch)
OR
* `when_data` is a file with one or more values for the columns we want to use for the prediction

The following example uses a dictionary via the `when` argument:


```python
# Load the `Predictor` we just trained via calling `learn`
mdb = Predictor(name='reading_score_predictor')

# Make a prediction using a dictionary of input values
predictions = mdb.predict(
  when={
      'writing_score' : 80,
      'math_score' : 40,
      'lunch' : 'standard'
  }
)
```

Finally we print out the result:

```python
# The dictionary containing the prediction
print(predictions)
# The confidence we have in the prediction (`0` being the lowest confidence and `1` being 100% confident)
# Note, the confidence is not equal to the model's overall accuracy
print(predictions['reading_score_confidence'])
# The actual value predicted for `reading_score`
print(predictions['reading_score'])
```

You can find our GoogleColab example here: https://colab.research.google.com/drive/1qsIkMeAQFE-MOEANd1c6KMyT44OnycSb
