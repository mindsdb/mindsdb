[<Back to Table of Contents](../README.md)
# Using MindsDB with Google Colab

[Google Colab](https://colab.research.google.com) is a free cloud service that supports free GPU! You can use MindsDB there, here its how:

Fortunately, this is really easy.
Inside Google Colab, start a new python 3 notebook and in a cell, insert the following
```
!pip install mindsdb
```

## Let's Build an Example

First we'll import mindsdb
```Python
from mindsdb import *
```
This is where it gets interesting. It's now up to you to install any dataset you want, so long as its a CSV file. We'll be linking it to colab next.
In this example we'll be using a students dataset from kaggle. You can get it [here](https://www.kaggle.com/spscientist/students-performance-in-exams) if you want to follow along.

Once you have your CSV dataset, download it and put it in a new folder on your Google Drive. We'll call ours `Dataset`.
We'll import it into colab using the following lines
```Python
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
Remember that depending on your dataset, these variables might change. Just remember that `predict` is the column you want to make your prediction on and that mindsdb will automatically rename all your collums to snake case.
```Python
mdb = MindsDB()

mdb.learn(
  from_data=file, # call file from google drive
  predict='reading_score',
  model_name='reading_predictor'
)
```

## Testing

`mdb.predict` takes 3 parameters
`predict` is the same column as in `mdb.learn`
`when` is the parameters (i.e. we want to predict the reading score of a student who got a writing score of 80, a math score of 40, and has a standard lunch)
`model_name` is the same as in `mdb.learn`

```Python
result = mdb.predict(
  predict='reading_score',
  when={
      'writing_score' : 80,
      'math_score' : 40,
      'lunch' : 'standard'
  },
  model_name='reading_predictor'
)
```

Finally we print out the result
```Python
print(
    'The predicted reading score is {score} with {conf} confidence'
      .format(score=result.predicted_values[0]['reading_score'], 
       conf=result.predicted_values[0]['prediction_confidence'])
)
```
