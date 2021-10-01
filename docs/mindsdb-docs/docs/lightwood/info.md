

Lightwood is a Pytorch based framework with two objectives:

- Make it so simple that you can build predictive models with a line of code.
- Make it so flexible that you can change and customize everything.

Lightwood was inspired on [Keras](https://keras.io/)+[Ludwig](https://github.com/uber/ludwig) but runs on Pytorch and gives you full control of what you can do.

## Prerequisites

Python >=3.6 64bit version

## Installing Lightwood

You can install Lightwood using `pip`:
```bash
pip3 install lightwood
```

If this fails, please report the bug on github and try installing the current master branch:

```bash
git clone git@github.com:mindsdb/lightwood.git;
cd lightwood;
pip install --no-cache-dir -e .
```

>Please note that, depending on your os and python setup, you might want to use `pip` instead of `pip3`.

You need python 3.6 or higher.

Note on MacOS, you need to install libomp:

```bash
brew install libomp
```

### Install using virtual environment

We suggest you to install Lightwood on a virtual environment to avoid dependency issues. Make sure your Python version is >=3.6. To set up a virtual environment:

#### Install on Windows

Install the latest version of `pip`:

```bash
python -m pip install --upgrade pip
pip --version
```

Activate your virtual environment and install lightwood:

```bash
py -m pip install --user virtualenv
.\env\Scripts\activate
pip install lightwood
```
>You can also use `python` instead of `py`

#### Install on Linux or macOS 

Before installing Lightwood in a virtual environment you need to first create and activate the `venv`:

```bash
python -m venv env
source env/bin/activate
pip install lightwood
```


## Quick example

Assume that you have a training file (sensor_data.csv) such as this one.

| sensor1  | sensor2 | sensor3 |
|----|----|----|
|  1 | -1 | -1 |
| 0  | 1  | 0  |
| -1 | -1 | 1  |
| 1  | 0  | 0  |
| 0  | 1  | 0  |
| -1 | 1  | -1 |
| 0  | 0  | 0  |
| -1 | -1 | 1  |
| 1  | 0  | 0  |

And you would like to learn to predict the values of *sensor3* given the readings in *sensor1* and *sensor2*.

### Learn

You can train a Predictor as follows:

```python
from lightwood import Predictor
import pandas

sensor3_predictor = Predictor(output=['sensor3'])
sensor3_predictor.learn(from_data=pandas.read_csv('sensor_data.csv'))

```

### Predict

You can now be given new readings from *sensor1* and *sensor2* predict what *sensor3* will be.

```python

prediction = sensor3_predictor.predict(when={'sensor1':1, 'sensor2':-1})
print(prediction)
```

Of course, that example was just the tip of the iceberg, please read below about the main concepts of lightwood, the API and then jump into examples.

# Lightwood Predictor API


```python
from lightwood import Predictor

```

Lightwood has one main class; The **Predictor**, which is a modular construct that you can train and get predictions from. It is made out of 3 main building blocks (*features, encoders, mixers*) that you can configure, modify and expand as you wish.

![building_blocks](https://docs.google.com/drawings/d/e/2PACX-1vTrzcXyqDeaGWOwG-3BWOV5wj1U2M5v7ojracqv39z2Ljv-oFqxh4bxFiJxjjtd7CgehptMeBlLYx6w/pub?w=1399&h=818&a=1)

!!! info "Building blocks"
    
    * **Features**:
        * **input_features**: These are the columns in your dataset that you want to take as input for your predictor.
        * **output_features**: These are the columns in your dataset that you want to learn how to predict.
    * **Encoders**: These are tools to turn the data in your input or output features into vector/tensor representations and vice-versa.
    * **Mixers**: How you mix the output of encoded features and also other mixers


## Constructor, \__init__()

```python

my_predictor = Predictor( output=[] | config={...} | load_from_path=<file_path>)

```

!!! note ""
    Predictor, can take any of the following **arguments**
    
    * **load_from_path**: If you have a saved predictor that you want to load, just give the path to the file
    * **output**: A list with the column names you want to predict. (*Note: If you pass this argument, lightwood will simply try to guess the best config possible*)
    * **config**: A dictionary, containing the configuration on how to glue all the building blocks. 

### **config**

The config argument allows you to pass a dictionary that defines and gives you absolute control over how to build your predictive model.
A config example goes as follows:
```python
from lightwood import COLUMN_DATA_TYPES, BUILTIN_MIXERS, BUILTIN_ENCODERS

config = {

        ## REQUIRED:
        'input_features': [
        
            # by default each feature has an encoder, so all you have to do is specify the data type
            {
                'name': 'sensor1',
                'type': COLUMN_DATA_TYPES.NUMERIC
            },
            {
                'name': 'sensor2',
                'type': COLUMN_DATA_TYPES.NUMERIC
            },
            
            # some encoders have attributes that can be specified on the configuration
            # in this particular lets assume we have a photo of the product, we would like to encode this image and optimize for speed
            {
                'name': 'product_photo',
                'type': COLUMN_DATA_TYPES.IMAGE,
                'encoder_class': BUILTIN_ENCODERS.Image.Img2VecEncoder, # note that this is just a class, you can build your own if you wish
                'encoder_attrs': {
                    'aim': 'speed' 
                    # you can check the encoder attributes here: 
                    #  https://github.com/mindsdb/lightwood/blob/master/lightwood/encoders/image/img_2_vec.py
                }
            }
        ],

        'output_features': [
            {
                'name': 'action_to_take',
                'type': COLUMN_DATA_TYPES.CATEGORICAL
            }
        ],
        
        ## OPTIONAL
        'mixer': {
            'class': BUILTIN_MIXERS.NnMixer
        }
        
    }
```






#### features

Both **input_features** and **output_features** configs are simple dicts that have the following schema

```python
{
    'name': str,
    Optional('type'): any of COLUMN_DATA_TYPES,
    Optional('encoder_class'): object,
    Optional('encoder_attrs'): dict
}
```
!!! note ""
    * **name**: is the name of the column as it is in the input data frame
    * **type**: is the type of data contained. Where out of the box, supported COLUMN_DATA_TYPES are ```NUMERIC, CATEGORICAL, DATETIME, IMAGE, TEXT, TIME_SERIES```:


    !!! info "If you specify the type, lightwood will use the default encoder for that type, however, you can specify/define any encoder that you want to use. "    
        
        
        * **encoder_class**: This is if you want to replace the default encoder with a different one, so you put the encoder class there
        * **encoder_attrs**: These are the attributes that you want to setup on the encoder once the class its initialized 
        

#### mixer

The **default_mixer** key, provides information as to what mixer to use. The schema for this variable is as follows:

```python
mixer_schema = Schema({
    'class': object,
    Optional('attrs'): dict
})
```

!!! note ""
    * **class**: It's the actual class, that defines the Mixer, you can use any of the BUILTIN_MIXERS or pass your own.
    * **attrs**: This is a dictionary containing the attributes you want to replace on the mixer object once its initialized. We do this, so you have maximum flexibility as to what you can customize on your Mixers.

## learn()

```python
my_predictor.learn(from_data=pandas_dataframe)
```
!!! note ""
    This method is used to make the predictor learn from some data, thus the learn method takes the following arguments.
    
    * **from_data**: A pandas dataframe, that has some or all the columns in the config. The reason why we decide to only support pandas dataframes, its because, its easy to load any data to a pandas dataframe, and spark for python dataframe is a format we support.
    * **test_data**: (Optional) This is if you want to specify what data to test with, if no test_data passed, lightwood will break the from_data into test and train automatically.
    * **callback_on_iter**: (Optional) This is function callback that is called every 100 epochs during the learn process.


## predict()

```python
my_predictor.predict(when={..} | when_data=pandas_dataframe)
```
!!! note ""
    This method is used to make predictions and it can take one of the following arguments
    
    * **when**: this is a dictionary of conditions to predict under.
    * **when_data**: Sometimes you want to predict more than one row at a time, so here it is: a pandas dataframe containing the conditional values you want to use to make a prediction.


## save()

```python
my_predictor.save(path_to=string to path)
```
!!! note ""
    Use this method to save the predictor into a desired path

## calculate_accuracy()

```python
print(my_predictor.calculate_accuracy(from_data=data_source))

```
!!! note ""
    Returns the predictors overall accuracy. 
