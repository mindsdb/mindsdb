# MLFLow and MindsDB


## Simple example - Logistic Regression

MLFlow is a tool that you can use to train and serve models, among other features like organizing experiments, tracking metrics, etc.

Given there is no way to train an MLflow-wrapped model using its API, you will have to train your models outside of MindsDB by pulling your data manually (i.e. with a script), ideally using a MLflow run or experiment.

The first step would be to create a script where you train a model and save it using one of the saving methods that MLflow exposes. For this example, we will use the model in this [simple tutorial](https://github.com/mlflow/mlflow#saving-and-serving-models) where the method is `mlflow.sklearn.log_model` ([here](https://github.com/mlflow/mlflow/blob/9781af9c0898827bf616a8f159168477a69036dd/examples/sklearn_logistic_regression/train.py#L15)), given that the model is built with scikit-learn.

Once trained, you need to make sure the model is served and listening for input in a URL of your choice (note, this can mean your model can run on a different machine than the one executing MindsDB). Let's assume this URL to be `http://localhost:5000/invocations` for now.

This means you would execute the following command in your terminal, from the directory where the model was stored:

`mlflow models serve --model-uri runs:/<run-id>/model`

With `<run-id>` given in the output of the command `python train.py` used for actually training the model.

Next, we're going to bring this model into MindsDB:

```sql
CREATE PREDICTOR mindsdb.byom_mlflow 
PREDICT `1`  -- `1` is the target column name
USING 
    url.predict='http://localhost:5000/invocations', 
    format='mlflow', 
    data_dtype={"0": "integer", "1": "integer"};
```

We can now run predictions as usual, by using the `WHERE` statement or joining on a data table with an appropriate schema:

```sql
SELECT `1`
FROM byom_mlflow
WHERE `0`=2;
```

## Advanced example - Keras NLP model

Same use case as in section 1.2, be sure to download the dataset to reproduce the steps here. In this case, we will take a look at the best practices when your model needs custom data preprocessing code (which, realistically, will be fairly common).

The key difference is that we now need to use the `mlflow.pyfunc` module to both 1) save the model using `mlflow.pyfunc.save_model` and 2) subclass `mlflow.pyfunc.PythonModel` to wrap the model in an MLflow-compatible way that will enable our custom inference logic to be called.

### Saving the model

In the same script where you train the model (which you can find in the final section of 2.2) there should be a call at the end where you actually use mlflow to save every produced artifact:

```python
mlflow.pyfunc.save_model(
    path="nlp_kaggle",
    python_model=Model(),
    conda_env=conda_env,
    artifacts=artifacts
)
```

Here, `artifacts` will be a dictionary with all expected produced outputs when running the training phase. In this case, we want both a model and a tokenizer to preprocess the input text. On the other hand, `conda_env` specifies the environment under which your model should be executed once served in a self-contained conda environment, so it should include all required packages and dependencies. For this example, they look like this:

```python
# these will be accessible inside the Model() wrapper
artifacts = {
    'model': model_path,
    'tokenizer_path': tokenizer_path,
}

# specs for environment that will be created when serving the model
conda_env = {
    'name': 'nlp_keras_env',
    'channels': ['defaults'],
    'dependencies': [
        'python=3.8',
        'pip',
        {
            'pip': [
                'mlflow',
                'tensorflow',
                'cloudpickle',
                'nltk',
                'pandas',
                'numpy',
                'scikit-learn',
                'tqdm',
            ],
        },
    ],
}
```

Finally, to actually store the model you need to provide the wrapper class that will 1) load all produced artifacts into an accessible "context" and 2) implement all required inference logic:

```python
class Model(mlflow.pyfunc.PythonModel):
    def load_context(self, context):
        # we use paths in the context to load everything
        self.model_path = context.artifacts['model']
        self.model = load_model(self.model_path)
        with open(context.artifacts['tokenizer_path'], 'rb') as f:
            self.tokenizer = pickle.load(f)

    def predict(self, context, model_input):
        # preprocess input, tokenize, pad, and call the model
        df = preprocess_df(model_input)
        corpus = create_corpus(df)
        sequences = self.tokenizer.texts_to_sequences(corpus)
        tweet_pad = pad_sequences(sequences, 
                                  maxlen=MAX_LEN, 
                                  truncating='post', 
                                  padding='post')
        df = tweet_pad[:df.shape[0]]

        y_pre = self.model.predict(df)
        y_pre = np.round(y_pre).astype(int).flatten().tolist()

        return list(y_pre)
```

As you can see, here we are loading multiple artifacts and using them to guarantee the input data will be in the same format that was used when training. Ideally, you would abstract this even further into a single `preprocess` method that is called both at training time and inference time.

Finally, serving is simple. Go to the directory where you called the above script, and execute `mlflow models serve --model-uri ./nlp_kaggle`.

At this point, the rest is essentially the same as in the previous example. You can link the MLflow model with these SQL statements:

```sql
CREATE PREDICTOR mindsdb.byom_mlflow_nlp
PREDICT `target`
USING 
    url.predict='http://localhost:5000/invocations',
    format='mlflow',
    dtype_dict={"text": "rich text", "target": "binary"};
```

To get predictions, you can directly pass input data using the `WHERE` clause:

```sql
SELECT target
FROM mindsdb.byom_mlflow_nlp
WHERE text='The tsunami is coming, seek high ground';
```

Or you can `JOIN` with a data table. For this, you should ensure the table actually exists and that the database it belongs to has been connected to your MindsDB instance. For more details, refer to the same steps in the Ray Serve example (section 1.2).

```sql
SELECT
    ta.text,
    tb.target AS predicted
FROM db_byom.test.nlp_kaggle_test AS ta
JOIN mindsdb.byom_mlflow_nlp AS tb;
```

### Full Script

Finally, for reference, here's the full script that trains and saves the model. The model is exactly the same as in section 1.2, so it may seem familiar.

```python
import re
import pickle
import string

import mlflow.pyfunc

import nltk
import tqdm
import sklearn
import tensorflow
import cloudpickle
import numpy as np
import pandas as pd
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from sklearn.model_selection import train_test_split
from tensorflow.keras.initializers import Constant
from tensorflow.keras.layers import Embedding, LSTM, Dense, SpatialDropout1D
from tensorflow.keras.models import Sequential
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.preprocessing.sequence import pad_sequences
from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.models import load_model


stop = set(stopwords.words('english'))

MAX_LEN = 100
GLOVE_DIM = 50
EPOCHS = 10


def preprocess_df(df):
    df = df[['text']]
    funcs = [remove_URL, remove_html, remove_emoji, remove_punct]
    for fn in funcs:
        df['text'] = df['text'].apply(lambda x: fn(x))
    return df


def remove_URL(text):
    url = re.compile(r'https?://\S+|www\.\S+')
    return url.sub(r'', text)


def remove_html(text):
    html = re.compile(r'<.*?>')
    return html.sub(r'', text)


def remove_punct(text):
    table = str.maketrans('', '', string.punctuation)
    return text.translate(table)


def remove_emoji(text):
    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # emoticons
                               u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                               u"\U0001F680-\U0001F6FF"  # transport & map symbols
                               u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                               u"\U00002702-\U000027B0"
                               u"\U000024C2-\U0001F251"
                               "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', text)


def create_corpus(df):
    corpus = []
    for tweet in tqdm.tqdm(df['text']):
        words = [word.lower() for word in word_tokenize(tweet) if ((word.isalpha() == 1) & (word not in stop))]
        corpus.append(words)
    return corpus


class Model(mlflow.pyfunc.PythonModel):

    def load_context(self, context):

        self.model_path = context.artifacts['model']
        with open(context.artifacts['tokenizer_path'], 'rb') as f:
            self.tokenizer = pickle.load(f)
        self.model = load_model(self.model_path)

    def predict(self, context, model_input):

        df = preprocess_df(model_input)
        corpus = create_corpus(df)
        sequences = self.tokenizer.texts_to_sequences(corpus)
        tweet_pad = pad_sequences(sequences, maxlen=MAX_LEN, truncating='post', padding='post')
        df = tweet_pad[:df.shape[0]]

        y_pre = self.model.predict(df)
        y_pre = np.round(y_pre).astype(int).flatten().tolist()

        return list(y_pre)


if __name__ == '__main__':
    train_model = True

    model_path = './'
    tokenizer_path = './tokenizer.pkl'
    run_name = 'test_run'
    mlflow_pyfunc_model_path = "nlp_kaggle"
    mlflow.set_tracking_uri("sqlite:///mlflow.db")

    if train_model:

        # preprocess data
        df = pd.read_csv('./train.csv')
        target = df[['target']]
        target_arr = target.values
        df = preprocess_df(df)
        train_corpus = create_corpus(df)

        # load embeddings
        embedding_dict = {}
        with open('./glove.6B.50d.txt', 'r') as f:
            for line in f:
                values = line.split()
                word = values[0]
                vectors = np.asarray(values[1:], 'float32')
                embedding_dict[word] = vectors
        f.close()

        # generate and save tokenizer
        tokenizer_obj = Tokenizer()
        tokenizer_obj.fit_on_texts(train_corpus)

        with open(tokenizer_path, 'wb') as f:
            pickle.dump(tokenizer_obj, f)

        # tokenize and pad
        sequences = tokenizer_obj.texts_to_sequences(train_corpus)
        tweet_pad = pad_sequences(sequences, maxlen=MAX_LEN, truncating='post', padding='post')
        df = tweet_pad[:df.shape[0]]

        word_index = tokenizer_obj.word_index
        num_words = len(word_index) + 1
        embedding_matrix = np.zeros((num_words, GLOVE_DIM))

        # fill embedding matrix
        for word, i in tqdm.tqdm(word_index.items()):
            if i > num_words:
                continue

            emb_vec = embedding_dict.get(word)
            if emb_vec is not None:
                embedding_matrix[i] = emb_vec

        X_train, X_test, y_train, y_test = train_test_split(df, target_arr, test_size=0.15)

        # generate model
        model = Sequential()
        embedding = Embedding(num_words,
                              GLOVE_DIM,
                              embeddings_initializer=Constant(embedding_matrix),
                              input_length=MAX_LEN,
                              trainable=False)
        model.add(embedding)
        model.add(SpatialDropout1D(0.2))
        model.add(LSTM(64, dropout=0.2, recurrent_dropout=0.2))
        model.add(Dense(1, activation='sigmoid'))

        optimizer = Adam(learning_rate=1e-5)
        model.compile(loss='binary_crossentropy', optimizer=optimizer, metrics=['accuracy'])

        # train and save
        model.fit(X_train, y_train, batch_size=4, epochs=EPOCHS, validation_data=(X_test, y_test), verbose=2)
        model.save(model_path)

    # save in mlflow format
    artifacts = {
        'model': model_path,
        'tokenizer_path': tokenizer_path,
    }

    conda_env = {
        'channels': ['defaults'],
        'dependencies': [
            'python=3.8',
            'pip',
            {
                'pip': [
                    'mlflow',
                    'tensorflow=={}'.format(tensorflow.__version__),
                    'cloudpickle=={}'.format(cloudpickle.__version__),
                    'nltk=={}'.format(nltk.__version__),
                    'pandas=={}'.format(pd.__version__),
                    'numpy=={}'.format(np.__version__),
                    'scikit-learn=={}'.format(sklearn.__version__),
                    'tqdm=={}'.format(tqdm.__version__)
                ],
            },
        ],
        'name': 'nlp_keras_env'
    }

    # Save and register the MLflow Model
    with mlflow.start_run(run_name=run_name) as run:
        mlflow.pyfunc.save_model(
            path=mlflow_pyfunc_model_path,
            python_model=Model(),
            conda_env=conda_env,
            artifacts=artifacts)

        result = mlflow.register_model(
            f"runs:/{run.info.run_id}/{mlflow_pyfunc_model_path}",
            f"{mlflow_pyfunc_model_path}"
        )
```
