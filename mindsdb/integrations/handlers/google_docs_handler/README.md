# Google Docs API Integration
This handler integrates with the Google Docs API to make the docs content available for the use case of NLP model training and predictions.

# Connect to the Google Docs API 
First we need to create a database to connect to the Google Docs API.

However, you will need to have a Google account and have enabled the Google Docs API. Also, you will need to have a doc id from your google docs and the credentials for that google doc in a json file.

This creates a database called my_docs. This database connects to the google docs service to reterive the google doc contents and name of the google doc as of now and the list of rest api endpoints supported by google docs is [here](https://developers.google.com/docs/api/reference/rest)

~~~sql
CREATE DATABASE my_docs
WITH  ENGINE = 'google_docs',
parameters = {
    'credentials': '/Users/bseetharaman/Desktop/FY23/MindsDB/Google-Docs/mindsdb/mindsdb/integrations/handlers/google_docs_handler/credentials.json'
};  
~~~

# Implemented Features
[x]Google Docs - get_doc_details table
  [x] Support LIMIT
  [x]Support WHERE
  [x]Support ORDER BY
  [x]Support column selection

# Select Data
~~~~sql
SELECT * FROM my_docs.get_doc_details
WHERE google_doc_id = "1ip1WRzegUGx-zrZNRMFHtemXZxm2zH5WkItLKOVV9og";
~~~~

# Create Model
~~~~sql
CREATE MODEL openai_test
PREDICT summary
USING
    engine = 'openai',
    prompt_template = 'Summarize the following text within 100 words in full sentences - {{text}}',
    max_tokens = 100,
    temperature = 0.3,
    api_key = 'your openai key';
~~~~

# Run Predictions
~~~~sql
SELECT summary
FROM openai_test
WHERE text = (SELECT doc_content FROM my_docs.get_doc_details WHERE google_doc_id = "1ip1WRzegUGx-zrZNRMFHtemXZxm2zH5WkItLKOVV9og");
~~~~















