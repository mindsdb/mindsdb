# Google Drive API Integration
This handler integrates with the Google Drive API to make the drive content available for the use case of NLP model training and predictions.

# Connect to the Google Drive API 
First we need to create a database to connect to the Google Drive API.

However, you will need to have a Google account and have enabled the Google Drive API. 

This creates a database called my_docs. This database connects to the google docs service to reterive the google doc contents and name of the google doc as of now and the list of rest api endpoints supported by google docs is [here](https://developers.google.com/docs/api/reference/rest)

~~~sql
CREATE DATABASE my_drive
WITH  ENGINE = 'google_drive',
parameters = {
    'credentials': '/Users/bseetharaman/Desktop/FY23/MindsDB/Google-Docs/mindsdb/mindsdb/integrations/handlers/google_drive_handler/credentials.json'
};  
~~~

# Implemented Features
- [x] Google Drive - list_pdf_files table
  - [x] Support LIMIT
  - [x] Support WHERE
  - [x] Support ORDER BY
  - [x] Support column selection
- [x] Google Drive - download_pdf table
  - [x] Support SELECT

# Possible Feature Additions
TBD

# List the pdf files in the drive using SELECT statement
~~~~sql
SELECT * FROM my_drive.list_pdf_files;
~~~~

# Download the pdf file using file id in the drive using SELECT statement
~~~~sql
SELECT * FROM my_drive.download_pdf_file
WHERE google_drive_file_id = "1jdRQi2zrSo0j6TpAdJehs0Cs4QJLN5qN";
~~~~


