---
id: functional-interface
title: Functional interface
---

This section goes into detail about each of the methods exposed by Functional and each of the arguments they work with. All of the Functional methods act as utilities for Predictors.


### Get Models

`F.get_models()`

Takes no argument, returns a list with all the models and some information about them.

**Note: this is akin to a static method, it acts the same way no matter what predictor object you call it on, but due to various consideration it hasn't been swtiched to a static method yet**

### Get Model Data

`F.get_model_data(model_name='model_name')`

Returns all the data we have about a given model. This is a rather complex python dictionary meant to be interpreted by the Scout GUI. We recommend looking at the training logs mindsdb_native gives to see some of these insights in an easy to read format.

* model_name -- Required argument, the name of the model to return data about.

**Note: this is akin to a static method, it acts the same way no matter what predictor object you call it on, but due to various consideration it hasn't been swtiched to a static method yet**

### Export Model

`F.export_predictor(model_name='model_name')`

Exports this predictor's data (or the data of another predictor) to a zip file inside the CONFIG.MINDSDB_STORAGE_PATH directory.

* model_name -- The name of the model to export (defaults to the name of the current Predictor).

### Rename Model

`F.rename_model(old_model_name='old_name', new_model_name='new_name')`

Renames the created model.

* old_model_name: the name of the model you want to rename
* new_model_name: the new name of the model

### Export Storage

`F.export_storage(mindsdb_storage_dir='mindsdb_storage')`

Exports mindsdb's storage directory to a zip file.

* mindsdb_storage_dir -- The location where you want to save the mindsdb storage directory.

### Load Model
`F.import_model(model_archive_path='path/to/predictor.zip')`

Loads a predictor that was previously exported into the current mindsdb_native storage path so you can use it later.

* model_archive_path -- full_path that contains your mindsdb_native predictor zip file.


### Delete Model

`F.delete_model(model_name='blah')`

Deletes a given predictor from the storage path mindsdb_native is currently operating with.

* model_name -- The name of the model to delete (defaults to the name of the current Predictor).

### Analyse dataset

`F.analyse_dataset(from_data=the_data_source, sample_settings={})`

Analyse the dataset inside the data source, file, ulr or pandas dataframe. This runs all the steps prior to actually training a predictive model.

* from_data -- the data that you want to analyse, this can be either a file, a pandas data frame, a url or a mindsdb data source.
* sample_settings --  A dictionary of options for sampling from the dataset. Includes `sample_for_analysis`. `sample_for_training`, `sample_margin_of_error`, `sample_confidence_level`, `sample_percentage`, `sample_function`.
