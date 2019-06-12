---
id: inside-mindsdb
title: Inside MindsdDB
---

Different transactions PREDICT, CREATE MODEL etc, require different steps/phases, however they may share some of these phases, in order to make this process modular we keep the variables in the Transaction controller (the data bus) as the communication interface, as such, the implementation of a given phase can change, so long as the expected variables in the bus prevail. (We will describe in more detail some of the Phase Modules in the next section)

## DataExtractor

It deals with extracting inputs from various data-sources such as files, directories and SQL compatible databases. If input is a query, it builds the joins with all implied tables (if any).

* **StatsLoader**: There is some transaction such as PREDICT where it's assumed that the statistical information is already known, all we have to do is make sure we load the right statistics to the transaction BUS.

At the moment we don't support loading database from {char}svs that don't have headers or have incomplete headers.

**NOTE**: *That as of now mindsDB requires that the full dataset can be loaded into memory, in the future we might look into supporting very large datasets using something like apache drill to query a FS or db for the chunks of data we need in order to train and generate our statistical analysis*.


## StatsGenerator

Once the data is pulled and aggregated from the various data sources, MindsDB runs an analysis of each of the columns of the corpus.

The purpose of the stats generator is  two fold:

* To provide various data quality scores in order to determine the overall quality of a column (e.g. variance, some correlation metrics between columns, amount of duplicates).

* To provide properties about the columns which have to be used in the following steps and in order to rain the model. (e.g. histogram, data type)

	After all stats are computed, we warn the user of any interesting insights we found about his data and (if web logs are enabled), use the
generated values to plot some interesting information about the data (e.g. data type distribution, outliers, histogram).

![](https://docs.google.com/drawings/d/e/2PACX-1vTAJo6Zll3jRg-QpZTu2RkXOL0TQXl5dgBHOZqpD3jsW4frhlWxIqc0Mv1OnKbOXNc1cYMFYXMlJ96U/pub?w=502&h=252)

Finally, the various stats are passed on as part of the metadata, so that further phases and the model itself can use them.


## Model Interface

* **Train mode**: When calling `learn`,the model interface will feed the data to a machine learning framework which does the training in order to build a model.

* **Preidct mode**: When calling `predict`, the model interface will feed the data to the model built by `learn` in order to generate a prediction.

* **Data adaption**: The `ModelInterface` phase is simply a lighteight wrapper over the model [backends](https://github.com/mindsdb/mindsdb/tree/master/mindsdb/libs/backends) which handle adapting the data frame used by mindsdb into a format they can work with. During this process additional metadata for the machine learning libraries/frameworks is generated based on the results of the **Stats Generator** phase.

* **Learning backend**: The learning backends as the [ensemble learning](https://en.wikipedia.org/wiki/Ensemble_learning) libraries used by mindsdb to train the model that will generate the predictions.
Currently the two learning backends we are working on supporting are Ludwig (maintained mainly by Uber, fully supported) and Lightwood (created by us, based on the pre 1.0 version of mindsdb, work in progress).

## ModelAnalyzer

The model analyzer phase runs after training is done in order to gather insights about the model and gather insights about the data
that we can only get post-training.

At the moment, it contains the fitting for a  probabilistic model which is used to determine the accuracy of future prediction, based on the number of missing features and the bucket in which the predicted value falls.
