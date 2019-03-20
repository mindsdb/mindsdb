[<Back to Table of Contents](../README.md)
## Inside MindsDB

![](https://docs.google.com/drawings/d/e/2PACX-1vQPGU3nzH0dwpgjzZ-bb95nJRhYUDYFuTuzIUERoVBGMMZW1ocUA1LAyDCldNKKp5RCw3Wxac21qPP7/pub?w=960&h=252)

Different transactions PREDICT, CREATE MODEL etc, require different
steps/phases, however they may share some of these phases,
in order to make this process modular we keep the variables in the Transaction
controller (the data bus) as the communication interface, as such,
the implementation of a given phase can change, so long as the expected
variables in the bus prevail. (We will describe in more detail some of
the Phase Modules in the next section)

### DataExtractor

It deals with extracting inputs from various data-sources such as files, directories and SQL compatible databases. If input is a query, it builds the joins with all implied tables (if any).

* **StatsLoader**: There are some transaction such as PREDICT where it's assumed that the statistical information is already known, all we have to do is make sure we load the right statistics to the transaction BUS.

At the moment we don't support loading database from {char}svs that don't have headers or have incomplete headers.

**NOTE**: *That as of now mindsDB requires that the full dataset can be loaded into memory, in the future we might look into supporting very large datasets using something like apache drill to query a FS or db for the chunks of data we need in order to train and generate our statistical analysis*.


### StatsGenerator

Once the data is pulled and aggregated from the various data sources, MindsDB runs an analysis of each of the columns of the corpus.

The purpose of the stats generator is  two fold:

* To provide various data quality scores in order to determine the overall quality of a column (e.g. variance, some correlation metrics between columns, amount of duplicates).

* To provide properties about the columns which have to be used in the following steps and in order to rain the model. (e.g. histogram, data type)

	After all stats are computed, we warn the user of any interesting insights we found about his data and (if web logs are enabled), use the
generated values to plot some interesting information about the data (e.g. data type distribution, outliers, histogram).

![](https://docs.google.com/drawings/d/e/2PACX-1vTAJo6Zll3jRg-QpZTu2RkXOL0TQXl5dgBHOZqpD3jsW4frhlWxIqc0Mv1OnKbOXNc1cYMFYXMlJ96U/pub?w=502&h=252)

Finally, the various stats are passed on as part of the metadata, so that further phases and the model itself can use them.


### StatsLoader

There are some transaction such as PREDICT for which the statistical information should be already known from a previous TRAIN. This phase loads the right stats in the transaction metadata.

### DataVectorizer

In this phase the idea is to translate each column into *numpy* tensors that can be ingested by data models. Tensors are made of vector representations of each cell. This involves to understand what transformations are necessary depending on the column type. Currently the following column data types are supported **Note that these can be expanded or updated for various needs**:

* **Categorical**:

	![](https://docs.google.com/drawings/d/e/2PACX-1vR7PCdT5QCCuQ8pG6pSRc8RfdmkCPnVVrOZNPAA9QTvqluf8e2EQRdSDXutlXho2ymz_OP3LGxo-GxE/pub?w=359)

	* **text-tokens**: columns where the values are *TEXT* but the distribution of the text is made of words or combinations of words and the number of uniques does not exceed 10% of the total number of rows.

	* **numeric-tokens**: columns where the values are *NUMERIC* but the number of uniques does not exceed 10% of the total number of rows.

* **Continuous**:

	* **numeric**: These are *NUMERIC* values that don't match the criteria of *numeric-tokens* or *date-time*.

		![](https://docs.google.com/drawings/d/e/2PACX-1vQt9FeMEgBMIoEF23NqQcF3D28Vnk-D2z0pXIIjHy1LCt4l9NdBrbJ_koYLCaecCRd2n7fDhYnLX1MN/pub?w=258&h=100)


	* **date-time**: These are values that are in fact timestamps as flagged by the datastore or *TEXT* recognized as a datetime string, and thus can be converted into timestamp.

		![](https://docs.google.com/drawings/d/e/2PACX-1vR8WPzM6V5KaoSP7A8Zsuw4vcnANRfIUI2dgyZf3J688XOys4JARtZqu9e4wAps8j_KVERMUCDAfxdy/pub?w=600&h=130)

* **Sequential**:

	These are *TEXT* values where it doesnt fit the *text-tokens* classification. Or lists/arrays of values. Its vector representation is the last hidden state of an encoder (See next section).

	![](https://docs.google.com/drawings/d/e/2PACX-1vQGvf3up825nlRlCyEOn0T9hfvup7QQUFRp_55u5aRWVbPE1G75pEa3ZWD7x-NntSbZDgqhIgBEmvTZ/pub?w=416&h=102)



### DataEncoder

This step aims to reduce dimensionality of each vector representation as well as pass an encoded state of *sequential* data, this is so that in further steps all columns Tensors can be if desired, concatenated and passed as input to a model.

### RNNEncoder

It is used to encode sequential data using the latest hidden state of a *recurrent neural network* This encoder has as a hyper parameter the type of recurrent neural network to be used as well as the *hidden state* $h_N$ size. The topology that we use now are [*GRU*](https://towardsdatascience.com/understanding-gru-networks-2ef37df6c9be).

	![](https://docs.google.com/drawings/d/e/2PACX-1vRcEvtzTVhA-7GYGlVMGh37Qg2hbmHTZtTy5j8qUTtiXVSBKQEFmAoy_f8FrAlXLlQbCfO2crzYetd1/pub?w=795&h=130)

### FullyConnectedNetEncoder

This tries to reduce the dimensionality of the input using a two fully connected layer, asumming that one imput row belongs to $I\!R^N$, then the middle layer has N neurons and the second layer has $I\!R^M$, where is M is the dimension of the output/target.

![](https://docs.google.com/drawings/d/e/2PACX-1vQET8k9-wBDsAZJQiS0E4xnOnk23TrBUAyPO8OZTC8T_f9QZyUqogbf9T59fbrdvwU_Os3_nX8GGZBG/pub?w=776&h=150)

### ModelTrainer

* **ModelTrainer**: The Model Trainer uses the tensor representations of the columns and instantiates Train and Test Samplers (A Sampler allows to fetch data from the Column Tensor by batches and it can be lopped by epochs, it provides an abstraction that is independent from the ML Framework). It also instantiates, trains and validates various model constructs (essentially the way that models are coded in MindsDB is as Meta-models, MindsDB ships with some general meta-models, however, advanced users can add any meta-model they want so long is coded in either pytorch or tensorflow. The structure of the resulting  meta-models are dependent on the Sampler Input and Output structures) and each also has a flexible number of configurations/hyper-parameters.

It also instanciates, trains and validates various model constructs (essentially the way that models are coded in MindsDB is as Meta-models, MindsDB ships with some general meta-models, however, advanced users can add any meta-model they want so long is coded in either pytorch or tensorflow. The structure of the resulting  meta-models are dependant on the Sampler Input and Output structures) and each also has a flexible number of configurations/hyper-parameters.

### FullyConnectedNet

This takes the input as a concatenation of all of the input tensors, which in turn are the outputs of the encoders for each column, so assuming that there are $M$ columns in the input $I$, and that the Output $O \in I\!R^U$, make $I \in I\!R^{UxM}$. Another hyper-parameter is the number of layers, which can be any of {$3,6,9$} where the middle layer has a a dimensionality of $\in I\!R^{U/2}$.

		![](https://docs.google.com/drawings/d/e/2PACX-1vQtENZgP1MmKKy9jpRzbUfVxXrnltyuoyvo5yjdF6aB85VI9DRoPKuvxThgamwZ8Iaueomo8r14BkzB/pub?h=250)

### EnsembleConvNet

This architecture is an ensemble of each input being connected to a fully connected layer with output of the same size as the target $\in I\!R^U$, then as each ensemble net has the same size, we plug the concatenation of the ensemble outputs, to a stack of convolutional layers, as we assume that those convolutions can learn features from the ensembles, the depth can be any of {$2,4,6$} and the number of filters per layer can be any from {$N/2,..N$}, $N$ being the number of columns in the input. and finally over a fully connected layer with a linear output, what is key here is that the loss, assuming the output of each ensemble $O_{i}$ where $i \in {1,..,N}$. The output of the final fully connected layer is $O_{net}$, assuming a loss function $f(O_{model}, O_{real})$ such as $RMSE$, the loss for the full network is defined as $net_{loss}=f(O_{net},O_{real})+\frac{1}{N}{\sum_{i=1}^N{f(O_{i},O_{real})}}$.

		![](https://docs.google.com/drawings/d/e/2PACX-1vT3nWCGidpxgbidLyzopKqYbCVdbP6kphUl4Pa8SxvrnZJJQp_Ots_FD1sxyEvo_ADi_wzT1X8wojpa/pub?w=859&h=605)

<<<<<<< HEAD
### EnsembleFullyConnectedNet

This architecture is similar to the *ensemble conv net*, with the exception that it has no convolutional layers from ensemble it goes straight to a fully connected stack. The calculation of the loss is the same as described in *ensemble conv net*.

![](https://docs.google.com/drawings/d/e/2PACX-1vSVkBw0t28xaIPF_8UiLmf5vGuArsICKrR-KfylzZKJbexQVo60meRWxas0rU_-9njN9t7xTPraySMn/pub?w=859&h=605)

### ModelAnalyzer

The model analyzer phase runs after training is done in order to gather insights about the model and gather insights about the data
that we can only get post-training.

At the moment, it contains the fitting for a  probabilistic model which is used to determine the accuracy of future prediction, based on the number of missing features and the bucket in which the predicted value falls.


### ModelPredictor
The model predictor is called when the transaction is a *PREDICT* transaction. It loads the model with the highest $R^2$, the lookup for the models available is the columns in the input and output, it will look for models that match the same order in column names and data types. Once the Predictions are done, it replaces the predicted values in an output tensor (which is a copy of the input tensor).  

### DataDeVectorizer
Once the output data is ready and updated with the predictions, it proceeds to denomalize each vector that corresponds to a cell and produces a list of lists that contains the out, which will be taken by the proxy and returned to the client as if the data excited in the data store. Unless specified, it also adds a column for confidence, which is pulled from the training stats of the model, in which it can determine $P(O_{predicted}=O_{real})$ and we produce as the confidence of the individual prediction.
=======
	* **EnsembleFullyConnectedNet**: This architecture is similar to the *ensemble conv net*, with the exception that it has no convolutional layers from ensemble it goes straight to a fully connected stack. The calculation of the loss is the same as described in *ensemble conv net*.

		![](https://docs.google.com/drawings/d/e/2PACX-1vSVkBw0t28xaIPF_8UiLmf5vGuArsICKrR-KfylzZKJbexQVo60meRWxas0rU_-9njN9t7xTPraySMn/pub?w=859&h=605)

* **ModelPredictor**: The model predictor is called when the transaction is a *PREDICT* transaction. It loads the model with the highest $R^2$, the lookup for the models available is the columns in the input and output, it will look for models that match the same order in column names and data types. Once the predictions are done, it replaces the predicted values in an output tensor (which is a copy of the input tensor).  

* **DataDeVectorizer**: Once the output data is ready and updated with the predictions, it proceeds to denormalize each vector that corresponds to a cell and produces a list of lists that contains the output, which will be taken by the proxy and returned to the client as if the data excited in the data store. Unless specified, it also adds a column for confidence, which is pulled from the training stats of the model, in which it can determine $P(O_{predicted}=O_{real})$ and we produce as the confidence of the individual prediction.
>>>>>>> origin
