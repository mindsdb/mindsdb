

#### Copyright (C) 2017 MindsDB Inc. <copyright@mindsdb.com>

MindsDB's goal is to make it very simple for developers to use the power
of artificial neural networks in their projects. 


# Why use MindsDB?

Developers today are more aware of the capabilities of Machine Learning,
however from ideas of using ML to actual implementations, there are many
hurdles and therefore most ideas of using Machine Learning never even
start.

Thanks to MindsDB people building products can **focus more on**:

* Understanding what problems/predictions are interesting for the business.
* What data should be of interest for a given prediction.

**Less on:**  spending countless hours building models, making data fit into such models, training, testing, validating, tuning hyper-parameters, ....

MindsDB makes the journey of incorporating AI/Machine Learning to a project 
simpler than ever before; to do this we use a combination of various Neural 
Networks methodologies, we picked this approach of all Machine Learning 
techniques because Neural Nets have shown to be optimal at diverse 
problems and for various reasons  they tend to outperform non-neural 
network approaches to learning from data.

# How does it work?

You tell mindsDB what you want to predict and from what data it should 
learn this prediction. Such as:

```sql
 FROM <this> PREDICT <that>
```

After that; MindsDB figures out how to:

* Build a custom set of neural networks that can possibly best achieve the prediction you want.
* Prepare the data so it can be fed into the proposed neural networks
* Train, test and compare the the accuracy and complexity of each neural network.
* Deploy the best suited model to a production environment that can  be used and updated with tools that are very familiar to any developer (see the proxy section).

On top of all this it provides you with a step by step explanation of what is doing to obtain such predictions and what finds to be important within your data. 

Currently MindsDB works with relational data sources. What this means, is data lives in tables in: excel spreadsheets, CSV files or tables in any of the following database servers (oracle, mysql, postgres,  mariadb, redshift, aurora, oracle, TyDB)




# The code

 * ```config/__init__.py```: All server configuration variables are set here or via env variables
 * ```external_libs```: Any library or code that is not originally developed by mindsDB
 * ```libs```: All mindsDB code
    * ```constants```: All mindsDB constants and structs
    * ```controllers```: The server controllers; which handle transaction requests
    * ```data_models/<framework>```: Here are the various model templates by framework, given the dynamic graph capabilities of pytorch we ship with only pytorch models, but support for tensorflow is provided
    * ```data_types```: These are MindsDB data types shared across modules
    * ```helpers```: These are the mindsDB collection of functions that can be used across modules
    * ```phases```: These are the modular phases involved in any given transaction in MindsDB
    * ```workers```: Since we can distribute train and test over a computing cloud, we place train and test as worker code that can run independently and in entirely different memory spaces.





# Architecture

MindsDB system is made out of the following components

* *MindsDB Client*: This can be any application that can connect to MindsDB via either Websockets, MySQL TCP 4.2.2 or REST over HTTP.
* *MindsDB Server*: This a mindsDB instance, that is made out of a backend and frontend application, and it handles, connections and transactions over such connections.
* *MindsDB worker*: This is sub-component that can run either inside a full MindsDB instance or on a separate server, we do this to distribute computational load.
* *Data Source*: This is where the actual data lives.

The following figure illustrates the sequence and flow of data amongst the various components.

![](https://docs.google.com/drawings/d/e/2PACX-1vQ24Z_U4U7SyUOjhO_hp680WlXaFnUt9MrAY58YGZZLloFRvi5mIFE9CVxXc-71EV6Dkgn2SqpA6EzD/pub?w=598&h=826)


## MindsDB server

The MindsDB server has a backend element in charge of managing the work 
associated with each request and a front-end which is a User Interface 
and API designed to  visualize the state of each transaction as well as 
a graphical interface to facilitate the management of data sources and 
generated models.  It is worth to notice that the back-end can run 
independently of the front-end. And that the front-end can be pointed 
to any MindsDB back-end system, which is how we handle multi-tenancy.

A MindsDB backend has the following modules:

![MindsDB Server components](https://docs.google.com/drawings/d/e/2PACX-1vRZIAeWGETNKficz2PL4KQQtURhCu95NMnruA6kOjvgjgLC2IoG4uJCFlD0RIZZUfG9KC735jeJrwUT/pub?w=768&h=720)

* **Proxy**: Plays the role of exposing the Session controller over various protocols, it allows independence of the access protocol to MindsDB.
* **SessionController**: Manages the global elements of a given connection. It becomes relevant for stateful connections over WebSockets or TCP Sockets. In REST request the session starts and dies with the request.
* **TransactionController**: It coordinates depending on the type of transaction that different phase modules get called, it also keeps a state of the transaction which allows the various Phase modules to work with the data they need, think of it as a data Bus across the various Modules. 
* **Phase Modules**: This are singular units of work on a given transaction. 



### Phase Modules

![](https://docs.google.com/drawings/d/e/2PACX-1vQPGU3nzH0dwpgjzZ-bb95nJRhYUDYFuTuzIUERoVBGMMZW1ocUA1LAyDCldNKKp5RCw3Wxac21qPP7/pub?w=960&h=252)

Different transactions PREDICT, CREATE MODEL etc, require different 
steps/phases, however they may share some of these phases, 
in order to make this process modular we keep the variables in the Transaction 
controller (the data bus) as they communication interface, as such, 
the implementation of a given phase can change, so long the expected 
variables in the bus prevail. (We will describe in more detail some of 
the Phase Modules in the next section)

* **DataExtractor**: It deals with taking a query and pulling the data from the various data-sources implied in the query, building the joins (if any) and loading the full result into memory. **NOTE**: *That as of now mindsDB requires that the full dataset can be loaded into memory*. To add flexibility right now we support [Apache Drill](https://drill.apache.org/) as a data aggregator.

* **StatsGenerator**: Once the data is pulled and aggregated from the various data sources, MindsDB runs an analysis of each of the columns of the corpus, their distributions and statistical properties, the counts, etc, .. This information is to be used by later phases, the goal here is to run as much analysis on the data as possible so that later phases don't have to repeat calculations and computations over the dataset or sub-dataset they are working on. *One example of this is when we normalize a value ```j``` we want to make ```normalized(j)=(j-mean)/range```, if every single time we run normalized(j) we had to calculate the mean and range of the column that ```j``` belongs to,  it will be too expensive computationally, so calculating and storing those values in the transaction BUS makes things efficient.*
		![](https://docs.google.com/drawings/d/e/2PACX-1vTAJo6Zll3jRg-QpZTu2RkXOL0TQXl5dgBHOZqpD3jsW4frhlWxIqc0Mv1OnKbOXNc1cYMFYXMlJ96U/pub?w=502&h=252)

* **StatsLoader**: There are some transaction such as PREDICT where its assumed that the statistical information is already known, all we have to do is make sure we load the right statistics to the transaction BUS.

* **DataVectorizer**: In this phase the idea is to translate each column into *numpy* tensors that can be ingested by data models. Tensors are made of vector representations of each cell. This involves to understand what transformations are necessary depending on the column type. Currently the following column data types are supported **Note that these can be expanded or updated for various needs**:

	* **Categorical**:
	
		![](https://docs.google.com/drawings/d/e/2PACX-1vR7PCdT5QCCuQ8pG6pSRc8RfdmkCPnVVrOZNPAA9QTvqluf8e2EQRdSDXutlXho2ymz_OP3LGxo-GxE/pub?w=359)

		* **text-tokens**: columns where the values are *TEXT* but the distribution of the text is made of workds or combinations of words and the number of uniques does not exceed 10% of the total number of rows. 

		* **numeric-tokens**: columns where the values are *NUMERIC* but the number of uniques does not exceed 10% of the total number of rows.

	* **Continuous**:
		
		* **numeric**: These are *NUMERIC* values that don't match the criteria of *numeric-tokens* or *date-time*.
		
			![](https://docs.google.com/drawings/d/e/2PACX-1vQt9FeMEgBMIoEF23NqQcF3D28Vnk-D2z0pXIIjHy1LCt4l9NdBrbJ_koYLCaecCRd2n7fDhYnLX1MN/pub?w=258&h=100)

	
		* **date-time**: These are values that are in fact timestamps as flagged by the datastore or *TEXT* recognized as a datetime string, and thus can be converted into timestamp.
	
			![](https://docs.google.com/drawings/d/e/2PACX-1vR8WPzM6V5KaoSP7A8Zsuw4vcnANRfIUI2dgyZf3J688XOys4JARtZqu9e4wAps8j_KVERMUCDAfxdy/pub?w=600&h=130)
		
	* **Sequential**: 
			
		These are *TEXT* values where it doesnt fit the *text-tokens* classification. Or lists/arrays of values. Its vector representation is the last hidden state of an encoder (See next section).
		
		![](https://docs.google.com/drawings/d/e/2PACX-1vQGvf3up825nlRlCyEOn0T9hfvup7QQUFRp_55u5aRWVbPE1G75pEa3ZWD7x-NntSbZDgqhIgBEmvTZ/pub?w=416&h=102)
			




* **DataEncoder**: This step aims to reduce dimensionality of each vector representation as well as pass an encoded state of *sequential* data, this is so that in further steps all columns Tensors can be if desired, concatenated and passed as input to a model.
	
	* **RNNEncoder**: It is used to encode sequential data using the latest hidden state of a *recurrent neural network* This encoder has as a hyper parameter the type of recurrent neural network to be used as well as the *hidden state* $h_N$ size. The topology that we use now are [*GRU*](https://towardsdatascience.com/understanding-gru-networks-2ef37df6c9be).

	![](https://docs.google.com/drawings/d/e/2PACX-1vRcEvtzTVhA-7GYGlVMGh37Qg2hbmHTZtTy5j8qUTtiXVSBKQEFmAoy_f8FrAlXLlQbCfO2crzYetd1/pub?w=795&h=130)

	* **FullyConnectedNetEncoder**: This tries to reduce the dimensionality of the input using a two fully connected layer, asumming that one imput row belongs to $I\!R^N$, then the middle layer has N neurons and the second layer has $I\!R^M$, where is M is the dimension of the output/target. 

		![](https://docs.google.com/drawings/d/e/2PACX-1vQET8k9-wBDsAZJQiS0E4xnOnk23TrBUAyPO8OZTC8T_f9QZyUqogbf9T59fbrdvwU_Os3_nX8GGZBG/pub?w=776&h=150)

* **ModelTrainer**: The Model Trainer uses the tensor representations of the columns and instanciates Train and Test Samplers (A Sampler allows to fetch data from the Column Tensor by batches and it can be lopped by epochs, it provides an abstraction that is independent from the ML Framework). It also instanciates, trains and validates various model constructs (essentially the way that models are coded in MindsDB is as Meta-models, MindsDB ships with some general meta-models, however, advanced users can add any meta-model they want so long is coded in either pytorch or tensorflow. The structure of the resulting  meta-models are dependant on the Sampler Input and Output structures) and each also has a flexible number of configurations/hyper-parameters.

	* **FullyConnectedNet**: This takes the input as a concatenation of all of the input tensors, which in turn are the outputs of the encoders for each column, so assuming that there are $M$ columns in the input $I$, and that the Output $O \in I\!R^U$, make $I \in I\!R^{UxM}$. Another hyper-parameter is the number of layers, which can be any of {$3,6,9$} where the middle layer has a a dimensionality of $\in I\!R^{U/2}$.

		![](https://docs.google.com/drawings/d/e/2PACX-1vQtENZgP1MmKKy9jpRzbUfVxXrnltyuoyvo5yjdF6aB85VI9DRoPKuvxThgamwZ8Iaueomo8r14BkzB/pub?h=250)
	
	* **EnsembleConvNet**: This architecture is an ensemble of each input being connected to a fully connected layer with output of the same size as the target $\in I\!R^U$, then as each ensemble net has the same size, we plug the concatenation of the ensemble outputs, to a stack of convolutional layers, as we assume that those convolutions can learn features from the ensembles, the depth can be any of {$2,4,6$} and the number of filters per layer can be any from {$N/2,..N$}, $N$ being the number of columns in the input. and finally over a fully connected layer with a linear output, what is key here is that the loss, assuming the output of each ensemble $O_{i}$ where $i \in {1,..,N}$. The output of the final fully connected layer is $O_{net}$, assuming a loss function $f(O_{model}, O_{real})$ such as $RMSE$, the loss for the full network is defined as $net_{loss}=f(O_{net},O_{real})+\frac{1}{N}{\sum_{i=1}^N{f(O_{i},O_{real})}}$.

		![](https://docs.google.com/drawings/d/e/2PACX-1vT3nWCGidpxgbidLyzopKqYbCVdbP6kphUl4Pa8SxvrnZJJQp_Ots_FD1sxyEvo_ADi_wzT1X8wojpa/pub?w=859&h=605)

	* **EnsembleFullyConnectedNet**: This architecture is simiar to the *ensemble conv net*, with the exception that it has no convolutional layers from ensemble it goes straight to a fully connected stack. The calculation of the loss is the same as described in *ensemble conv net*.
	
		![](https://docs.google.com/drawings/d/e/2PACX-1vSVkBw0t28xaIPF_8UiLmf5vGuArsICKrR-KfylzZKJbexQVo60meRWxas0rU_-9njN9t7xTPraySMn/pub?w=859&h=605)

* **ModelPredictor**: The model predictor is called when the transaction is a *PREDICT* transaction. It loads the model with the highest $R^2$, the lookup for the models available is the columns in the input and output, it will look for models that match the same order in column names and data types. Once the Predictions are done, it replaces the predicted values in an output tensor (which is a copy of the input tensor).  

* **DataDeVectorizer**: Once the output data is ready and updated with the predictions, it proceeds to denomalize each vector that corresponds to a cell and produces a list of lists that contains the out, which will be taken by the proxy and returned to the client as if the data excited in the data store. Unless specified, it also adds a column for confidence, which is pulled from the training stats of the model, in which it can determine $P(O_{predicted}=O_{real})$ and we produce as the confidence of the individual prediction. 


