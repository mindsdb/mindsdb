


# The code

 * ```config/__init__.py```: All server configuration variables are set here or via env variables
 * ```external_libs```: Any library or code that is not originally developed by mindsDB
 * ```libs```: All mindsDB code
    * ```constants```: All mindsDB constants and structs
    * ```controllers```: The server controllers; which handle transaction requests
    * ```ml_models/<framework>```: Here are the various model templates by framework, given the dynamic graph capabilities of pytorch we ship with only pytorch models, but support for tensorflow is provided
    * ```data_types```: These are MindsDB data types shared across modules
    * ```helpers```: These are the mindsDB collection of functions that can be used across modules
    * ```phases```: These are the modular phases involved in any given transaction in MindsDB
    * ```workers```: Since we can distribute train and test over a computing cloud, we place train and test as worker code that can run independently and in entirely different memory spaces.
    * ```data_sources```: MindsDB allows to use various forms of datasets and also to mix them.
    * ```data_entities```: MindsDB stores training data in an object database and these are the entities that we access these through.

        


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

