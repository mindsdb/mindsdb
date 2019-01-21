[<Back to Table of Contents](../README.md)

# FAQ

### Why MindsDB?

We are building MindsDB, because we want to *Democratize Machine Learning*.
If you want to learn more about why this is important and how we aim to do this, you can checkout our presentation [here](https://docs.google.com/presentation/d/e/2PACX-1vTfmQfc4rV2OjepzAMsCRAK3Z0h-IbOLMx66Ao_tFAvNX6yjNv4f1AkRK31mCLmKm8DbWwz-2F0NeBQ/pub?start=false&loop=false&delayms=3000).

### Who are we building MindsDB for?

We are building MindsDB for all of those that can get their hands in data. and can type a few lines of code.

### What are MindsDB's current Goals?

MindsDB has 3 simple goals.

1) Provide a way to learn and predict from data with a line of code.
2) Explainable, by answering the following questions:
    * When learning:
        * What is interesting in my data and why?
        * When can I trust this model and why?
        * When I should not trust this model and why?
        * How can I improve this model?
    * When predicting:
        * Why this prediction?
        * Why not something else?
3) Remain state of the art. Since MindsDB users are delegating the ML machinery, MindsDB should try to always generate state of the art models for the users.

### What is the roadmap?

MindsDB roadmap is amied to be aligned with our goals:
 
* **versions  > 0.9** was focused on goal 1 *(December 2018)*. 
* **versions 1.0** and greater will focus on for goal 2 *(January 2019)*
    * What to expect:
        * MindsDB UI where one can visualize explainability goals.
* **versions 2.0** and greater will focus on goal 3 *(March 2019)*.
    * What to expect:
        * Simpler modularization of meta-model building for ML/Experts to contribute.

### What type of data can MindsDB learn and predict from?   

Currently we support tabular data, this is CSV, excel, json files or relational data stores.
You can learn more about how to prepare data in MindsDB [(here)](DataSources.md)

### How does it work? ###

in very simple terms, midnsdb follows the following steps:

 * **to learn**:
    * break data source intro train, test, validate
    * transform data source into tensors
    * build and train encoders (if necessary)
    * produce a neural network based model that can take in the input tensor and produce the target tensor
    * break train data into batches and try learning a model that can fit the target
      * test and validate until model convergence
    * store metadata about most fit model
 * **to predict**:
    * transform question data into input tensor
    * load most fit model
    * run input tensor into model
    * transform output tensor into readable output
    
You can learn a more detailed description [here](InsideMindsDB.md)

### How can I help?

You can help in the following ways:

 * Trying MindsDB and [reporting issues](https://github.com/mindsdb/mindsdb/issues/new/choose).
 * If you know python, you can also help us debug open issues. 
 * You can help us with documentation and examples.
 * Tell your friends, write a blog post about MindsDB.
 * Join our team.
 
### Why is it called MindsDB?

Well, as most names, we needed one, we like science fiction and the [culture series](https://en.wikipedia.org/wiki/The_Culture_(series)), where there are these AI super smart entities called Minds.

How about the DB part?. Although in the future we will support all kinds of data, currently our objective is to add intelligence to existing data stores/databases, hence the term DB.
As to becoming a **Mind** to your **DB**.

Why the bear? Who *don't* like beers! anyway, a bear for UCBerkeley where this all was initially coded.
