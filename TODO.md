
# Data description and analysis

The objective is that we can visualize findings about our data, 
one of the most intuitive forms of representing the shape of data is in a histogram. 
Therefore, it makes sense to develop methodologies be able to be able to store histrogram representation data of all columns regardless of their type. 

### Histogram friendly data types

* NUMERIC
* CATEGORICAL
* DATE

### Histogram non-friendly data types 

* BINAY.IMAGE
* SEQUENTIAL.TEXT
* SEQUENTIAL.TIME_SERIES
* BINARY.VIDEO
* BINARY.AUDIO

A solution for this is to:
 - Define a methodology to calculate a [*phash*](https://en.wikipedia.org/wiki/Perceptual_hashing) for a given sample.
 - Extract a subset of random samples, calculate the sample size using [Cochran’s sample size calculator](https://github.com/mindsdb/mindsdb/blob/master/mindsdb/external_libs/stats.py) defined by margin of error 
 - Define a methodology to calculate the *averge_phash* of all samples in the subset
 - Create histogram based on distance between the phash of each sample in the subset and the mean_phash.
 - Store some samples/(or averages when possible) of each bucket of the histogram so that they can be visualized.

NOTE:  A phash is a vector or hopefully a value such that the euclidean distance between two samples represents how close they are from eachother.

By having distances to the mean for each sample, we can also calculate quality metrics such as distances variance, outliers, etc.., just as we calculate them for histogram friendly data types.

#### phash calculations

* BINARY.IMAGE: There is a python library [imagehash](https://pypi.org/project/ImageHash/) for image phash calculation, lets use that
* SEQUENTIAL.TEXT: If we cannot find a phash library lets just add the one hot vectors for each word and the caclulate distances using the aggregate one hot vector.
* SEQUANTIAL.TIME_SERIES: There is a library cesium-ml.org for time series feature extraction, lets create a vector of features from that and use it to calculate distances.

not urgent:

* BINARY.AUDIO: There is python library for phash on audio
* BINARY.VIDEO: Todo


# Model evaluation

Once the model is trained, we need to evaluate the model

* Is the model good or bad in overall?
* I want to know is when I (should and should not) trust the model?
* Identify what are the factors that influence why it can be or not trusted.
* what recommendations can I make.


## Is the model good or bad in overall?

We can calculate the overall accuracy of the model, this will depend on the target variable, how can we generalize it,
As well as we need to determine an error value between predicted and real
Also, ideally, we can have an accuracy representation calculation, which can be a value from 0-1, so that it can read in percentage

* NUMERIC/DATE: 
    * accuracy: R^2
    * error: abs(real - predicted)/abs(real)
    * accuracy_rep: average in the diagonal of the confusion matrix for the histogram
* CATEGORICAL: 
    * accuracy: K.mean(K.equal(K.argmax(y_true, axis=-1), K.argmax(y_pred, axis=-1))) (taken from categorical keras accuracy)
    * error: vector representations : distance(real,predicted) or crossentropy?
    * accuracy_rep: average in the diagonal of the confusion matrix for the histogram
* SEQUENTIAL:
    * accuracy: ?
    * error: phash : distance(real,predicted)
    * accuracy_rep: ?    
* BINARY.IMAGE:
    * accuracy: ?
    * error: phash : distance(real,predicted)
    * accuracy_rep: ?
* BINARY.AUDIO:
    * accuracy: ?
    * error: phash : distance(real,predicted)
    * accuracy_rep: ?
    
NOTE: For this v1.0 release, we should leave out BINARY.VIDEO and possibly BINARY.AUDIO

### Feature importance methodology

Iterate over all input features for each target, calculate r^2 when only that feature is passed and when all but that feature and use this to score feature importance.
If we have a way to calculate uncertainty, also calculate which features bring the least uncertainty in similar fashion to r^2 score.

TODO: How to estimate uncertainty.

Ideas: https://www.itl.nist.gov/div898/handbook/pmd/section5/pmd512.htm

### Visualizing accuracy

The idea is to be able to understand the following:

* Are there any concerns in the accuracy? 
    * Are there target values that are not being predicted that well?
        * Why can be the reasons for the target values that have less accuracy?
            * Which are the most important features for these target values?
            * Are there any data quality warnings for the important features and the subset of data?
    * What are the data points responsible for similar errors and what is common between them? 
        * Identify what are the most important features in general for the model 
        * find clusters based on error value (use Spectral clustering or DBSCAN)
        * for the clusters that have that share the highest error, find distributions for each important general model  feature
        
