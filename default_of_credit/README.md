## Summary

This is a "standard" financial dataset for which neural networks would not actually be applied, since they are black box models and the financial industries often needs to know the "why" behind an event, a simple prediction isn't enough. It would be interesting to do a more in-depth analysis of this data based on the mindsdb log to determine that.

It was hard to find any "state of the art" models on this dataset, and every paper I've found seems to have chosen to measure accuracy differently. Thus, I can't really rank the best/worst models.

Compared to some standard analysis techniques mindsdb seem to preform quite well based on the accuracy scores those analysis have chosen (ROC Area under curve and confusion matrix based individual weighted accuracy).

Strangely enouhg, when the dataset is "weighted" as to yield and equal amount of rows for each target variable, the accuracy gets worst.

## Accuracy of other models

On this type of dataset it's hard to evaluate the exact accuracy, since depending on usecase we might put more or less weight behind FP, FN, TP or TN.

After searching the internet for a while I've found two pretty well-explained articles running some standard regression and decission tree based analysis on the dataset:

* https://www.kaggle.com/gpreda/default-of-credit-card-clients-predictive-models#Conclusions
* http://inseaddataanalytics.github.io/INSEADAnalytics/CourseSessions/ClassificationProcessCreditCardDefault.html#step_6_test_accuracy

The former uses ROC AUC score, and the best score it obtains is somewhat better than mindsdb (~0.73 vs mindsdb's ~0.67)
The later uses a confusion matrix, and mindsdb beats it in predicting the label (1) when duplicating data and closely matches both accuracies when working on the raw dataset.


## Mindsdb accuracy

The test dataset contains 4577 0s and 1295 1s

-- On the raw dataset --
The test dataset contains 4577 0s and 1295 1s
Log loss accuracy of 81.08% !
AUC score of 0.6391496680987614
Accuracy of 94.63% for predicting 0 labels
Accuracy of 33.2% for predicting 1 labels


-- With target variable based duplication--
The test dataset contains 4577 0s and 1295 1s
Log loss accuracy of 77.52% !
AUC score of 0.6653220779067404
Accuracy of 86.19% for predicting 0 labels
Accuracy of 46.87% for predicting 1 labels


### Lightwood
Above accuracies are obtained with lightwood

### Ludwig
Predicts all 0 when data is uneven, crashes with a weird error when target-val == 1 rows are duplicated.

## References
*https://bradzzz.gitbooks.io/ga-dsi-seattle/dsi/dsi_05_classification_databases/2.1-lesson/assets/datasets/DefaultCreditCardClients_yeh_2009.pdf
*http://inseaddataanalytics.github.io/INSEADAnalytics/CourseSessions/ClassificationProcessCreditCardDefault.html#step_6_test_accuracy
*https://www.kaggle.com/uciml/default-of-credit-card-clients-dataset/kernels
