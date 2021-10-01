# Evaluate model quality using Studio

Once the model is trained, you can use the model quality preview to get insights about the trained model.

!!! question "Visualize model quality"
    Note that any model can be evaluated using MindsDB Studio. That means not only models
    trained with Studio but also models trained from SQL clients, MindsDB APIs or the SDK.

To do this:

1. From the left navigation menu, select the `Predictors` dashboard.
2. Click on the `PREVIEW` button on the model you want to evaluate.
3. Click on the minus sign to expand each section.

![Model quality info](/assets/predictors/model-quality.gif)

### Model Accuracy

In this section, MindsDB will show you visualizations about:

* Data splitting (80% training data and 20% test data)
* Model accuracy (How accurate is the model when blindsided)

![Model accuracy](/assets/predictors/model-accuracy.png)

### Column importance

This section tries to answer the question, `What is important for this model?`. MindsDB tries various combinations of missing columns to determine the importance of each one. The column importance rating ranges from 0, which means the column is useless to 10, meaning the column's predictive ability is great.

![Column importance](/assets/predictors/column-importance.png)

### Confusion matrix

This section tries to answer the question, `When can you trust this model?`.
Here, the confusion matrix shows the performance that the model gets when solving a classification problem. Each entry in the confusion matrix shows how many samples of each class were correctly classified and how many and where incorrectly classified. To get a detailed explanation, you can move the mouse cursor over the graphics.

![Confusion matrix](/assets/predictors/confusion-matrix.png)


!!! Success "Let's query the model :bar_chart: :mag_right:"
    After evaluating the performance of the model, the next step is to get predictions by [querying the model](/model/query/studio/).

