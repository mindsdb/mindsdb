---
id: column_metrics
title: Column metrics
---

Mindsdb generate some metrics (we call them "scores") for each of the columns it processes in order to determine the quality.

This document contains information on each of these scores, as well as an outline for how mindsdb computes and uses them.

# The Scores

## Value Distribution Score

Warning if: > 0.8

Reliability: Average

The duplicates score represents how biased the data is towards a certain value.

We compute this as one minus the number of time the most common value occurs over the mean of the nr of occurrences for all values.

This score can indicate either biasing towards one specific value of a column or a large number of outliers. So it is a reliable quality indicator but we aren’t always sure “why” that is (as in, it can fall into 2 buckets).



We might want to give different warnings based on the value of the z score and the lof score.

If this score is especially high > 0.8 (meaning a value is present 5x times more than the mean), then we will warn the user and inform him what said most common value is.  


## Duplicates Score

Warning if: > 0.5 AND overall quality is low

Reliability: Poor

As it stands, duplicates score consists in the % of duplicate values / 100. So, it can range from 0 (no duplicates) to 1 (all the values have one or more duplicates).

This score is always 0 if the data type is categorical or date. Since duplicates in those cases are to be expected.

It’s also not necessarily “bad” if this score is large, for example, a text column consisting of: [‘house’, ‘apartment’, ‘house’, ‘apartment’, ‘duplex’, ‘house’, ‘apartment’, ‘duplex’], maybe not be interpreted as “categorical” by our data type generator and will have a duplicate score of 1.

Thus, we should give a warning about this score if it’s above 0.5, but ONLY if the overall quality of the data is bad overall.


## Empty cells score

Warning if: > 0.2

Reliability: Good

This score is computed as the % of empty values / 100. It’s very reliable on its own, since empty values in a column are never a good sign.

If more than 20% of a column is empty, the user should be warned about it, since that column will likely introduce some unreliability within the model itself.


## Data type distribution score

Warning if: > 0.2

Reliability: Average

This score indicates the % of data that are not of the same data type as the principal column type / 100.

Realistically speaking, it’s unlikely this score will often be close to 1, so we should give a warning at a rather low value (I’ve chosen 0.2).


## Z Test based outlier score

Warning if: > 0.3

Reliability: Average

This score indicates the % of data that are 3 STDs or more away from the mean.

 \
There are various types of data where this can be the case, but usually it might indicate the data has too much noise.

We should warn the user if this value is above 0.3 (over 30% of the data are considered outliers based on the test).


## Local outlier factor score

Warning if: > 0.2

Reliability: Average

The LOF algorithm from sklearn, which is a knn based approach using the 20th nearest neighbours to classify a data point.

Data points are clusters and an outlier score is given to each data point (1 is an inliner, -1 is an outlier).

We consider every point with an LOF score < 0.8 to be an outlier and compute the lof outlier score as the nr of these outliers / 100.

LOF is fairly proven on real datasets at discovering outliers, but less intuitive (we currently treat it as a blackbox) than something like the z score, so we should be careful on relying too much on it.

Since obtaining a score of < 0.8 based on LOF is rather difficult, we should be worries with the overall number of outliers is > 20% (score > 0.2), which is a significant amount of data that doesn’t fit closely in any cluster.


## Similarity score

Warning if: > 0.5

Reliability: Good (When high)

This score is simply a matthews correlation applied between this column and all other column.

The score * 100 is the number of values which are similar in the column that is most similar to the scored column.

If this score is > 0.5 that means half the values are similar (in the same position) in two columns, we should warn the user about this by information him of which two columns are correlated.


## Correlation score

Warning if: > 0.4

Reliability: Average

This score uses a classifier (in this case a decision tree) and tries to predict the value in one column based on the values in all the other column (the classifier is pre-fit on the data it tries to predict, so we essentially just look at the features biasing inside the classifier by running a predict).

If the prediction is highly accurate and one column in particular is most important in making the prediction, this score will be bad. We compute it as the highest correlated column (from 0 to 1) times the accuracy of the prediction (using sklearn’s score function, at most this value is 1).

If this score is above 0.4, we should warn the user that one of his column is heavily correlated with this.


# Derived scores


## Consistency score

Warning if: > 0.2

Reliability: Average

The data consistency score is mainly based upon the **Data Type Distribution Score** and the **Empty Cells Score**, the **Duplicates Score** is also taken into account if present but with a smaller (2x smaller) bias.

If this score is > 0.2 we should warn the user about the data in his column being inconsistent (as in, incomplete and/or vague)


## Redundancy score

Warning if: > 0.45

Reliability: Average

The value is based in equal part on the **Similarity Score** and the **Correlation Score**.

If this score is > 0.45 we should warn the user about the data in his column being possibly redundant, that means any insights implied by this column are already present in the data from other columns).

Variability/Randomness score

Warning if: > 0.5

Reliability: Average

The value is based in equal part on the **Z Test based outliers score**, the **LOG based outlier score** and the **Value Distribution Score**.

In certain cases, only the **Value Distribution Score **can be computed for the column. Then, we reduce the relevance of this score by dividing it by 2 (possibly not the best approach).

If this score is > 0.5 we should warn the user that there’s too much randomness/noise in this data.
