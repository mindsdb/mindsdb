---
id: column-metrics
title: Column metrics
---

Mindsdb generate some metrics (we call them "scores") for each of the columns it processes in order to determine the quality.

This document contains information on each of these scores, as well as an outline for how mindsdb computes and uses them.

A score of zero is considered very bad, whilst a score of one is considered perfect.

# The Scores

## Value Distribution Score

**Warning if < 3**

*Reliability: Average*

The value distribution score represents how biased the data is towards a certain value.

This score can indicate either biasing towards one specific value of a column or a large number of outliers. So it is a reliable quality indicator but we aren't always sure “why” that is (as in, it can fall into 2 buckets).

Currently a warning based on this score indicates a value is present that's at least 5x times more than the mean.


## Duplicates Score

**Warning if < 6 and overall quality is low**

*Reliability: Poor*

As it stands, duplicates score consists in the % of duplicate values / 100. So, it can range from 0 (no duplicates) to 1 (all the values have one or more duplicates).

This score is always 0 if the data type is categorical or date. Since duplicates in those cases are to be expected.

It’s also not necessarily “bad” if this score is large, for example, a text column consisting of: [‘house’, ‘apartment’, ‘house’, ‘apartment’, ‘duplex’, ‘house’, ‘apartment’, ‘duplex’], maybe not be interpreted as “categorical” by our data type generator and will have a duplicate score of 1.


## Empty cells score

**Warning if < 8**

*Reliability: Good*

This score is computed as the % of empty values / 100. It’s very reliable on its own, since empty values in a column are never a good sign.


## Data type distribution score

**Warning if > 7**

*Reliability: Average*

This score indicates the % of data that are not of the same data type as the principal column type / 100.


## Z Test based outlier score

**Warning if < 6**

*Reliability: Average*

This score indicates the % of data that are 3 STDs or more away from the mean.

There are various types of data where this can be the case, but usually it might indicate the data has too much noise.



## Local outlier factor score

**Warning if < 4**

*Reliability: Average*

The data consistency score is mainly based upon the **Data Type Distribution Score** and the **Empty Cells Score**, the **Duplicates Score** is also taken into account if present but with a smaller (2x smaller) bias.


## Similarity score

**Warning if < 6**

*Reliability: Good (When high)*

This score is simply a matthews correlation applied between this column and all other column.

The score * 100 is the number of values which are similar in the column that is most similar to the scored column.

# Derived scores


## Consistency score

**Warning if: > 0.2**

*Reliability: Average*

The data consistency score is mainly based upon the **Data Type Distribution Score** and the **Empty Cells Score**, the **Duplicates Score** is also taken into account if present but with a smaller (2x smaller) bias.

## Redundancy score

**Warning if < 5**

*Reliability: Average*

The value is based in equal part on the **Similarity Score** and the **Correlation Score**.

## Variability/Randomness score

**Warning if < 6**

*Reliability: Average*

The value is based in equal part on the **Z Test based outliers score**, the **LOG based outlier score** and the **Value Distribution Score**.

In certain cases, only the **Value Distribution Score **can be computed for the column. Then, we reduce the relevance of this score by dividing it by 2 (possibly not the best approach).
