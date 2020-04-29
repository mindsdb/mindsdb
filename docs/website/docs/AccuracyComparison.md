---
id: accuracy-comparison
title: Comparing Mindsdb accuracy 
hide_title: true
---

# Comparing Mindsdb accuracy with state-of-the-art models
We have a few example datasets where we try to compare the accuracy obtained by Mindsdb with that of the best models we could find.

It should be noted, Mindsdb accuracy doesn't always beat or match stat-of-the-art models, but the main goal of Mindsdb is to learn quickly, be very easy to use and be adaptable on any dataset.

If you have the time and know-how to build a model that performs better than Mindsdb, but you still want the insights into the model and the data, as well as the pre-processing that Mindsdb provides, you can always plug in a custom machine learning model into Mindsdb.

You can find an up to date list of accuracy comparisons here:

https://github.com/mindsdb/mindsdb-examples/tree/master/benchmarks

Each directory containes different examples, datasets and `README.md`. To see the accuracies and the models, simply run `mindsdb_acc.py` to run mindsdb on the dataset.

At some point we might keep a more easy to read list of these comparisons, but for now the results change to often and there are too many models to make this practical to maintain.

We invite anyone with an interesting dataset and a well performing models to send it to us, or contribute to this repository, so that we can see how mindsdb stands up to it (or try it themselves and tell us the results they got).