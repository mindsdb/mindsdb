---
title: Anomaly Detection Handler
sidebarTitle: Anomaly Detection
---

The Anomaly Detection handler implements supervised, semi-supervised, and unsupervised anomaly detection algorithms using the pyod, catboost, xgboost, and sklearn libraries. The models were chosen based on the results in the [ADBench benchmark paper](https://proceedings.neurips.cc/paper_files/paper/2022/hash/cf93972b116ca5268827d575f2cc226b-Abstract-Datasets_and_Benchmarks.html).

<Info>
**Additional information**

- If no labelled data, we use an unsupervised learner with the syntax `CREATE ANOMALY DETECTION MODEL <model_name>` without specifying the target to predict. MindsDB then adds a column called `outlier` when generating results.

- If we have labelled data, we use the regular model creation syntax. There is backend logic that chooses between a semi-supervised algorithm (currently XGBOD) vs. a supervised algorithm (currently CatBoost).

- If multiple models are provided, then we create an ensemble and use majority voting.

- See the anomaly detection proposal [document](https://docs.google.com/document/d/1Yd7ARZVg_67xlcY-JR2kuO7mak9Ia2YER1Jk0EdpEa0/edit#heading=h.mo4wxsae6t1d) for more information.

</Info>

<Info>
**Context about types of anomaly detection**

- Supervised: we have inlier/outlier labels, so we can train a classifier the normal way. This is very similar to a standard classification problem.

- Semi-supervised: we have inlier/outlier labels and perform an unsupervised preprocessing step, and then a supervised classification algorithm.

- Unsupervised: we don’t have inlier/outlier labels and cannot assume all training data are inliers. These methods construct inlier criteria that will classify some training data as outliers too based on distributional traits. New observations are classified against these criteria. However, it’s not possible to evaluate how well the model detects outliers without labels.

</Info>

<Info>
**Default dispatch logic**

We propose the following logic to determine type of learning:
- Use supervised learning if labels are available and the dataset contains at least 3000 samples.
- Use semi-supervised learning if labels are available and number of samples in the dataset is less than 3000.
- If the dataset is unlabelled, use unsupervised learning.

We’ve chosen 3000 based on the results of the NeurIPS AD Benchmark paper (linked above). The authors report that semi-supervised learning outperforms supervised learning when the number of samples used is less than 5% of the size of the training dataset. The average size of the training datasets in their study is 60,000, therefore this 5% corresponds to 3000 samples on average.

</Info>

<Info>
**Reasoning for default models on each type**

We refer to the NeurIPS AD Benchmark paper (linked above) to make these choices:
- For supervised learning, use CatBoost. It often outperforms classic algorithms.
- For semi-supervised, XGBod is a good default from PyOD.
- There’s no clear winner for unsupervised methods, it depends on the use case. ECOD is a sensible default with a fast runtime. If we’re not concerned about runtime, we can use an ensemble.

</Info>

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To use Anomaly Detection handler within MindsDB, install the required dependencies following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).

## Setup

Create an AI engine from the [Anomaly Detection handler](https://github.com/mindsdb/mindsdb/tree/staging/mindsdb/integrations/handlers/anomaly_detection_handler).

```sql
CREATE ML_ENGINE anomaly_detection_engine
FROM anomaly_detection;
```

Create a model using `anomaly_detection_engine` as an engine.

```sql
CREATE ANOMALY DETECTION MODEL anomaly_detection_model
FROM datasource
    (SELECT * FROM data_table)
PREDICT target_column
USING
      engine = 'anomaly_detection_engine',  -- engine name as created via CREATE ML_ENGINE
      ...;                                  -- other parameters shown in usage examples below
```

## Usage

To run example queries, use the data from this [CSV file](https://raw.githubusercontent.com/mindsdb/mindsdb/staging/tests/unit/ml_handlers/data/anomaly_detection.csv).

### Unsupervised detection

```sql
CREATE ANOMALY DETECTION MODEL mindsdb.unsupervised_ad
FROM files
    (SELECT * FROM anomaly_detection)
USING 
    engine = 'anomaly_detection_engine';

DESCRIBE MODEL mindsdb.unsupervised_ad.model;

SELECT t.class, m.outlier as anomaly
FROM files.anomaly_detection as t
JOIN mindsdb.unsupervised_ad as m;
```

### Semi-supervised detection

```sql
CREATE MODEL mindsdb.semi_supervised_ad
FROM files
    (SELECT * FROM anomaly_detection)
PREDICT class
USING
    engine = 'anomaly_detection_engine';

DESCRIBE MODEL mindsdb.semi_supervised_ad.model;

SELECT t.carat, t.category, t.class, m.class as anomaly
FROM files.anomaly_detection as t
JOIN mindsdb.semi_supervised_ad as m;
```

### Supervised detection

```sql
CREATE MODEL mindsdb.supervised_ad
FROM files
    (SELECT * FROM anomaly_detection)
PREDICT class
USING
    engine = 'anomaly_detection_engine',
    type = 'supervised';

DESCRIBE MODEL mindsdb.supervised_ad.model;

SELECT t.carat, t.category, t.class, m.class as anomaly
FROM files.anomaly_detection as t
JOIN mindsdb.supervised_ad as m;
```

### Specific model

```sql
CREATE ANOMALY DETECTION MODEL mindsdb.unsupervised_ad_knn
FROM files
    (SELECT * FROM anomaly_detection)
USING 
    engine = 'anomaly_detection_engine',
    model_name = 'knn';

DESCRIBE MODEL mindsdb.unsupervised_ad_knn.model;

SELECT t.class, m.outlier as anomaly
FROM files.anomaly_detection as t
JOIN mindsdb.unsupervised_ad_knn as m;
```

### Specific anomaly type

```sql
CREATE ANOMALY DETECTION MODEL mindsdb.unsupervised_ad_local
FROM files
    (SELECT * FROM anomaly_detection)
USING 
    engine = 'anomaly_detection_engine',
    anomaly_type = 'local';

DESCRIBE MODEL mindsdb.unsupervised_ad_local.model;

SELECT t.class, m.outlier as anomaly
FROM files.anomaly_detection as t
JOIN mindsdb.unsupervised_ad_local as m;
```

### Ensemble

```sql
CREATE ANOMALY DETECTION MODEL mindsdb.ad_ensemble
FROM files
    (SELECT * FROM anomaly_detection)
USING 
    engine = 'anomaly_detection_engine',
    ensemble_models = ['knn','ecod','lof'];

DESCRIBE MODEL mindsdb.ad_ensemble.model;

SELECT t.class, m.outlier as anomaly
FROM files.anomaly_detection as t
JOIN mindsdb.ad_ensemble as m;
```

<Tip>

**Next Steps**

Watch [demo 1](https://www.loom.com/share/0996e5faa3f7415bacd51a6e8e161d5e?sid=9bacd29a-975b-4a94-b081-de2255b93607) and [demo 2](https://www.loom.com/share/c22335d83cb04ac281e2ef080792f2dd) to see usage examples.

Go to the [Use Cases](https://docs.mindsdb.com/use-cases/overview) section to see more examples.
</Tip>
