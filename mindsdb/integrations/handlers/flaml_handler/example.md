# Preliminaries

1. Make sure your python environment has mindsdb and FLAML installed.

# MindsDB example commands

We will use the "[Heart Disease](https://raw.githubusercontent.com/mindsdb/mindsdb-examples/master/classics/heart_disease/processed_data/train.csv)" dataset:

>NOTE: In below tutorial, it has been assumed that **heart_disease** is in **files** database

1. Select a subsample of the data to inspect it:

```sql
SELECT * 
FROM files.heart_disease 
LIMIT 10;
```
```sql
+------+------+------+----------+-------+------+---------+---------+-------+---------+-------+------+------+--------+
| age  | sex  | cp   | trestbps | chol  | fbs  | restecg | thalach | exang | oldpeak | slope | ca   | thal | target |
+------+------+------+----------+-------+------+---------+---------+-------+---------+-------+------+------+--------+
| 63.0 |  1.0 |  3.0 |    145.0 | 233.0 |  1.0 |     0.0 |   150.0 |   0.0 |     2.3 |   0.0 |  0.0 |  1.0 |    1.0 |
| 37.0 |  1.0 |  2.0 |    130.0 | 250.0 |  0.0 |     1.0 |   187.0 |   0.0 |     3.5 |   0.0 |  0.0 |  2.0 |    1.0 |
| 56.0 |  1.0 |  1.0 |    120.0 | 236.0 |  0.0 |     1.0 |   178.0 |   0.0 |     0.8 |   2.0 |  0.0 |  2.0 |    1.0 |
| 57.0 |  0.0 |  0.0 |    120.0 | 354.0 |  0.0 |     1.0 |   163.0 |   1.0 |     0.6 |   2.0 |  0.0 |  2.0 |    1.0 |
| 57.0 |  1.0 |  0.0 |    140.0 | 192.0 |  0.0 |     1.0 |   148.0 |   0.0 |     0.4 |   1.0 |  0.0 |  1.0 |    1.0 |
+------+------+------+----------+-------+------+---------+---------+-------+---------+-------+------+------+--------+
```
2. Create an AI Table with TPOT as the ML backend:
   
```sql
CREATE MODEL 
  mindsdb.heart_disease_flaml_model
FROM files
  (SELECT * FROM heart_disease )
PREDICT target
USING
engine='FLAML';
```




3. Check the status of the predictor. It may take a while to finish training:

```sql
SELECT * FROM mindsdb.models where name='heart_disease_flaml_model';
```
```sql
+-------------+--------+---------+---------+----------+----------+---------+---------------+-----------------+-------+---------------------+-----------------------------------+------------------------+-----------------------+---------------------+------+----------------------------+
| NAME        | ENGINE | PROJECT | VERSION | STATUS   | ACCURACY | PREDICT | UPDATE_STATUS | MINDSDB_VERSION | ERROR | SELECT_DATA_QUERY   | TRAINING_OPTIONS                  | CURRENT_TRAINING_PHASE | TOTAL_TRAINING_PHASES | TRAINING_PHASE_NAME | TAG  | CREATED_AT                 |
+-------------+--------+---------+---------+----------+----------+---------+---------------+-----------------+-------+---------------------+-----------------------------------+------------------------+-----------------------+---------------------+------+----------------------------+
| heart_disease_flaml_model | FLAML  | mindsdb |       1 | complete | NULL     | target  | up_to_date    | 23.4.4.4        | NULL  | SELECT * FROM Heart | {'target': 'target', 'using': {}} | NULL                   | NULL                  | NULL                | NULL | 2023-05-07 10:53:46.937682 |
+-------------+--------+---------+---------+----------+----------+---------+---------------+-----------------+-------+---------------------+-----------------------------------+------------------------+-----------------------+---------------------+------+----------------------------+
```

3. Let me make a prediction for you. 

```sql 
SELECT *
FROM mindsdb.heart_disease_flaml_model
WHERE age=63
AND sex=1
AND cp=3
AND trestbps=145
AND chol=244
AND fbs=1
AND restecg=0
AND thalach=160
AND exang=0
AND oldpeak=2.3
AND slope=0
AND ca=0
AND thal=1;
```
```sql

+--------+------+------+------+----------+------+------+---------+---------+-------+---------+-------+------+------+
| target | age  | sex  | cp   | trestbps | chol | fbs  | restecg | thalach | exang | oldpeak | slope | ca   | thal |
+--------+------+------+------+----------+------+------+---------+---------+-------+---------+-------+------+------+
|    1.0 | 63   | 1    | 3    | 145      | 244  | 1    | 0       | 160     | 0     | 2.3     | 0     | 0    | 1    |
+--------+------+------+------+----------+------+------+---------+---------+-------+---------+-------+------+------+
```