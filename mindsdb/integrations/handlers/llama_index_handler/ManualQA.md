Manual QA tests for LlamaIndex using Blackrock's webpage.

webpage:https://www.blackrock.com/za/individual/about-us

### Testing CREATE ML_Engine

`CREATE ML_ENGINE llamaindex
FROM llama_index;
`

Result:
![1 ml_engine](https://github.com/mindsdb/mindsdb/assets/32901682/4904dac1-48c8-4e73-b254-82fce068c26c)


### Testing CREATE MODEL

```sql
CREATE MODEL qa_blackrock
FROM files
    (SELECT * FROM about_blackrock)
PREDICT Answers
USING 
  engine = 'llamaindex', 
  index_class = 'GPTVectorStoreIndex',
  reader = 'DFReader',
  source_url_link = 'https://www.blackrock.com/za/individual/about-us',
  input_column = 'Questions',
  openai_api_key = 'your_api_key';
  ```
  
Result:
![2 create_model](https://github.com/mindsdb/mindsdb/assets/32901682/409205c1-a6d9-4219-88f3-28382bc2b200)


### Describe model

```sql
DESCRIBE qa_blackrock```

Result:
![3 describe](https://github.com/mindsdb/mindsdb/assets/32901682/f68f6160-e4a4-47ef-b7ff-79758ea34a3b)


### Select data from the model

```sql
SELECT Questions,Answers
FROM mindsdb.qa_blackrock
WHERE Questions = 'What is the best long term investment with minimal risks for private investors';
```

Result:
![4 select_model](https://github.com/mindsdb/mindsdb/assets/32901682/86e25738-b434-49a5-a4ec-75640779dbfc)


Answer
`The best long term investment with minimal risks for private investors is a diversified portfolio of low-cost index funds. These funds typically track a broad market index, such as the S&P 500, and provide exposure to a wide variety of publicly traded companies with minimal risk. Blackrock is one of the largest asset management companies in the world, and offers a range of index funds, exchange-traded funds, and other investment products for private investors.`

### Testing Batch Prediction

```sql
SELECT a.Questions,b.Answers
FROM mindsdb.qa_blackrock as b
JOIN files.about_blackrock as a;
```

Result
![5 batch](https://github.com/mindsdb/mindsdb/assets/32901682/95416ef7-ddae-40ff-b215-8fa7b56a9b5c)


Results.

Was able to successfully create a ml_engine and model, as well as make single and batch predictions.

Drop a remark based on your observation.
- [x] Works Great 💚 (This means that all the steps were executed successfully and the expected outputs were returned.)
- [] There's a Bug 🪲 [Monkeylearn batch predictions give the same value #7033](https://github.com/mindsdb/mindsdb/issues/7033) 
