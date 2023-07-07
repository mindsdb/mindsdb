# Preliminaries

1. Make sure your python environment has mindsdb and replicate installed.

# MindsDB example commands

1. Create an Image Generating AI Model using REPLICATE engine:
   
```sql
CREATE MODEL aiforever
PREDICT url
USING
    engine = 'replicate',
    model_name= 'ai-forever/kandinsky-2',
    version ='2af375da21c5b824a84e1c459f45b69a117ec8649c2aa974112d7cf1840fc0ce',
    api_key = 'r8_BpO.........................';
```

2. Now you can use DESCRIBE PREDICTOR query to check available parameters for our model:

```sql
DESCRIBE PREDICTOR mindsdb.aiforever.features;
```

3. Now you can use available parameters to make your predictions and customize it.
   
```sql 
SELECT *
FROM aiforever
WHERE prompt='Great warrior Arjun from Mahabharata with his bow and arrow , 4k quality'
```

### OUTPUT 
![GENERATED_IMAGES](https://replicate.delivery/pbxt/K9dMTBWgQg6cMB7Ekeo88PKAQG5N5lXIx0sVNdv4uf8ztJGRA/out_0.png)

