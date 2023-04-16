
```
SELECT *
FROM my_red.submission
WHERE subreddit = 'MachineLearning' AND sort_type = 'top' AND items = 5;
```

```
SELECT *
FROM my_red.comment
WHERE submission_id = '12gls93'
```