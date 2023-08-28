## Testing GitHub Handler

1. Testing CREATE DATABASE

```
CREATE DATABASE mayank2130_github
WITH ENGINE = 'github',
PARAMETERS = {
  "repository": "mayank2130/mayank2130"
};
```

![CREATE_DATABASE]([Image URL of the screenshot](https://i.postimg.cc/GpYZW4vs/Create-Database.png))


2. Testing SELECT FROM DATABASE

```
SELECT * FROM mayank2130_github.pull_requests
```
![SELECT_FROM]([Image URL of the screenshot](https://i.postimg.cc/sXz7cLVZ/Testing-SELECT.png))


### Results

Drop a remark based on your observation.
- [X] Works Great 💚 (This means that all the steps were executed successfully and the expected outputs were returned.)
- [ ] There's a Bug 🪲 [Issue Title](URL To the Issue you created) (This means you encountered a Bug. Please open an issue with all the relevant details using the Bug Issue Template)
