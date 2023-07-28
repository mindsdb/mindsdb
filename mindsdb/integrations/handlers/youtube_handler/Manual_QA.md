# Welcome to the MindsDB Manual QA Testing for YouTube Handler

> **Please submit your PR in the following format after the underline below `Results` section.
> Don't forget to add an underline after adding your changes i.e., at the end of your `Results` section.**

## Testing YouTube Handler

**1. Testing CREATE DATABASE**

```sql
CREATE DATABASE mindsdb_youtube_handler
WITH ENGINE = 'youtube',
PARAMETERS = {
  "youtube_api_token": "AIzaSyCl_5mEdSA7FZ4vY--lN2EiPx8rgah5yPQ"  
};
```

![CREATE_DATABASE](https://github.com/mindsdb/mindsdb/assets/75406794/94e12cbf-70af-4727-b0ec-533b0cb9b051)

**2. Testing SELECT**

```sql
SELECT * FROM mindsdb_youtube_handler.get_comments
WHERE youtube_video_id = "gWIaQko8TnE";
```

![SELECT](https://github.com/mindsdb/mindsdb/assets/75406794/bf555b77-babc-4f7d-838b-0dc9f27ceb07)

### Results

Drop a remark based on your observation.
- [x] Works Great 💚 (This means that all the steps were executed successfully and the expected outputs were returned.)
- [ ] There's a Bug 🪲 [Issue Title](URL To the Issue you created) ( This means you encountered a Bug.
Please open an issue with all the relevant details with the Bug Issue Template)

---