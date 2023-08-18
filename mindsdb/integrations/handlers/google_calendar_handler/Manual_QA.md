# Welcome to the MindsDB Manual QA Testing for Google Calendar Handler


## Testing Google Calendar Handler with [Dataset Name](URL to the Dataset)

**1. Testing CREATE DATABASE**

```
CREATE
DATABASE my_calendar2
WITH  ENGINE = 'google_calendar',
parameters = {
    'credentials': '/home/marios/PycharmProjects/mindsdb/mindsdb/integrations/handlers/google_calendar_handler/creds.json'
};
```

[![google-calendar-create-db.png](https://i.postimg.cc/gJS2rP6Q/google-calendar-create-db.png)](https://postimg.cc/Q973nwmJ)

**2. Testing SELECT FROM Database**

```
SELECT *
FROM my_calendar.events;
```

[![google-calendar-select-method.png](https://i.postimg.cc/wTH9HvPM/google-calendar-select-method.png)](https://postimg.cc/fkHGfwMQ)

### Results

Drop a remark based on your observation.
- [ ] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
- [ ] There's a Bug 🪲 [Issue Title](URL To the Issue you created) ( This means you encountered a Bug. Please open an issue with all the relevant details with the Bug Issue Template)

---