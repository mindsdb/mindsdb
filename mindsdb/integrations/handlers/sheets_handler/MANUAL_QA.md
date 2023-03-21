# Welcome to the MindsDB Manual QA Testing for Sheets Handler

> **Please submit your PR in the following format after the underline below `Results` section. Don't forget to add an underline after adding your changes i.e., at the end of your `Results` section.**

## Testing Sheets Handler with [Job Placement Prediction](https://docs.google.com/spreadsheets/d/1YNC_dsngcJtpY_SGjCBvOujAocWhDbvlRfFshkPLOh8/edit#gid=1103325148)

**1. Testing CREATE DATABASE**

```
CREATE DATABASE job_placement_datasource       --- display name for the database
WITH ENGINE = 'sheets',                        --- name of the MindsDB handler
PARAMETERS = {
  "spreadsheet_id": "1YNC_dsngcJtpY_SGjCBvOujAocWhDbvlRfFshkPLOh8",          --- unique ID of the Google Sheet
  "sheet_name": "Job_Placement_Data"           --- name of the Google Sheet
};
```

![CREATE_DATABASE](https://user-images.githubusercontent.com/81156510/226104455-57979d0a-1d0e-4ebf-93a9-b11bbab79635.png)

**2. Testing CREATE PREDICTOR**

```
CREATE PREDICTOR mindsdb.job_placement_status_predictor
FROM job_placement_datasource (
    SELECT gender, ssc_percentage, ssc_board, hsc_percentage, hsc_board, 
    hsc_subject, degree_percentage, undergrad_degree, work_experience, 
    emp_test_percentage, specialisation, mba_percent, status
    FROM Job_Placement_Data
) PREDICT status;
```

![CREATE_PREDICTOR](https://user-images.githubusercontent.com/81156510/226104467-43716668-7063-4d20-835b-8045a0f7f144.png)

**3. Testing SELECT FROM PREDICTOR**

```
SELECT *
FROM mindsdb.predictors
WHERE name='job_placement_status_predictor';
```

![SELECT_FROM](https://user-images.githubusercontent.com/81156510/226104522-36cea5f5-675a-491e-b42e-99c63f26e609.png)

### Results

Drop a remark based on your observation.
- [x] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
- [ ] There's a Bug 🪲 [Issue Title](URL To the Issue you created) ( This means you encountered a Bug. Please open an issue with all the relevant details with the Bug Issue Template)

---