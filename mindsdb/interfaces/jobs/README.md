
## Create job

**Syntax**
```sql
CREATE JOB [<project_name>.]<job name> [AS] (
   <mindsdb sql query>[; <mindsdb sql query>]
)
START { now | <date> }
END { now | <date> }
EVERY [<numer>] <period> 
```
Parameters:
- START - optional, first date when job have to start. Default is now
- END - optional, after this date job shouldn't run anymore. If not set - repeat forever
- EVERY - optional job repeat rule.
  - If not set - run once (no matter what was set in END)
  - if <number> is not set - default value is 1 

Supported periods:
- minute/minutes/min
- hour/hours
- day/days
- week/weeks
- month/months

Supported date formats:
- '%Y-%m-%d %H:%M:%S'
- '%Y-%m-%d'

**Examples**:
1:
```sql
create job proj1.retrain_rentals (
   retrain mindsdb.home_rentals_model
   using
      join_learn_process=true;

   insert into int1.rentals(
      select * from int2.table1
      join mindsdb.home_rentals_model
   )
) 
end '2023-04-01 00:00:00'
every 2 days
```

This example creates job in 'proj1' project and runs it every 2 days till 1st April. 

This job have to
- retrain predictor 
- use trained predictor on records from table 'int2.table1'
- append results of prediction to int1.rentals table

join_learn_process=true - this parameter is used for waiting to retrain predictor
(not run next command from job until retrain completed). 

This parameter can also be useful when we need to prevent to run several retrains simultaneously
(if retrain may not finish till next run of job) 

2:
```sql
create job slice_predictions (
   insert into int1.`result_{{START_DATE}}`(
      select * from int2.table1
      join mindsdb.home_rentals_model2
   )
)
every hour
```
This examples create job in current project and runs it every hour. 
This job on every run will create a new table int1.result_yyy-mm-dd
and fill it with result of prediction.

For creating different names for tables template variable START_DATE is used 

3: 
```sql
create job becalm (
   drop model mindsdb.home_rentals_model
) 
start '2023-04-01'
```
Creates one time run job and run it once on 1st April

**Template variables:**
Supported variables at the moment: 
- PREVIOUS_START_DATETIME - replaced by date and time of previous run of this job 
- START_DATE - replaced by date of job run
- START_DATETIME- replaced by datetime of job run

## Delete job

**Syntax**
```sql
DROP JOB [<project_name>.]<job name>
```
**Examples**
```sql
drop job proj1.j1 -- drop in 'proj1' project
 
drop job j1 -- drop in current project
```

## Querying jobs:

```sql
select * from jobs -- view jobs in current project

select * from proj2.jobs -- view jobs in 'proj2' project 

select * from information_schema.jobs -- view all jobs from all projects 
```

Result columns: NAME, PROJECT, START_AT, END_AT, NEXT_RUN_AT, SCHEDULE_STR, QUERY
- START_AT, END_AT - calculated start and end time.
- NEXT_RUN_AT - datetime when job will be executed next time 
- SCHEDULE_STR - contents repeat rules
- QUERY - mindsdb sql that is going to be called on every run

If job was executed last time (and not going to be executed more) it is deleted from jobs 

## Querying jobs history:

```sql
select * from jobs_history -- in current project

select * from proj2.jobs_history  -- in 'proj2' project 

select * from information_schema.jobs_history  -- all jobs from all projects 
```

Result columns: NAME, PROJECT, RUN_START, RUN_END, ERROR, QUERY
- RUN_START - when current execution starts
- RUN_END - when current execution ends
- ERROR - information about error if execution was failed  

## config

```
    "jobs": {
        "disable": false,
        "check_interval": 30
    }
```

1. disable: scheduler activity. By default, scheduled is always starting with start of mindsdb. 
To disable scheduler need to set it to false
2. check_interval: interval in seconds to check schedule table. Default is 30 sec 

## Technical information

### Tables

**Jobs table:**
Contents information about current periodic tasks

Columns:
- name - name of the job defined by user
- company_id, user_class - information about user
- active - if not - job is not run
- project_id - link to project, job with the same name can exist in different projects of the user
- query_str - string of the query to execute inside the job
- start_at - if specified, the date and time of first run of the job 
- end_at - if specified, the date and time after which job mustn't be run
- next_run_at - calculated date of next run, it is updated after every running
- schedule_str - schedule rules in format 'EVERY [<number>] <time period>'
  - time period can be 'minute', 'min', 'day', 'month' and others
  - examples: 'every hour', 'every 2 hours'

**Jobs_History table:**
Contents information about executed jobs. 

Columns:
- company_id - the same as in jobs.company_id
- job_id - link to jobs.id
- query_str - rendered query that was actually executed
- start_at and end_at - when execution started and finished
- error - in case of error during execution will contend its text
- created_at - equal next_run_at if the job.
- updated_at - this column updates during job execution to indicate that execution is in progress

Table has unique constraint: job_id and start_at 

### Work scheme

Mindsdb node runs jobs scheduler process.
This process:
- checks jobs table every X seconds
- picks all jobs with next_run_at is in the past.
- tries to lock it
  - creates history record with next_run_at time
  - because jobs_history table has the constraint on job_id, start_at: only one mindsdb node will be able to create such record
    - this node will execute this task  
    - other nodes will skip it
- after execution of task the new next_run_at will be calculated and stored

**Better implementation of jobs for microservice cloud:**
- having one scheduler service (only one process running at the time) which will check jobs table and if time to execute the job - put record in queue for job execution
- having multiple workers of executor which pick tasks from queue and execute them

**Long tasks monitoring**
Jobs scheduler has special thread to execute tasks in it. Main thread updates the updated_at column of jobs_history table
- it is needed for understand if task is still executed or something went wrong, and we need to run this job again



