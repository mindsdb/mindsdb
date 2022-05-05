---
title: "MindsDB Profile"
---

## Overview of dbt-mindsdb
**Maintained by:** MindsDB
**Author:** MindsDB  
**Source:** [Github](https://github.com/mindsdb/dbt-mindsdb)       
<!-- **dbt Cloud:** Supported        
**dbt Slack channel** [Link to channel](https://getdbt.slack.com/archives/CJN7XRF1B)       -->


## Authentication Methods

### User / Password authentication

Currently MindsDB only supports user/password authentication, as shown below.

<File name='~/.dbt/profiles.yml'>

With local installation:

```yml
mindsdb:
  outputs:
    dev:
      type: mindsdb
      database: 'mindsdb'
      host: '127.0.0.1'
      port: 47335
      schema: 'mindsdb'
      password: ''
      username: 'mindsdb'
  target: dev
```

With MindsDB Cloud:

```yml
mindsdb:
  outputs:
    dev:
      type: mindsdb
      database: 'mindsdb'
      host: 'cloud.mindsdb.com'
      port: 47335
      schema: '[dbt schema]'
      username: '[mindsdb cloud username]'
      password: '[mindsdb cloud password]'
  target: dev
```

</File>