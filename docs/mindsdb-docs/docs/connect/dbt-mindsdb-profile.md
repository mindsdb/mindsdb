# DBT-MindsDB Profile
## Overview of dbt-mindsdb
**Maintained by:** MindsDB
**Author:** MindsDB  
**Source:** [Github](https://github.com/mindsdb/dbt-mindsdb)
<!-- **dbt Cloud:** Supported        
**dbt Slack channel** [Link to channel](https://getdbt.slack.com/archives/CJN7XRF1B)       -->

## Authentication Methods

### User / Password authentication

Currently MindsDB only supports user/password authentication, as shown below on: `~/.dbt/profiles.yml`

=== "Self-Hosted Local Deployment"

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

=== "MindsDB Cloud"

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


| Key      | Required | Description                                          | Example                        |
| -------- | :--------: | ---------------------------------------------------- | ------------------------------ |
| `type`     |    ✔️   | The specific adapter to use                          | `mindsdb`                      |
| `host`     |    ✔️   | The MindsDB (hostname) to connect to                 | `cloud.mindsdb.com`            |
| `port`     |    ✔️   | The port to use                                      | `3306`  or `47335`             |
| `schema`   |    ✔️   | Specify the schema (database) to build models into   | The MindsDB datasource         |
| `username` |    ✔️   | The username to use to connect to the server         | `mindsdb` or mindsdb cloud user|
| `password` |    ✔️   | The password to use for authenticating to the server | `pass`                          |
