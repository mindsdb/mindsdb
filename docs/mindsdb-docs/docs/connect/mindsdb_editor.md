# MindsDB SQL Editor

MindsDB provides a SQL Editor so you don't need to download additional SQL clients to connect to MindsDB. There are two ways you can use the Editor:

=== "Self-Hosted Local Deployment"

    After any of the Self-Hosted Setups - [Linux](/setup/self-hosted/pip/linux), [Windows](/setup/self-hosted/pip/windows), [MacOs](/setup/self-hosted/pip/macos) or directly from [source code](/setup/self-hosted/pip/source) go to you terminal and execute: 

    ```bash
      python -m mindsdb 
    ```

    On execution, you should get:

    ```bash
    ...
    2022-05-06 14:07:04,599 - INFO -  - GUI available at http://127.0.0.1:47334/
    ...
    ```

    Immediately after, your browser will automatically open the MindsDB SQL Editor. In case it doesn't, just visit the URL ```http://127.0.0.1:47334/``` in your browser of preference. 

=== "MindsDB Cloud"

    1. Go to [MindsDB Cloud](https://cloud.mindsdb.com )

    2. Just log in to your account, and you will be automatically directed to the Editor.

<figure markdown> 
    ![GUI](/assets/cloud/gui_query.png){ width="800", loading=lazy  }
    <figcaption></figcaption>
</figure>

!!! tip "What is next?"
    We recommend you to follow one of our tutorials or jump more into detail understanding the [MindsDB Database](/sql/description/mindsdb_database)
