# MindsDB SQL Editor

MindsDB provides a SQL Editor so you don't need to download additional SQL clients to connect to MindsDB.

## How to Use the MindsDB SQL Editor

There are two ways you can use the Editor, as below.

=== "Self-Hosted Local Deployment"

    After any of the Self-Hosted Setups - [Linux](/setup/self-hosted/pip/linux), [Windows](/setup/self-hosted/pip/windows), [MacOs](/setup/self-hosted/pip/macos) or directly from [source code](/setup/self-hosted/pip/source) - go to your terminal and execute: 

    ```bash
    python -m mindsdb 
    ```

    On execution, we get:

    ```bash
    ...
    2022-05-06 14:07:04,599 - INFO -  - GUI available at http://127.0.0.1:47334/
    ...
    ```

    Immediately after, your browser will automatically open the MindsDB SQL Editor. In case it doesn't, just visit the URL [`http://127.0.0.1:47334/`](http://127.0.0.1:47334/){:target="_blank"} in your browser of preference.

=== "MindsDB Cloud"

    1. Go to [MindsDB Cloud](https://cloud.mindsdb.com)

    2. Just log in to your account, and you will be automatically directed to the Editor.

Here is a sneak peek of the MindsDB Editor:

<figure markdown> 
    ![GUI](/assets/cloud/gui_query.png){ width="800", loading=lazy  }
    <figcaption></figcaption>
</figure>

## What's Next?

Now that you are all set, we recommend you check out our **Tutorials** and **Community Tutorials** sections, where you'll find various examples of regression, classification, and time series predictions with MindsDB.

To learn more about MindsDB itself, follow the guide on [MindsDB database structure](/sql/table-structure/). Also, don't miss out on the remaining pages from the **SQL API** section, as they explain a common SQL syntax with examples.

Have fun!
