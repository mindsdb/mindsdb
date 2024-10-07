---
title: Symbl
sidebarTitle: Symbl
---

This documentation describes the integration of MindsDB with [Symbl](https://symbl.ai/), a platform with state-of-the-art and task-specific LLMs that enables businesses to analyze multi-party conversations at scale.
This integration allows MindsDB to process conversation data and extract insights from it.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To connect Symbl to MindsDB, install the required dependencies following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).

<Tip>
Please note that in order to successfully install the dependencies for Symbl, it is necessary to install `portaudio` and few other Linux packages in the Docker container first. To do this, run the following commands:

1. Start an interactive shell in the container:
```bash
docker exec -it mindsdb_container sh
```
If you haven't specified a name when spinning up the MindsDB container with `docker run`, you can find it by running `docker ps`.

<Note>
If you are using Docker Desktop, you can navigate to 'Containers', locate the multi-container application running the extension, click on the `mindsdb_service` container and then click on the 'Exec' tab to start an interactive shell.
</Note>

2. Install the required packages:
```bash
apt-get update && apt-get install -y \
        libportaudio2 libportaudiocpp0 portaudio19-dev \
        python3-dev \
        build-essential \
        && rm -rf /var/lib/apt/lists/*
``` 
</Tip>

## Connection

Establish a connection to your Symbl from MindsDB by executing the following SQL command:

```sql
CREATE DATABASE mindsdb_symbl
WITH ENGINE = 'symbl',
PARAMETERS = {
    "app_id": "app_id",
    "app_secret":"app_secret"
};
```

Required connection parameters include the following:

* `app_id`: The Symbl app identifier.
* `app_secret`: The Symbl app secret.

## Usage

First, process the conversation data and get the conversation ID via the `get_conversation_id` table:

```sql
SELECT * 
FROM mindsdb_symbl.get_conversation_id
WHERE audio_url="https://symbltestdata.s3.us-east-2.amazonaws.com/newPhonecall.mp3";
```

Next, use the conversation ID to get the results of the above from the other supported tables:

```sql
SELECT *
FROM mindsdb_symbl.get_messages
WHERE conversation_id="5682305049034752";
```

Other supported tables include:

* `get_topics`
* `get_questions`
* `get_analytics`
* `get_action_items`

<Note>
The above examples utilize `mindsdb_symbl` as the datasource name, which is defined in the `CREATE DATABASE` command.
</Note>