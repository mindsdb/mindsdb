# MindsDB and Kafka

MindsDB provides the Kafka connector plugin to connect to the Kafka cluster.

Please visit the [Kafka Connect MindsDB page](https://www.confluent.io/hub/mindsdb/mindsdb-kafka-connector) on the official Confluent site. It contains all the instructions on how to install the connector from the Confluent hub.

## Setting up Kafka in Docker

Let's review the instructions here as well.

You may use the official connector docker image:

```bash
docker pull mindsdb/mindsdb-kafka-connector
```

The source code of the Kafka connector is in the [kafka_connector GitHub repository](https://github.com/mindsdb/kafka_connector). Please read the [instructions](https://github.com/mindsdb/kafka_connector/blob/main/README.md) before building the connector from scratch.

It is possible to integrate and use the Kafka connector as part of your own Kafka cluster, or you may use our test docker environment.

To try out our [test docker environment](https://github.com/mindsdb/kafka_connector/blob/main/docker-compose.yml), follow the steps below.

Before you bring the docker container up, please note that there are two types of connector configuration:

 - For [MindsDB Cloud](https://github.com/mindsdb/kafka_connector/blob/main/examples/kafkaConfig.json)
 - For a separate [MindsDB installation](https://github.com/mindsdb/kafka_connector/blob/main/examples/kafkaConfigSeparateMindsdbInstance.json)

 No matter which option you choose, these files require real values to be set in place of a username, password, Kafka connection details, etc. The SASL mechanism details are optional, as local Kafka installation may not have this mechanism configured - or you can use [this data](https://github.com/mindsdb/kafka_connector/blob/main/kafka_server_jaas.conf#L11,L12) for the SASL username and password.

 Now that your config files store real data, you can execute the command below from the root of the [kafka_connector GitHub repository](https://github.com/mindsdb/kafka_connector) to build the connector and launch it in the test environment locally.

```bash
docker-compose up -d
```

Let's go over some examples.

## Examples

### Prerequisites

- Launch MindsDB instance where HTTP API interface runs on `docker network interface inet ip`. Usually, the IP address is `172.17.0.1`, and the port is `47334`.

    You can modify the HTTP API interface details in the [MindsDB config file](https://github.com/mindsdb/mindsdb/blob/staging/mindsdb/utilities/config.py#L50,L52).

    Now, to launch your MindsDB, run the following command:

    ```bash
    python -m mindsdb --config=YOURCONFIGFILE
    ```

- Train a new model. You may use [this tutorial](/sql/tutorials/bodyfat) as an example.

- Run the test environment as instructed in the [Setting up Kafka in Docker](#setting-up-kafka-in-docker) section above.

### Create the Connector Instance

To create a connector, you need to send a POST request to a specific `connectors` URL with connector configuration in JSON format, as below.

```python
import requests

MINDSDB_URL = "http://172.17.0.1:47334"
CONNECTOR_NAME = "MindsDBConnector"
INTEGRATION_NAME = 'test_kafka'
KAFKA_PORT = 9092
KAFKA_HOST = "127.0.0.1"

CONNECTOR_NAME = "MindsDBConnector"
CONNECTORS_URL = "http://127.0.0.1:9021/api/connect/connect-default/connectors"
STREAM_IN = "topic_in"
STREAM_OUT = "topic_out"

PREDICTOR_NAME = "YOUR_MODEL_NAME" # set actual model name here

params = {"name": CONNECTOR_NAME,
          "config": {"connector.class": "com.mindsdb.kafka.connect.MindsDBConnector",
                     "topics": STREAM_IN,
                     "mindsdb.url": MINDSDB_URL,
                     "kafka.api.host": KAFKA_HOST,
                     "kafka.api.port": KAFKA_PORT,
                     "kafka.api.name": INTEGRATION_NAME,
                     "predictor.name": PREDICTOR_NAME,
                     "output.forecast.topic": STREAM_OUT,
                     "security.protocol": "SASL_PLAINTEXT",
                     "sasl.mechanism": "PLAIN",
                     "sasl.plain.username": "admin",
                     "sasl.plain.password": "admin-secret",
                     }
          }

headers = {"Content-Type": "application/json"}
res = requests.post(CONNECTORS_URL, json=params, headers=headers)
```

This code creates a MindsDB Kafka connector that uses the `PREDICTOR_NAME` model, receives source data from the `STREAM_IN` Kafka topic, and sends prediction results to the `STREAM_OUT` Kafka topic.

### Send Source Data and Receive Prediction Results

There are many [Kafka client implementations](https://docs.confluent.io/platform/current/clients/index.html) - choose the most suitable one depending on your goals.

The code below generates and sends the source records to `topic_in` by default. You can use any other Kafka topic by providing its name as a CMD parameter.

```python
import sys                                                                                                                                                  
import json
import kafka
connection_info = {"bootstrap_servers": "127.0.0.1:9092",
                   "security_protocol": "SASL_PLAINTEXT",
                   "sasl_mechanism": "PLAIN",
                   "sasl_plain_username": "admin",
                   "sasl_plain_password": "admin-secret"}
producer = kafka.KafkaProducer(**connection_info)
if __name__ == '__main__':
    print(json.dumps(connection_info))
    if len(sys.argv) == 1:
        topic = "topic_in"
    else:
        topic = sys.argv[1]
    for x in range(1, 4):
        data = {"Age": x+20, "Weight": x * x * 0.8 + 200, "Height": x * x * 0.5 + 65}
        to_send = json.dumps(data)
        producer.send(topic, to_send.encode('utf-8'))
    producer.close()
```

And the following code shows how to read prediction results from `topic_out` by default. Again, you can use any other Kafka topic by providing its name as a CMD parameter.

```python
import sys
import json
import kafka

connection_info = {"bootstrap_servers": "127.0.0.1:9092",
                   "security_protocol": "SASL_PLAINTEXT",
                   "sasl_mechanism": "PLAIN",
                   "sasl_plain_username": "admin",
                   "sasl_plain_password": "admin-secret",
                   "auto_offset_reset": 'earliest',
                   "consumer_timeout_ms": 1000}

consumer = kafka.KafkaConsumer(**connection_info)

if __name__ == '__main__':
    print(json.dumps(connection_info))
    if len(sys.argv) == 1:
        topic = "topic_out"
    else:
        topic = sys.argv[1]
    consumer.subscribe(topics=[topic])
    for msg in consumer:
        print(msg.value)
        print("-" * 100)
```

!!! tip "What's next?"
    We recommend you to follow one of our tutorials or learn more about the [MindsDB Database](/sql/table-structure/).
