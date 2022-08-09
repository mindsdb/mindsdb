# MindsDB and Kafka

MindsDB provides the Kafka connector plugin to connect to the Kafka cluster.

Please visit the [Kafka Connect Mindsdb](https://www.confluent.io/hub/mindsdb/mindsdb-kafka-connector) page on the official Confluent site. It contains all the instructions on how to install the connector from the Confluent hub.

Let's review the instructions here as well.

You may use official connector docker image:

```bash
docker pull mindsdb/mindsdb-kafka-connector
```

The source code of the Kafka connector is located [here](https://github.com/mindsdb/kafka_connector). Please read the [instruction](https://github.com/mindsdb/kafka_connector/blob/main/README.md) before building the connector from scratch.

## Inside the Test Docker Environment

It is possible to integrate and use the Kafka connector as part of your own Kafka cluster. Or you may try out our [test docker environment](https://github.com/mindsdb/kafka_connector/blob/main/docker-compose.yml).

Before you bring the docker container up, please note that there are two types of connector configuration:

 - For [MindsDB Cloud](https://github.com/mindsdb/kafka_connector/blob/main/examples/kafkaConfig.json)
 - For a separate [MindsDB installation](https://github.com/mindsdb/kafka_connector/blob/main/examples/kafkaConfigSeparateMindsdbInstance.json)

 Depending on which option you choose, these files require real values to be set in place of username, password, Kafka connection details, etc. The SASL mechanism details are optional, as local Kafka installation may not have this mechanism configured - or you can use [this data](https://github.com/mindsdb/kafka_connector/blob/main/kafka_server_jaas.conf#L11,L12) for the SASL username and password.

 Now that your config files store real data, you can execute the command below from the root of the [Kafka connector repository](https://github.com/mindsdb/kafka_connector) to build the connector and launch it in the test environment locally.

```bash
docker-compose up -d
```

Let's go over some examples.

## Example

### Prerequisites:
- Launch MindsDB instance where HTTP API interface is running on `docker network interface inet ip` - the IP address is usually `172.17.0.1`.

    You can modify the HTTP API interface details in the [MindsDB config file](https://github.com/mindsdb/mindsdb/blob/staging/mindsdb/utilities/config.py#L50,L52).

    Now, to launch your MindsDB, run the following command:

    ```bash
    python -m mindsdb --config=YOURCONFIGFILE
    ```

- Train a new model. You may use [this tutorial](/sql/tutorials/bodyfat) as an example.

- Run the test environment as instructed in the *Inside the Test Docker Environment* section above.

### Create the connector instance:
To create a connector need to send POST request to specific `connectors` url with connector configuration in JSON format:
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

The code above creates a MindsDB Kafka connector which uses `PREDICTOR_NAME` model, receives source data from `STREAM_IN` kafka topic and sends prediction results to `STREAM_OUT` kafka topic.

### Send source data and receive prediction results:
There are many kafka clients <ins>[implementations](https://docs.confluent.io/platform/current/clients/index.html)</ins>, and you may find the best one for your goals.

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

This piece of code generates and sends for source records to `topic_in` (by default) or any other topic (if it is provided and cmd parameter).

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

While this one shows how to read prediction results from `topic_out` (by default) or any other topic (if it is provided and cmd parameter).
