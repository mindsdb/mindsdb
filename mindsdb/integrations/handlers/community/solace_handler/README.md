# Solace Handler

## Run solace locally (optional):

Reference: https://solace.com/products/event-broker/software/getting-started/

1. Run solace app in docker
Here you can choose different user/password (instead of admin)
```bash
docker run -d -p 8080:8080 -p 55555:55555 -p 8008:8008 -p 1883:1883 -p 8000:8000 -p 5672:5672 -p 9000:9000 -p 2222:2222 --shm-size=2g --env username_admin_globalaccesslevel=admin --env username_admin_password=admin --name=solace solace/solace-pubsub-standard
```

2. Solace admin panel will be run on: http://localhost:8080/

3. Test Publisher/Subscriber using JS client in "3. Run Samples" on [get started](https://solace.com/products/event-broker/software/getting-started/) page 


## Client description

Pip module: https://pypi.org/project/solace-pubsubplus/

Client api reference: https://docs.solace.com/API-Developer-Online-Ref-Documentation/python/source/rst/solace.messaging.html

Client usage samples: https://github.com/SolaceSamples/solace-samples-python/tree/main


## Connect to the Mindsdb to Solace

Command parameters:
```sql
CREATE DATABASE my_solace
WITH
  ENGINE = "solace"
  PARAMETERS = {
    "host": <host and port, for example: tcp://localhost:55555>,
    "username": <username or email>,
    "password": <password>,
    "vpn-name": <solace vpn name> -- optional, default is "default"
  };
```

Example:
```sql
CREATE DATABASE my_solace
WITH
  ENGINE = "solace"
  PARAMETERS = {
    "host": "localhost:55555",
    "username": "admin",
    "password": "admin"
  };
```

## Usage

Solace is event broker. You can send event or subscribe on event. But can't read past events

### Publish event:

Table name is topic name for publishing.   
```sql
INSERT INTO my_solace.<topic_name> (<column>, ...)
VALUES (<value>, ...)
```

If topic contents slash then it can be replaced by '.' similar to access sql objects
For example to send event to topic `solace/test/my` with dict {'id': 1, 'name': 'me'} use 
```sql
INSERT INTO my_solace.solace.test.my ('id', 'name', 'type')
VALUES (1, 'me')
```


### Subscribe on event:

It is possible to subscribe to incoming events using triggers.
In this example events with topic `solace/test` will be inserted to my_pg.log table:
```sql
create trigger my_trigger 
on my_solace.solace.test
(
    insert into my_pg.log
    select * from TABLE_DELTA
) 
```
