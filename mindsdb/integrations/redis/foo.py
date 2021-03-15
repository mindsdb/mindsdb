import redis

client = redis.client.Redis(host="127.0.0.1", port=6379, socket_connect_timeout=10)
print(client.echo("ping"))
print(client.dbsize())
