local key = KEYS[1]
local value = ARGV[1]
local max_idx = redis.call('HLEN', key)
redis.call('HSET', key, max_idx, value)
