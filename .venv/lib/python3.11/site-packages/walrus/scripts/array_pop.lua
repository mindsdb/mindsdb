local key = KEYS[1]
local last_idx = redis.call('HLEN', key) - 1
if last_idx < 0 then
  return nil
end
local value = redis.call('HGET', key, last_idx)
redis.call('HDEL', key, last_idx)
return value
