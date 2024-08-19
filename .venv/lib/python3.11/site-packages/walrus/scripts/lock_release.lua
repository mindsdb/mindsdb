local key = KEYS[1]
local event_key = KEYS[2]
local lock_id = ARGV[1]
if redis.call("get", key) == lock_id then
  redis.call("lpush", event_key, 1)
  redis.call("ltrim", event_key, 0, 0) -- Trim all but the first item.
  return redis.call("del", key)
else
  return 0
end
