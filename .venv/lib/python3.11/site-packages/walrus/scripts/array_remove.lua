local key = KEYS[1]
local idx = tonumber(ARGV[1])
local arr_len = redis.call('HLEN', key)
if idx < 0 then
  idx = arr_len + idx
end
if idx < 0 or idx >= arr_len then
  return nil
end
local value = redis.call('HGET', key, idx)
local tmpval
while idx < arr_len do
  tmpval = redis.call('HGET', key, idx+1)
  if tmpval then
    redis.call('HSET', key, idx, tmpval)
  end
  idx = idx + 1
end
redis.call('HDEL', key, idx - 1)
return value
