local key = KEYS[1]
local idx = tonumber(ARGV[1])
local arr_len = redis.call('HLEN', key)
if idx < 0 then
  idx = arr_len + idx
end
if idx < 0 or idx >= arr_len then
  return nil
end
return redis.call('HGET', key, idx)
