-- Filter.
local zset_key = KEYS[1]
local dest_key = KEYS[2]
local low = ARGV[1]
local high = ARGV[2]
local values = redis.call('ZRANGEBYSCORE', zset_key, low, high)
if # values > 0 then
  for i, value in ipairs(values) do
    redis.call('SADD', dest_key, value)
  end
end
return # values
