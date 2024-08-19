local key = KEYS[1]
local nargs = #ARGV
local offset = redis.call('HLEN', key)
local idx = 0
while idx < nargs do
  redis.call('HSET', key, idx + offset, ARGV[idx + 1])
  idx = idx + 1
end
