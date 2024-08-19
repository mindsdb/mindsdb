local key = KEYS[1]
local lock_id = ARGV[1]
local ttl = tonumber(ARGV[2])
local ret = redis.call('setnx', key, lock_id)
if ret == 1 and ttl > 0 then
  redis.call('pexpire', key, ttl)
end
return ret
