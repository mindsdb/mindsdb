local key = KEYS[1]
local limit_count = tonumber(ARGV[1])
local limit_time = tonumber(ARGV[2])
local current_time = tonumber(ARGV[3])

local rc = redis.call('LLEN', key)

local is_limited = 0
local oldest = 0

if rc < limit_count then
    redis.call('LPUSH', key, current_time)
else
    oldest = tonumber(redis.call('LINDEX', key, -1)) or 0
    if (current_time - oldest) < limit_time then
        is_limited = 1
    else
        redis.call('LPUSH', key, current_time)
    end
    redis.call('LTRIM', key, 0, limit_count - 1)
    redis.call('PEXPIRE', key, limit_count * 2000)
end
return is_limited
