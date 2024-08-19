-- Simple compare-and-swap using a prefixed match for the compare portion.
-- This makes it possible to do some basic fencing/etc.
local value_length = string.len(ARGV[1])
if redis.call('GETRANGE', KEYS[1], 0, value_length - 1) == ARGV[1] then
  redis.call('SET', KEYS[1], ARGV[2])
  return 1
else
  return 0
end
