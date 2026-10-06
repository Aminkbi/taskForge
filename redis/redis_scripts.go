package redis

import (
	"github.com/redis/go-redis/v9"
)

var (
	publishReadyTaskScript = redis.NewScript(`
if tonumber(ARGV[3]) > 0 and redis.call("EXISTS", KEYS[3]) == 1 then
  return 0
end
if ARGV[4] ~= "" then
  redis.call("SADD", KEYS[4], ARGV[4])
end
redis.call("XADD", KEYS[1], "*", ARGV[1], ARGV[2])
if ARGV[4] ~= "" then
  redis.call("LPUSH", KEYS[5], ARGV[5])
  redis.call("LTRIM", KEYS[5], 0, 0)
end
local fieldCount = tonumber(ARGV[6])
if fieldCount > 0 then
  local fields = {}
  for index = 1, fieldCount * 2 do
    fields[index] = ARGV[6 + index]
  end
  redis.call("HSET", KEYS[2], unpack(fields))
  redis.call("PERSIST", KEYS[2])
end
if tonumber(ARGV[3]) > 0 then
  redis.call("PSETEX", KEYS[3], ARGV[3], "1")
end
return 1
`)
	publishDelayedScript = redis.NewScript(`
redis.call("ZADD", KEYS[1], ARGV[1], ARGV[2])
local head = redis.call("ZRANGE", KEYS[1], 0, 0, "WITHSCORES")
if head[1] then
  redis.call("ZADD", KEYS[2], head[2], ARGV[3])
else
  redis.call("ZREM", KEYS[2], ARGV[3])
end
if ARGV[5] == "1" then
  redis.call("ZADD", KEYS[3], ARGV[1], ARGV[4])
end
local fieldCount = tonumber(ARGV[6]) or 0
if fieldCount > 0 then
  local fields = {}
  local position = 7
  for index = 1, fieldCount * 2 do
    fields[index] = ARGV[position]
    position = position + 1
  end
  redis.call("HSET", KEYS[4], unpack(fields))
  redis.call("PERSIST", KEYS[4])
end
return 1
`)
	publishDelayedWithReceiptScript = redis.NewScript(`
if redis.call("EXISTS", KEYS[2]) == 1 then
  return 0
end
redis.call("ZADD", KEYS[1], ARGV[1], ARGV[2])
local head = redis.call("ZRANGE", KEYS[1], 0, 0, "WITHSCORES")
if head[1] then
  redis.call("ZADD", KEYS[3], head[2], ARGV[4])
else
  redis.call("ZREM", KEYS[3], ARGV[4])
end
if ARGV[6] == "1" then
  redis.call("ZADD", KEYS[4], ARGV[1], ARGV[5])
end
redis.call("PSETEX", KEYS[2], ARGV[3], "1")
local fieldCount = tonumber(ARGV[7]) or 0
if fieldCount > 0 then
  local fields = {}
  local position = 8
  for index = 1, fieldCount * 2 do
    fields[index] = ARGV[position]
    position = position + 1
  end
  redis.call("HSET", KEYS[5], unpack(fields))
  redis.call("PERSIST", KEYS[5])
end
return 1
`)
	finalizeDeliveryScript = redis.NewScript(`
local pending = redis.call("XPENDING", KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
if not pending[1] or pending[1][1] ~= ARGV[2] then
  return {0, "", 0}
end
local owner = pending[1][2]
local idle = tonumber(pending[1][3]) or 0
if ARGV[3] ~= "" and owner ~= ARGV[3] then
  return {2, owner, idle}
end
local ttl = tonumber(ARGV[4]) or 0
if ttl > 0 and idle >= ttl then
  return {3, owner, idle}
end
local acked = redis.call("XACK", KEYS[1], ARGV[1], ARGV[2])
local deleted = redis.call("XDEL", KEYS[1], ARGV[2])
if acked ~= 1 or deleted ~= 1 then
  return {0, owner, idle}
end
pcall(function() redis.call("ZREM", KEYS[2], ARGV[2]) end)
if KEYS[3] then
  local stateTTL = tonumber(ARGV[5]) or -1
  local fieldCount = tonumber(ARGV[6]) or 0
  local fields = {}
  local position = 7
  for index = 1, fieldCount * 2 do
    fields[index] = ARGV[position]
    position = position + 1
  end
  if #fields > 0 then
    redis.call("HSET", KEYS[3], unpack(fields))
  end
  if stateTTL > 0 then
    redis.call("PEXPIRE", KEYS[3], stateTTL)
  else
    redis.call("PERSIST", KEYS[3])
  end
end
return {1, owner, idle}
`)
	fencedRenewLeasesScript = redis.NewScript(`
local results = {}
local redisTime = redis.call("TIME")
local nowMillis = tonumber(redisTime[1]) * 1000 + math.floor(tonumber(redisTime[2]) / 1000)
for index = 2, #ARGV, 3 do
  local id = ARGV[index]
  local expectedOwner = ARGV[index + 1]
  local ttl = tonumber(ARGV[index + 2]) or 0
  local deadline = nowMillis + ttl
  local pending = redis.call("XPENDING", KEYS[1], ARGV[1], id, id, 1)
  if not pending[1] or pending[1][1] ~= id then
    results[#results + 1] = 0
  elseif expectedOwner ~= "" and pending[1][2] ~= expectedOwner then
    results[#results + 1] = 2
  elseif ttl > 0 and tonumber(pending[1][3]) >= ttl then
    results[#results + 1] = 3
  else
    local claimed = redis.call("XCLAIM", KEYS[1], ARGV[1], expectedOwner, 0, id, "JUSTID")
    if #claimed == 1 then
      pcall(function() redis.call("ZADD", KEYS[2], deadline, id) end)
      results[#results + 1] = 1
    else
      results[#results + 1] = 0
    end
  end
end
return results
`)
	fencedReleaseReadyScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
  return {-1, 0}
end
local existed = redis.call("EXISTS", KEYS[4])
if existed == 0 then
  redis.call("XADD", KEYS[3], "*", ARGV[3], ARGV[4])
  redis.call("PSETEX", KEYS[4], ARGV[5], "1")
end
local removed = redis.call("ZREM", KEYS[2], ARGV[2])
redis.call("ZREM", KEYS[6], ARGV[7])
local head = redis.call("ZRANGE", KEYS[2], 0, 0, "WITHSCORES")
if head[1] then
  redis.call("ZADD", KEYS[5], head[2], ARGV[6])
else
  redis.call("ZREM", KEYS[5], ARGV[6])
end
return {existed, removed}
`)
	fencedReleaseFairReadyScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
  return {-1, 0}
end
local existed = redis.call("EXISTS", KEYS[6])
if existed == 0 then
  redis.call("SADD", KEYS[3], ARGV[3])
  redis.call("XADD", KEYS[4], "*", ARGV[4], ARGV[5])
  redis.call("LPUSH", KEYS[5], ARGV[6])
  redis.call("LTRIM", KEYS[5], 0, 0)
  redis.call("PSETEX", KEYS[6], ARGV[7], "1")
end
local removed = redis.call("ZREM", KEYS[2], ARGV[2])
redis.call("ZREM", KEYS[8], ARGV[9])
local head = redis.call("ZRANGE", KEYS[2], 0, 0, "WITHSCORES")
if head[1] then
  redis.call("ZADD", KEYS[7], head[2], ARGV[8])
else
  redis.call("ZREM", KEYS[7], ARGV[8])
end
return {existed, removed}
`)
	fencedReleaseDelayedScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
  return {-1, 0}
end
local existed = redis.call("EXISTS", KEYS[4])
if existed == 0 then
  redis.call("ZADD", KEYS[3], ARGV[3], ARGV[4])
  if ARGV[10] == "1" then
    redis.call("ZADD", KEYS[7], ARGV[3], ARGV[9])
  end
  redis.call("PSETEX", KEYS[4], ARGV[5], "1")
end
local removed = redis.call("ZREM", KEYS[2], ARGV[2])
redis.call("ZREM", KEYS[6], ARGV[8])
local oldHead = redis.call("ZRANGE", KEYS[2], 0, 0, "WITHSCORES")
if oldHead[1] then
  redis.call("ZADD", KEYS[5], oldHead[2], ARGV[6])
else
  redis.call("ZREM", KEYS[5], ARGV[6])
end
local newHead = redis.call("ZRANGE", KEYS[3], 0, 0, "WITHSCORES")
if newHead[1] then
  redis.call("ZADD", KEYS[5], newHead[2], ARGV[7])
else
  redis.call("ZREM", KEYS[5], ARGV[7])
end
return {existed, removed}
`)
)
