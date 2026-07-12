package lua

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// handleStalledParentScript applies the same parent policy used by
// moveToFinished after the upstream stalled-job script terminally fails a
// child. Cached child metadata is accepted because removeOnFail may already
// have deleted the child hash.
var handleStalledParentScript = redis.NewScript(`
local childKey = KEYS[1]
local timestamp = ARGV[1]
local rawOpts = ARGV[2]
local parentKey = ARGV[3]
local rawParent = ARGV[4]

if rawOpts == "" then rawOpts = redis.call("HGET", childKey, "opts") or "" end
if parentKey == "" then parentKey = redis.call("HGET", childKey, "parentKey") or "" end
if rawParent == "" then rawParent = redis.call("HGET", childKey, "parent") or "" end
if rawOpts == "" or parentKey == "" then return 0 end

local opts = cjson.decode(rawOpts)
if not opts['fpof'] and not opts['rdof'] then return 0 end

local function getJobId(jobKey)
  return string.match(jobKey, ".*:(.*)")
end

local function getPrefix(jobKey, jobId)
  return string.sub(jobKey, 1, #jobKey - #jobId)
end

local function targetList(prefix)
  if redis.call("HEXISTS", prefix .. "meta", "paused") == 1 then
    return prefix .. "paused", true
  end
  return prefix .. "wait", false
end

local function addPriorityMarker(waitKey)
  if redis.call("LLEN", waitKey) == 0 then redis.call("LPUSH", waitKey, "0:0") end
end

local function addWithPriority(prefix, parentId, priority, paused)
  local counter = redis.call("INCR", prefix .. "pc")
  local score = priority * 0x100000000 + bit.band(counter, 0xffffffffffff)
  redis.call("ZADD", prefix .. "prioritized", score, parentId)
  if not paused then addPriorityMarker(prefix .. "wait") end
end

local function addDelayMarker(target, delayedKey)
  if redis.call("LLEN", target) ~= 0 then return end
  local nextJob = redis.call("ZRANGE", delayedKey, 0, 0, "WITHSCORES")
  if #nextJob > 0 then redis.call("LPUSH", target, "0:" .. (tonumber(nextJob[2]) / 0x1000)) end
end

local function moveParentToWait(prefix, key, id)
  if redis.call("SCARD", key .. ":dependencies") ~= 0 then return end
  if redis.call("ZREM", prefix .. "waiting-children", id) ~= 1 then return end
  local target, paused = targetList(prefix)
  local attrs = redis.call("HMGET", key, "priority", "delay")
  local priority = tonumber(attrs[1]) or 0
  local delay = tonumber(attrs[2]) or 0
  if delay > 0 then
    local delayedAt = tonumber(timestamp) + delay
    local delayedKey = prefix .. "delayed"
    redis.call("ZADD", delayedKey, delayedAt * 0x1000, id)
    redis.call("XADD", prefix .. "events", "*", "event", "delayed", "jobId", id, "delay", delayedAt)
    addDelayMarker(target, delayedKey)
  else
    if priority == 0 then redis.call("RPUSH", target, id)
    else addWithPriority(prefix, id, priority, paused) end
    redis.call("XADD", prefix .. "events", "*", "event", "waiting", "jobId", id, "prev", "waiting-children")
  end
end

local failParent
failParent = function(prefix, key, id, failedChildKey)
  if redis.call("ZREM", prefix .. "waiting-children", id) ~= 1 then return end
  redis.call("ZADD", prefix .. "failed", timestamp, id)
  local reason = "child " .. failedChildKey .. " failed"
  redis.call("HMSET", key, "failedReason", reason, "finishedOn", timestamp)
  redis.call("XADD", prefix .. "events", "*", "event", "failed", "jobId", id, "failedReason", reason, "prev", "waiting-children")

  local grandParentJSON = redis.call("HGET", key, "parent")
  if not grandParentJSON then return end
  local grandParent = cjson.decode(grandParentJSON)
  local grandPrefix = grandParent['queueKey'] .. ":"
  local grandKey = grandPrefix .. grandParent['id']
  if grandParent['fpof'] then
    failParent(grandPrefix, grandKey, grandParent['id'], key)
  elseif grandParent['rdof'] then
    if redis.call("SREM", grandKey .. ":dependencies", key) == 1 then
      moveParentToWait(grandPrefix, grandKey, grandParent['id'])
    end
  end
end

local parentId = getJobId(parentKey)
local parentPrefix = getPrefix(parentKey, parentId)
if opts['fpof'] then
  failParent(parentPrefix, parentKey, parentId, childKey)
elseif opts['rdof'] then
  if redis.call("SREM", parentKey .. ":dependencies", childKey) == 1 then
    moveParentToWait(parentPrefix, parentKey, parentId)
  end
end
return 1
`)

func HandleStalledParent(ctx context.Context, client redis.Cmdable, childKey string, timestamp int64, optsJSON, parentKey, parentJSON string) error {
	result, err := handleStalledParentScript.Run(ctx, client, []string{childKey}, timestamp, optsJSON, parentKey, parentJSON).Result()
	if err != nil {
		return fmt.Errorf("failed to apply stalled child parent policy: %w", err)
	}
	if _, ok := result.(int64); !ok {
		return fmt.Errorf("unexpected stalled parent policy result %T", result)
	}
	return nil
}
