package lua

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// RemoveRepeatableCompat removes both current and pre-redesign Go repeat-job
// ID variants in the same Redis operation. The repeat registry mutation also
// invalidates schedulers watching that key, so removal cannot be resurrected by
// an already-running scheduling transaction.
var removeRepeatableCompatScript = redis.NewScript(`
local millis = redis.call("ZSCORE", KEYS[1], ARGV[3])
local function removeDelayed(prefix)
  local repeatJobId = prefix .. millis
  if redis.call("ZREM", KEYS[2], repeatJobId) == 1 then
    redis.call("DEL", ARGV[4] .. repeatJobId)
    redis.call("XADD", ARGV[4] .. "events", "*", "event", "removed", "jobId", repeatJobId, "prev", "delayed")
  end
end
if millis then
  removeDelayed(ARGV[1])
  if ARGV[2] ~= ARGV[1] then
    removeDelayed(ARGV[2])
  end
end
if redis.call("ZREM", KEYS[1], ARGV[3]) == 1 then
  return 0
end
return 1
`)

func RemoveRepeatableCompat(ctx context.Context, client redis.Cmdable, keys []string, currentPrefix, legacyPrefix, repeatKey, queuePrefix string) (any, error) {
	if len(keys) != 2 {
		return nil, fmt.Errorf("expected 2 keys but got %d", len(keys))
	}
	return removeRepeatableCompatScript.Run(ctx, client, keys, currentPrefix, legacyPrefix, repeatKey, queuePrefix).Result()
}
