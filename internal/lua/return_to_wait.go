package lua

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// returnToWaitScript undoes the worker's blocking wait-list read (BLMOVE
// wait -> active) for an ID the worker will not process because it paused or
// closed as the read returned. The ID goes back to the end of the list it
// was popped from, or of the paused list if the queue has been paused since.
// BullMQ has no such script: upstream aborts its blocking read by dropping
// the connection, so it never sees what the read returned.
var returnToWaitScript = redis.NewScript(`
local activeKey = KEYS[1]
local waitKey = KEYS[2]
local pausedKey = KEYS[3]
local metaKey = KEYS[4]
local stalledKey = KEYS[5]
local id = ARGV[1]

if redis.call("LREM", activeKey, 1, id) == 0 then return 0 end
redis.call("SREM", stalledKey, id)
if redis.call("HEXISTS", metaKey, "paused") == 1 then
  redis.call("RPUSH", pausedKey, id)
else
  redis.call("RPUSH", waitKey, id)
end
return 1
`)

// ReturnToWait puts id back in the queue. It does nothing when id is no
// longer in active.
func ReturnToWait(ctx context.Context, client redis.Cmdable, keyPrefix, id string) error {
	keys := []string{
		keyPrefix + "active",
		keyPrefix + "wait",
		keyPrefix + "paused",
		keyPrefix + "meta",
		keyPrefix + "stalled",
	}
	if err := returnToWaitScript.Run(ctx, client, keys, id).Err(); err != nil {
		return fmt.Errorf("failed to return %s to the wait list: %w", id, err)
	}
	return nil
}
