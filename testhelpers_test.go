package gobullmq

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

var testQueueCounter atomic.Int64

// testClient returns a Redis client for the integration test instance.
// Start one with: docker run -d --rm --name gobullmq-test-redis -p 6390:6379 redis:7-alpine
func testClient(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6390"
	}
	c := redis.NewClient(&redis.Options{Addr: addr})
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis not reachable at %s: %v (start with: docker run -d --rm -p 6390:6379 redis:7-alpine)", addr, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// testQueueName returns a unique queue name and registers cleanup of all its keys.
func testQueueName(t *testing.T, c *redis.Client) string {
	t.Helper()
	name := fmt.Sprintf("gbmqtest-%d-%d", time.Now().UnixNano(), testQueueCounter.Add(1))
	t.Cleanup(func() {
		ctx := context.Background()
		iter := c.Scan(ctx, 0, "bull:"+name+":*", 1000).Iterator()
		var keys []string
		for iter.Next(ctx) {
			keys = append(keys, iter.Val())
		}
		if len(keys) > 0 {
			_ = c.Del(ctx, keys...).Err()
		}
	})
	return name
}

// testQueue creates a Queue over map[string]any with automatic key cleanup.
func testQueue(t *testing.T, c *redis.Client) (*Queue[map[string]any], string) {
	t.Helper()
	name := testQueueName(t, c)
	q, err := NewQueue[map[string]any](name, c, nil)
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	return q, "bull:" + name + ":"
}
