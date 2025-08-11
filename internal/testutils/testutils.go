package testutils

import (
	"context"

	"github.com/redis/go-redis/v9"
)

func RemoveAllQueueData(redisClient *redis.Client, queueName string) error {
	// Get all keys with the queue name prefix
	keys, err := redisClient.Keys(context.Background(), queueName+"*").Result()
	if err != nil {
		return err
	}

	// Delete all keys
	for _, key := range keys {
		if _, err := redisClient.Del(context.Background(), key).Result(); err != nil {
			return err
		}
	}

	return nil
}
