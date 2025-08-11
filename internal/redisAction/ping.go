/**
 * @Description:
 * @FilePath: /bull-golang/internal/redisAction/ping.go
 * @Author: liyibing liyibing@lixiang.com
 * @Date: 2023-11-14 17:11:31
 */
package redisAction

import (
	"context"

	"github.com/redis/go-redis/v9"
)

func Ping(rdb redis.Cmdable) error {
	_, err := rdb.Ping(context.Background()).Result()
	return err
}
