/**
 * @Description:
 * @FilePath: /bull-golang/internal/redisAction/init.go
 * @Author: liyibing liyibing@lixiang.com
 * @Date: 2023-07-26 10:13:03
 */
package redisAction

import (
	"context"
	"errors"
	"regexp"

	"github.com/redis/go-redis/v9"
)

var (
	ErrRedisInitFail = errors.New("redis init error")
	ErrWrongIP       = errors.New("wrong init IP")
	ErrWrongMode     = errors.New("wrong redis mode")
)

// func InitRedisClient(ip string, passwd string) (redis.Cmdable, error) {
// 	rdb := redis.NewClient(&redis.Options{
// 		Addr:     ip,
// 		Password: passwd,
// 		DB:       0,
// 	})
// 	_, err := rdb.Ping(context.Background()).Result()
// 	if err != nil {
// 		return nil, errors.New("redis init failed")
// 	}
// 	return rdb, nil
// }

func Init(ip string, passwd string, mode int) (redis.Cmdable, error) {
	regex := regexp.MustCompile(`([a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?)(\.[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?)*(:[0-9]{1,5})?`)
	address := regex.FindAllString(ip, -1)
	if len(address) < 1 {
		return nil, ErrWrongIP
	}
	mode = 0 // 当前只需要支持单节点模式
	if mode == 0 {
		return initSingleNode(address[0], passwd)
	} else if mode == 1 {
		return initCluster(address, passwd)
	}

	return nil, ErrWrongMode
}

func initSingleNode(ip string, passwd string) (*redis.Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     ip,
		Password: passwd,
		DB:       0,
	})
	_, err := rdb.Ping(context.Background()).Result()
	if err != nil {
		return nil, ErrRedisInitFail
	}

	return rdb, nil
}

func initCluster(ip []string, passwd string) (*redis.ClusterClient, error) {
	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    ip,
		Password: passwd,
	})
	_, err := rdb.Ping(context.Background()).Result()
	if err != nil {
		return nil, ErrRedisInitFail
	}

	return rdb, nil
}
