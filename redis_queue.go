package queue

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-framework/config/redis"
)

const (
	TimestampSeparator = ":@"
)

// use redis list store queue data, redis BRPopLPush for safe pop data, when failed
// use ReturnElements function return back private key element to key. it can use for
// distributed queue.
type RedisQueue struct {
	// queue private key, format is [KEY][TimestampSeparator][timestamp] then match string is [KEY*][TimestampSeparator][timestamp].
	privateKey string
	// redis key.
	key string
	// redis client.
	client *redis.Client
}

// Return the private key to the queue key greater than interval second.
// first get all elements back , after get self private elements back.
func (this *RedisQueue) ReturnElements(interval int64) error {
	for {
		// scan the keys match queue key.
		match := this.key + TimestampSeparator + "*"
		keys, cursor, err := this.client.Scan(0, match, 100).Result()
		if err != nil {
			return err
		}

		// no key break.
		if len(keys) == 0 {
			break
		}
		// range keys to return the matched elements to queue key.
		for _, key := range keys {
			// split key and get the timestamp
			// format is [KEY][TimestampSeparator][timestamp].
			splits := strings.Split(key, TimestampSeparator)
			timestamp, err := strconv.ParseInt(splits[len(splits)-1], 10, 64)
			if err != nil {
				continue
			}

			// just return the key greater than interval.
			if time.Now().Unix()-timestamp < interval && interval > 0 {
				continue
			}

			// pop and push key into queue key.
			err = this.client.RPopLPush(key, this.key).Err()
			if err != nil {
				continue
			}
		}

		// get the cursor is 0 break loop.
		if cursor == 0 {
			break
		}
	}

	return nil
}

// Init queue with context, use redis NewContext function to set redis config.
// will call ReturnElements at least.
func (this *RedisQueue) Init(ctx context.Context) error {
	if this.client == nil {
		// use redis default config.
		config := redis.DefaultRedisConfig

		if value, ok := redis.FromContext(ctx); ok {
			config = value
		}

		this.client = config.NewGoClient()
	}

	return this.ReturnElements(0)
}

// Push any data into the queue.
func (this *RedisQueue) Push(data interface{}) error {
	// inserting data at the end of the redis list.
	return this.client.LPush(this.key, data).Err()
}

// Pop first element with timeout, pop from queue key into the queue private key,
// when finish handle the Pop element please call the Remove function to remove.
// default duration is 1 second, context can carry Duration context and DoFunction context.
func (this *RedisQueue) Pop(ctx context.Context) (interface{}, error) {
	// get duration from context.
	duration, ok := DurationFromContext(ctx)
	if !ok {
		duration = time.Second
	}
	// do function if context exist DoFuncHandler.
	if f, ok := DoFunctionFromContext(ctx); ok {
		f()
	}
	// pop up the first element of the list with duration block.
	return this.client.BRPopLPush(this.key, this.privateKey, duration).Result()
}

// Remove the last element of private key.
func (this *RedisQueue) Remove(data interface{}) error {
	// pop up the first element of the list.
	return this.client.LRem(this.privateKey, -1, data).Err()
}

// New redis queue with key name,
// use linux timestamp for distinguish distributed read and write by privateKey.
func NewRedisQueue(key string) *RedisQueue {
	q := &RedisQueue{
		// format is [KEY][TimestampSeparator][timestamp].
		privateKey: fmt.Sprintf("%s%s%d", key, TimestampSeparator, time.Now().Unix()),
		key:        key,
	}

	return q
}