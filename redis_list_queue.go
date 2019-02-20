package queue

import (
	"bytes"
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
type RedisListQueue struct {
	// queue private key, format is [KEY][TimestampSeparator][timestamp] then match string is [KEY*][TimestampSeparator][timestamp].
	privateKey string
	// redis key.
	key string
	// redis client.
	client *redis.Client
}

// Return the private key to the queue key greater than interval second.
// first get all elements back , after get self private elements back.
func (this *RedisListQueue) ReturnElements(interval int64) error {
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
func (this *RedisListQueue) Init(ctx context.Context) error {

	// context contain redis config.
	if config, ok := redis.FromContext(ctx); ok {
		this.client = config.NewGoClient()
	}

	return this.ReturnElements(0)
}

// Push any data into the queue.
func (this *RedisListQueue) Push(data interface{}) error {
	// inserting data at the end of the redis list.
	return this.client.LPush(this.key, data).Err()
}

// Pop first element with timeout, pop from queue key into the queue private key,
// when finish handle the Pop element please call the Remove function to remove.
// default duration is 1 second, context can carry Duration context and DoFunction context.
func (this *RedisListQueue) Pop(ctx context.Context) (interface{}, error) {
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
	raw, err := this.client.BRPopLPush(this.key, this.privateKey, duration).Result()
	if err == redis.GoRedisNil {
		return nil, QueueNilErr
	}
	return raw, err
}

// Remove the last element of private key.
func (this *RedisListQueue) Remove(data interface{}) error {
	// pop up the first element of the list.
	return this.client.LRem(this.privateKey, -1, data).Err()
}

// Size of queuer.
func (this *RedisListQueue) Size() int64 {
	// pop up the first element of the list.
	return this.client.LLen(this.key).Val()
}

// Implement Marshaler.
func (this *RedisListQueue) MarshalJSON() ([]byte, error) {

	// range of list.
	results, err := this.client.LRange(this.key, 0, -1).Result()
	if err != nil {
		return nil, err
	}

	// convert []string to []byte as json format.
	buffer := bytes.Buffer{}

	buffer.WriteByte('[')
	for _, result := range results {
		buffer.WriteString(result)
		buffer.WriteByte(',')
	}
	buffer.WriteByte(']')

	return buffer.Bytes(), nil
}

// New redis list queue with key name and redis config,
// use linux timestamp for distinguish distributed read and write by privateKey.
func NewRedisListQueue(key string, config *redis.Config) *RedisListQueue {
	q := &RedisListQueue{
		// format is [KEY][TimestampSeparator][timestamp].
		privateKey: fmt.Sprintf("%s%s%d", key, TimestampSeparator, time.Now().Unix()),
		key:        key,
		client:     config.NewGoClient(),
	}

	// register for update.
	config.UpdateFuncRegister(func(c *redis.Config) {
		q.client = c.NewGoClient()
	})

	return q
}
