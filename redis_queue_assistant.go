package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
	"unsafe"
)

// mark is queue data.
const signQueueData = "#|"

// mark is queue data format, data type and data.
const signQueueDataFormat = "%s" + signQueueData + "%s"

// RedisQueueAssistant implement , use a chan controller the max concurrency.
type RedisQueueAssistant struct {
	queue          Queuer
	maxConcurrency chan struct{}
	assistants     sync.Map
	err            chan error
}

// Register assistant to queue.
func (q *RedisQueueAssistant) RegisterAssistanter(assistant Assistanter) {
	if assistant != nil {
		q.assistants.Store(assistant.NewData().Name(), assistant)
	}
}

// Push marshal raw data into queue, the key name is dataer name.
func (q *RedisQueueAssistant) Push(data Dataer) error {
	raw, err := data.Marshal()
	if err != nil {
		return err
	}
	d := fmt.Sprintf(signQueueDataFormat, data.Name(), raw)

	return q.queue.Push(d)
}

// Pop data from redis queue with context, return data type and store raw data.
func (q *RedisQueueAssistant) Pop(ctx context.Context) (string, string, error) {
	// pop data with timeout.
	data, err := q.queue.Pop(NewDurationContext(ctx, time.Second))
	if err != nil {
		return "", "", err
	}
	// redis queue is string type.
	str, ok := data.(string)
	if !ok {
		return "", "", errors.New("data is not string")
	}
	// data should contains sign queue data.
	splits := strings.Split(str, signQueueData)
	if len(splits) != 2 {
		return "", "", errors.New("split mark error")
	}

	return splits[0], splits[1], nil
}

// Pop the the data and parse the type, get the type function handle
// the raw data, when function return true will remove the queue.
func (q *RedisQueueAssistant) Done(ctx context.Context) {

	// assistant done function.
	done := func() (err error) {
		// defer recover.
		defer func() {
			// convert recover to error.
			if e := recover(); e != nil {
				switch e.(type) {
				case error:
					err = e.(error)
				default:
					err = fmt.Errorf("%v", e)
				}
			}
		}()

		// pop data with timeout.
		kind, raw, err := q.Pop(ctx)
		if err != nil {
			return
		}

		// put chan for one goroutine.
		q.maxConcurrency <- struct{}{}

		// goroutine run.
		go func(kind, data string) {
			// defer function.
			defer func() {
				// get recover.
				if e := recover(); e != nil {
					switch e.(type) {
					case error:
						err = e.(error)
					default:
						err = fmt.Errorf("%v", e)
					}
					if q.err != nil {
						q.err <- err
					}
				}
				// out chan.
				<-q.maxConcurrency
			}()

			// load data and do it.
			if face, ok := q.assistants.Load(kind); ok {
				if assistant, ok := face.(Assistanter); ok {
					// new data object.
					data := assistant.NewData()
					// unmarshal data.
					err = data.Unmarshal(*(*[]byte)(unsafe.Pointer(&raw)))
					if err != nil {
						return
					}
					// assistant done.
					err = assistant.Done(ctx, data)
				}
			}
		}(kind, raw)

		return
	}

	// loop done.
	for {
		select {
		case <-ctx.Done():
			return
		default:
			err := done()
			if q.err != nil {
				q.err <- err
			}
		}

	}
}

// Get error chan.
func (q *RedisQueueAssistant) Error() <-chan error {
	if q.err == nil {
		q.err = make(chan error, 32)
	}
	return q.err
}

// New queue with the RedisQueueAssistant and mac concurrency.
func NewRedisQueueAssistant(queue Queuer, maxConcurrency int) *RedisQueueAssistant {
	q := &RedisQueueAssistant{
		queue:          queue,
		maxConcurrency: make(chan struct{}, maxConcurrency),
	}

	return q
}
