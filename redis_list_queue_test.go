package queue

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis"

	"github.com/go-framework/config/redis"
)

func TestRedisQueue_Push(t *testing.T) {

	s, err := miniredis.Run()
	if err != nil {
		t.Error(err)
	}
	defer s.Close()

	key := "TestRedisQueue_Push"

	now := time.Now()

	config := redis.DefaultRedisConfig
	config.Addr = s.Addr()

	queue := NewRedisQueue(key, config)

	if err := queue.Init(context.TODO()); err != nil {
		t.Fatal(err)
	}

	if err := queue.Push(now.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}

	data, err := s.Lpop(key)
	if err != nil {
		t.Fatal(err)
	}

	if n, err := time.Parse(time.RFC3339Nano, data); err != nil {
		t.Fatal(err)
	} else if !n.Equal(now) {
		t.Fatal("value should be", now, "not", n)
	}
}

func TestRedisQueue_Pop(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		t.Error(err)
	}
	defer s.Close()

	key := "TestRedisQueue_Pop"

	now := time.Now()

	config := redis.DefaultRedisConfig
	config.Addr = s.Addr()

	queue := NewRedisQueue(key, config)

	if err := queue.Init(context.TODO()); err != nil {
		t.Fatal(err)
	}

	if err := queue.Push(now.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}

	str, err := queue.Pop(context.TODO())
	if err != nil {
		t.Fatal(err)
	}

	if n, err := time.Parse(time.RFC3339Nano, str.(string)); err != nil {
		t.Fatal(err)
	} else if !n.Equal(now) {
		t.Fatal("value should be", now, "not", n)
	}

	if l, err := s.List(queue.privateKey); err != nil && err != miniredis.ErrKeyNotFound {
		t.Fatal(err)
	} else if len(l) == 0 {
		t.Fatal("privateKey should have some data", l)
	}

	if err = queue.Remove(str); err != nil {
		t.Fatal(err)
	}

	if l, err := s.List(queue.privateKey); err != nil && err != miniredis.ErrKeyNotFound {
		t.Fatal(err)
	} else if len(l) != 0 {
		t.Fatal("privateKey should not have some data", l)
	}
}

func TestRedisQueue_ReturnElements(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		t.Error(err)
	}
	defer s.Close()

	key := "TestRedisQueue_ReturnElements"

	now := time.Now()

	config := redis.DefaultRedisConfig
	config.Addr = s.Addr()

	queue := NewRedisQueue(key, config)

	if err := queue.Init(context.TODO()); err != nil {
		t.Fatal(err)
	}

	if err := queue.Push(now.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}

	str, err := queue.Pop(context.TODO())
	if err != nil {
		t.Fatal(err)
	}

	if n, err := time.Parse(time.RFC3339Nano, str.(string)); err != nil {
		t.Fatal(err)
	} else if !n.Equal(now) {
		t.Fatal("value should be", now, "not", n)
	}

	if l, err := s.List(queue.key); err != nil && err != miniredis.ErrKeyNotFound {
		t.Fatal(err)
	} else if len(l) != 0 {
		t.Fatal("privateKey should not have some data", l)
	}

	if l, err := s.List(queue.privateKey); err != nil {
		t.Fatal(err)
	} else if len(l) == 0 {
		t.Fatal("privateKey should have some data", l)
	}

	if err := queue.ReturnElements(0); err != nil {
		t.Fatal(err)
	}

	if l, err := s.List(queue.key); err != nil {
		t.Fatal(err)
	} else if len(l) == 0 {
		t.Fatal("privateKey should have some data", l)
	}

	if l, err := s.List(queue.privateKey); err != nil && err != miniredis.ErrKeyNotFound {
		t.Fatal(err)
	} else if len(l) != 0 {
		t.Fatal("privateKey should not have some data", l)
	}
}

func TestRedisQueue_Remove(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		t.Error(err)
	}
	defer s.Close()

	key := "TestRedisQueue_Remove"

	config := redis.DefaultRedisConfig
	config.Addr = s.Addr()

	queue := NewRedisQueue(key, config)

	if err := queue.Init(context.TODO()); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		if err := queue.Push(i); err != nil {
			t.Error(err)
		}
	}

	if l, err := s.List(queue.key); err != nil {
		t.Error(err)
	} else if len(l) == 0 {
		t.Error("privateKey should have some data", l)
	}

	if l, err := s.List(queue.privateKey); err != nil && err != miniredis.ErrKeyNotFound {
		t.Error(err)
	} else if len(l) != 0 {
		t.Error("privateKey should not have some data", l)
	}

	for {
		if reply, err := queue.Pop(context.TODO()); err != nil || reply == nil {
			break
		}
	}

	if l, err := s.List(queue.key); err != nil && err != miniredis.ErrKeyNotFound {
		t.Error(err)
	} else if len(l) != 0 {
		t.Error("privateKey should not have some data", l)
	}

	if l, err := s.List(queue.privateKey); err != nil {
		t.Error(err)
	} else if len(l) == 0 {
		t.Error("privateKey should have some data", l)
	}

	for i := 0; i < 10; i++ {
		if err := queue.Remove(i); err != nil {
			t.Error(err)
		}
		if l, err := s.List(queue.privateKey); err != nil && err != miniredis.ErrKeyNotFound {
			t.Error(err)
		} else {
			for _, d := range l {
				if d == strconv.Itoa(i) {
					t.Error("privateKey should have", i)
				}
			}
		}
	}

	if l, err := s.List(queue.privateKey); err != nil && err != miniredis.ErrKeyNotFound {
		t.Error(err)
	} else if len(l) != 0 {
		t.Error("privateKey should not have some data", l)
	}
}
