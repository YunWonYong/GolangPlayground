package stream

import (
	"context"
	"errors"
	"testing"

	"github.com/YunWonYong/redis/pool"
)

func TestRedisStreamInit(t *testing.T) {

	options := new(pool.RedisPoolInitOptions)
	options.Network = "tcp"
	options.Address = "localhost:6379"
	options.MaxIdle = 10
	options.Database = 0

	pool, err := pool.NewRedisPool(context.TODO(), options)
	if err != nil {
		t.Error(err)
		return
	}

	rs := new(RedisStream)
	if err := rs.Init(pool.Pool); err != nil {
		t.Error(err)
		return
	}
}

func TestRedisStreamConsumerAdd(t *testing.T) {

	options := new(pool.RedisPoolInitOptions)
	options.Network = "tcp"
	options.Address = "localhost:6379"
	options.MaxIdle = 10
	options.Database = 0

	pool, err := pool.NewRedisPool(context.TODO(), options)
	if err != nil {
		t.Error(err)
		return
	}

	rs := new(RedisStream)
	if err := rs.Init(pool.Pool); err != nil {
		t.Error(err)
		return
	}
	info := GetRedisStreamConsumerInfo("stream-test")
	info.ConsumerGroupID = "stream-test-group"
	info.ConsumerID = "stg-1"
	info.IsBlock = true
	info.Count = 5
	info.ID = ">"
	c, err := rs.XREADGROUP(context.TODO(), info)

	if err != nil {
		t.Error(err)
		return
	}

	if c == nil {
		t.Error(errors.New("channel empty."))
		return
	}

	streamData := <-c
	if streamData.err != nil {
		t.Error(streamData.err)
		return
	}
}
