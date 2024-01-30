package stream

import (
	"context"
	"fmt"

	"github.com/YunWonYong/redis/pool"
	"github.com/gomodule/redigo/redis"
	"github.com/pkg/errors"
)

type (
	RedisStreamConsumerInfo struct {
		*StreamConsumerInfo
		ConsumerGroupID string
		ConsumerID      string
		IsBlock         bool
		BlockTimeoutSec int
		Count           int
		// 보통 ">" 를 사용함.
		ID string
	}

	RedisStream struct {
		pool *redis.Pool
	}
)

func GetRedisStreamConsumerInfo(streamKey string) *RedisStreamConsumerInfo {
	rsci := new(RedisStreamConsumerInfo)
	rsci.StreamConsumerInfo = new(StreamConsumerInfo)
	rsci.StreamKey = streamKey
	return rsci
}

func (rs *RedisStream) Init(p *redis.Pool) error {
	if err := pool.PingTest(p); err != nil {
		return err
	}

	rs.pool = p
	return nil
}

func (rs *RedisStream) XREADGROUP(ctx context.Context, info *RedisStreamConsumerInfo) (chan *StreamData, error) {
	if len(info.StreamKey) == 0 {
		return nil, errors.New("StreamKey required value.")
	}

	if len(info.ConsumerGroupID) == 0 {
		return nil, errors.New("ConsumerGroupID required value.")
	}

	consumerID := info.ConsumerID
	if len(consumerID) == 0 {
		return nil, errors.New("ConsumerID required value.")
	}

	if len(info.ID) == 0 {
		return nil, errors.New("ID required value.")
	}

	channel := make(chan *StreamData, 0)
	args := make([]interface{}, 0)
	args = append(args, "GROUP", info.ConsumerGroupID, consumerID)

	if info.Count > 0 {
		args = append(args, "COUNT", info.Count)
	}

	if info.IsBlock {
		args = append(args, "BLOCK", info.BlockTimeoutSec)
	}

	args = append(args, "STREAMS", info.StreamKey, info.ID)

	go func() {
		conn := rs.pool.Get()
		defer conn.Close()
		reply, err := conn.Do("XREADGROUP", args...)
		sd := new(StreamData)
		if err != nil {
			sd.err = err
			channel <- sd
			return
		}
		fmt.Println(reply)
		// select {
		// case <-ctx.Done():
		// 	close(channel)
		// }
	}()
	return channel, nil
}
