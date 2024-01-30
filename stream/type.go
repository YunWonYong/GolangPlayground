package stream

import (
	"context"

	"github.com/gomodule/redigo/redis"
)

type (
	StreamConsumerInfo struct {
		StreamKey string
	}

	StreamData struct {
		Key    []string
		Values map[string][]string
		err    error
	}
)

type Stream interface {
	Init(pool *redis.Pool) error
	AddConsumer(ctx context.Context, consumerInfo *StreamConsumerInfo) chan *StreamData
}
