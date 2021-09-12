package predis

import (
	"context"
	"time"

	"github.com/gomodule/redigo/redis"
)

func (c *Conn) Publish(channel string, message []byte) (int64, error) {
	return redis.Int64(c.Conn.Do("PUBLISH", channel, message))
}

func (c *Conn) Subscribe(ctx context.Context, timeout time.Duration, channel ...string) (
	<-chan *redis.Message,
	<-chan error,
	error,
) {
	channels := make([]interface{}, len(channel))
	for i := range channel {
		channels[i] = channel[i]
	}
	err := c.Conn.Send("SUBSCRIBE", channels...)
	if err != nil {
		return nil, nil, err
	}
	err = c.Conn.Flush()
	if err != nil {
		return nil, nil, err
	}

	ch := make(chan *redis.Message)
	errCh := make(chan error, 1)

	go func() {
		<-ctx.Done()
		_, _ = c.Conn.Do("UNSUBSCRIBE", channels...)
	}()

	go func() {
		for {
			reply, err := redis.Values(redis.ReceiveWithTimeout(c.Conn, timeout))
			if err != nil {
				close(ch)
				errCh <- err
				return
			}
			var kind string
			reply, err = redis.Scan(reply, &kind)
			if err != nil {
				close(ch)
				errCh <- err
				return
			}
			switch kind {
			case "message":
				var m redis.Message
				if _, err := redis.Scan(reply, &m.Channel, &m.Data); err != nil {
					close(ch)
					errCh <- err
					return
				}
				ch <- &m
			case "unsubscribe":
				close(ch)
				close(errCh)
				return
			}
		}
	}()
	return ch, errCh, nil
}
