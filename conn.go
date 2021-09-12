package predis

import (
	"context"
	"time"

	"github.com/gomodule/redigo/redis"
)

const OK = "OK"

type Conn struct {
	redis.Conn
}

func NewConn(ctx context.Context, address, password string, options ...redis.DialOption) (*Conn, error) {
	opts := []redis.DialOption{
		redis.DialConnectTimeout(5 * time.Second),
		redis.DialWriteTimeout(2 * time.Second),
		redis.DialPassword(password),
	}
	opts = append(opts, options...)
	c, err := redis.DialContext(
		ctx,
		"tcp",
		address,
		opts...,
	)
	if err != nil {
		return nil, err
	}
	_, err = c.Do("PING")
	if err != nil {
		return nil, err
	}
	return &Conn{
		Conn: c,
	}, nil
}

func (c *Conn) Ping() error {
	_, err := c.Conn.Do("PING")
	return err
}

func (c *Conn) Release() {
	if c.Conn == nil {
		return
	}
	conn := c.Conn
	c.Conn = nil
	_ = conn.Close()
}

func (c *Conn) Pipelined(fn func(pipe *Pipeline)) (*PipelineReply, error) {
	p := &Pipeline{
		conn: c.Conn,
	}
	fn(p)
	if p.err != nil {
		return nil, p.err
	}
	if err := c.Flush(); err != nil {
		return nil, err
	}
	reply := &PipelineReply{
		conn:         c.Conn,
		pipelineIncr: p.pipelineIncr,
	}
	return reply, nil
}

func (c *Conn) Watch(fn func(tx *Conn) bool, keys ...string) error {
	args := make([]interface{}, len(keys))
	for i := range keys {
		args[i] = keys[i]
	}
	_, err := c.Conn.Do("WATCH", args...)
	if err != nil {
		return err
	}
	abort := fn(c)
	if !abort {
		_, err = c.Conn.Do("UNWATCH", args...)
	}
	return err
}

func (c *Conn) Multi(fn func(pipe *Pipeline)) ([]interface{}, error) {
	p := &Pipeline{
		conn: c.Conn,
	}
	p.Send("MULTI")
	fn(p)
	p.Send("EXEC")
	if p.err != nil {
		return nil, p.err
	}
	if err := c.Flush(); err != nil {
		return nil, err
	}
	var (
		val interface{}
		err error
	)
	for i := 0; i < p.pipelineIncr; i++ {
		val, err = p.conn.Receive()
		if err != nil {
			return nil, err
		}
	}
	return redis.Values(val, nil)
}
