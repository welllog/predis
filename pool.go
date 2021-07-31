package predis

import (
	"context"
	"github.com/gomodule/redigo/redis"
	"log"
	"time"
)

type ConnPool struct {
	*redis.Pool
}

func (p ConnPool) Get() *Conn {
	return &Conn{
		Conn: p.Pool.Get(),
	}
}

func (p ConnPool) GetContext(ctx context.Context) (*Conn, error) {
	conn, err := p.Pool.GetContext(ctx)
	if err != nil {
		return nil, err
	}
	return &Conn{
		Conn: conn,
	}, nil
}

func NewPool(address, password string, opts ...Option) ConnPool {
	options := options{
		maxIdle:      0,
		maxActive:    0,
		idleTimeout:  0,
		connLifetime: 0,
		wait:         true,
		dialOps: []redis.DialOption{
			redis.DialConnectTimeout(5 * time.Second),
			redis.DialReadTimeout(time.Second),
			redis.DialWriteTimeout(2 * time.Second),
		},
	}

	for _, o := range opts {
		o.apply(&options)
	}
	options.dialOps = append(options.dialOps, redis.DialPassword(password))
	if options.check == nil {
		options.check = _defHealthCheckFunc
	}

	p := redis.Pool{
		// 最大空闲连接数
		MaxIdle: options.maxIdle,
		// 最大活跃连接数
		MaxActive: options.maxActive,
		// 空闲连接超时时间
		IdleTimeout: options.idleTimeout,
		// 链接最大存活时间
		MaxConnLifetime: options.connLifetime,
		// 创建连接函数
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp",
				address,
				options.dialOps...,
			)
			if err != nil {
				log.Printf("redis dial failed: %s \n", err.Error())
				return nil, err
			}
			return c, nil
		},
		// 健康检测函数
		TestOnBorrow: options.check,
		// 连接达到上限阻塞
		Wait: options.wait,
	}

	conn := p.Get()
	_, err := conn.Do("PING")
	_ = conn.Close()

	if err != nil {
		panic("redis connect error: " + err.Error())
	}

	return ConnPool{
		&p,
	}

}
