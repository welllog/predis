package predis

import (
	"github.com/gomodule/redigo/redis"
	"time"
)

type HealthCheckFunc func(c redis.Conn, t time.Time) error

type options struct {
	maxIdle      int
	maxActive    int
	idleTimeout  time.Duration
	connLifetime time.Duration
	wait         bool
	check        HealthCheckFunc
	dialOps      []redis.DialOption
}

type Option interface {
	apply(*options)
}

type optionFunc func(*options)

func (f optionFunc) apply(o *options) {
	f(o)
}

var _defHealthCheckFunc = func(c redis.Conn, t time.Time) error {
	// 一分钟之内不重复检测
	if time.Since(t) < time.Minute {
		return nil
	}
	_, err := c.Do("PING")
	return err
}

// 最大空闲链接数
func WithMaxIdle(maxIdle int) Option {
	return optionFunc(func(o *options) {
		o.maxIdle = maxIdle
	})
}

// 最大活跃链接数
func WithMaxActive(maxActive int) Option {
	return optionFunc(func(o *options) {
		o.maxActive = maxActive
	})
}

// 链接空闲超时时间
func WithIdleTimeout(idleTimeout time.Duration) Option {
	return optionFunc(func(o *options) {
		o.idleTimeout = idleTimeout
	})
}

// 链接最大存活时间
func WithConnLifetime(lifetime time.Duration) Option {
	return optionFunc(func(o *options) {
		o.connLifetime = lifetime
	})
}

// 达到最大活跃链接数是否阻塞
func WithWait(wait bool) Option {
	return optionFunc(func(o *options) {
		o.wait = wait
	})
}

// redis建立链接配置项
func WithDialOptions(dialOps ...redis.DialOption) Option {
	return optionFunc(func(o *options) {
		o.dialOps = append(o.dialOps, dialOps...)
	})
}

// 获取链接时的健康监测函数
func WithHealthCheckFunc(fn HealthCheckFunc) Option {
	return optionFunc(func(o *options) {
		o.check = fn
	})
}
