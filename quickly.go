package predis

import (
	"context"
)

var Pool ConnPool

func InitPool(address, password string, ops ...Option) {
	Pool = NewPool(
		address,
		password,
		ops...,
	)
}

func ClosePool() error {
	if Pool.Pool != nil {
		return Pool.Pool.Close()
	}
	return nil
}

func GetConn() *Conn {
	return Pool.Get()
}

func GetContextConn(ctx context.Context) (*Conn, error) {
	return Pool.GetContext(ctx)
}

func GetActiveCount() int {
	return Pool.ActiveCount()
}

func GetIdleCount() int {
	return Pool.IdleCount()
}
