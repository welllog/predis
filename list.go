package predis

import "github.com/gomodule/redigo/redis"

func (c *Conn) LPush(key string, vals ...interface{}) (int64, error) {
	args := make([]interface{}, len(vals)+1)
	args[0] = key
	copy(args[1:], vals)
	return redis.Int64(c.Conn.Do("LPUSH", args...))
}

func (c *Conn) RPush(key string, vals ...interface{}) (int64, error) {
	args := make([]interface{}, len(vals)+1)
	args[0] = key
	copy(args[1:], vals)
	return redis.Int64(c.Conn.Do("RPUSH", args...))
}

func (c *Conn) LPop(key string) (interface{}, error) {
	return c.Conn.Do("LPOP", key)
}

func (c *Conn) RPop(key string) (interface{}, error) {
	return c.Conn.Do("RPOP", key)
}

func (c *Conn) LPopString(key string) (string, error) {
	return redis.String(c.Conn.Do("LPOP", key))
}

func (c *Conn) LPopInt(key string) (int, error) {
	return redis.Int(c.Conn.Do("LPOP", key))
}

func (c *Conn) LPopInt64(key string) (int64, error) {
	return redis.Int64(c.Conn.Do("LPOP", key))
}

func (c *Conn) RPopString(key string) (string, error) {
	return redis.String(c.Conn.Do("RPOP", key))
}

func (c *Conn) RPopInt(key string) (interface{}, error) {
	return redis.Int(c.Conn.Do("RPOP", key))
}

func (c *Conn) RPopInt64(key string) (interface{}, error) {
	return redis.Int64(c.Conn.Do("RPOP", key))
}

func (c *Conn) LLen(key string) (int64, error) {
	return redis.Int64(c.Conn.Do("LLEN", key))
}

func (c *Conn) LRem(key string, count int, val string) (int64, error) {
	return redis.Int64(c.Conn.Do("LREM", key, count, val))
}

func (c *Conn) LTrim(key string, start, stop int) (interface{}, error) {
	return c.Conn.Do("LTRIM", key, start, stop)
}

func (c *Conn) LRange(key string, start, stop int) (interface{}, error) {
	return c.Conn.Do("LRANGE", key, start, stop)
}
