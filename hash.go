package predis

import (
	"encoding/json"
	"github.com/gomodule/redigo/redis"
)

func (c *Conn) HGetAll(key string, fill interface{}) error {
	r, err := redis.Values(c.Conn.Do("HGETALL", key))
	if err != nil {
		return err
	}
	if len(r) == 0 { // 找不到也判定为错误
		return redis.ErrNil
	}
	return redis.ScanStruct(r, fill)
}

func (c *Conn) HGetAllToStringMap(key string) (map[string]string, error) {
	return redis.StringMap(c.Conn.Do("HGETALL", key))
}

func (c *Conn) HGetAllToInt64Map(key string) (map[string]int64, error) {
	return redis.Int64Map(c.Conn.Do("HGETALL", key))
}

func (c *Conn) HGetAllToIntMap(key string) (map[string]int, error) {
	return redis.IntMap(c.Conn.Do("HGETALL", key))
}

func (c *Conn) HSet(key, field string, val interface{}) error {
	var value interface{}
	switch v := val.(type) {
	case string, int, uint, int8, int16, int32, int64, uint8, uint16, uint32, uint64, float32, float64, bool, []byte:
		value = v
	default:
		s, err := json.Marshal(v)
		if err != nil {
			return err
		}
		value = s
	}
	_, err := c.Conn.Do("HSET", key, field, value)
	return err
}

func (c *Conn) HMset(key string, val interface{}) error {
	_, err := c.Conn.Do("HMSET", redis.Args{}.Add(key).AddFlat(val)...)
	return err
}

func (c *Conn) HGet(key, field string) (interface{}, error) {
	return c.Conn.Do("HGET", key, field)
}

func (c *Conn) HGetString(key, field string) (string, error) {
	return redis.String(c.Conn.Do("HGET", key, field))
}

func (c *Conn) HGetInt(key, field string) (int, error) {
	return redis.Int(c.Conn.Do("HGET", key, field))
}

func (c *Conn) HGetInt64(key, field string) (int64, error) {
	return redis.Int64(c.Conn.Do("HGET", key, field))
}

func (c *Conn) HGetObject(key, field string, fill interface{}) error {
	r, err := redis.Bytes(c.Conn.Do("HGET", key, field))
	if err != nil {
		return err
	}
	return json.Unmarshal(r, fill)
}

func (c *Conn) HMGetStrings(key string, fields []string) ([]string, error) {
	args := make([]interface{}, len(fields)+1)
	args = append(args, key)
	for i := range fields {
		args = append(args, fields[i])
	}
	return redis.Strings(c.Conn.Do("HMGET", args...))
}

func (c *Conn) HMGetInt64s(key string, fields []string) ([]int64, error) {
	args := make([]interface{}, 0, len(fields)+1)
	args = append(args, key)
	for i := range fields {
		args = append(args, fields[i])
	}
	return redis.Int64s(c.Conn.Do("HMGET", args...))
}

func (c *Conn) HMGetInts(key string, fields []string) ([]int, error) {
	args := make([]interface{}, 0, len(fields)+1)
	args = append(args, key)
	for i := range fields {
		args = append(args, fields[i])
	}
	return redis.Ints(c.Conn.Do("HMGET", args...))
}

func (c *Conn) HIncrBy(key, field string, increment int64) (int64, error) {
	return redis.Int64(c.Conn.Do("HINCRBY", key, field, increment))
}

func (c *Conn) HSetNx(key, field string, val interface{}) (int64, error) {
	return redis.Int64(c.Conn.Do("HSETNX", key, field, val))
}

func (c *Conn) HLen(key string) (int64, error) {
	return redis.Int64(c.Conn.Do("HLEN", key))
}

func (c *Conn) HDel(key string, fields ...string) (int64, error) {
	args := make([]interface{}, 0, len(fields)+1)
	args = append(args, key)
	for _, v := range fields {
		args = append(args, v)
	}
	return redis.Int64(c.Conn.Do("HDEL", args...))
}

func (c *Conn) HExists(key string, field string) (bool, error) {
	r, err := c.Conn.Do("HEXISTS", key, field)
	if err != nil {
		return false, err
	}
	return r.(int64) != 0, nil
}
