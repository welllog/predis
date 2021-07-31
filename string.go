package predis

import (
	"encoding/json"

	"github.com/gomodule/redigo/redis"
)

func (c *Conn) GetSting(key string) (string, error) {
	return redis.String(c.Conn.Do("GET", key))
}

func (c *Conn) GetInt(key string) (int, error) {
	return redis.Int(c.Conn.Do("GET", key))
}

func (c *Conn) GetInt64(key string) (int64, error) {
	return redis.Int64(c.Conn.Do("GET", key))
}

func (c *Conn) GetFloat64(key string) (float64, error) {
	return redis.Float64(c.Conn.Do("GET", key))
}

func (c *Conn) GetObject(key string, fill interface{}) error {
	r, err := redis.Bytes(c.Conn.Do("GET", key))
	if err != nil {
		return err
	}
	return json.Unmarshal(r, fill)
}

func (c *Conn) Get(key string) (interface{}, error) {
	return c.Conn.Do("GET", key)
}

func (c *Conn) MGet(keys ...string) (interface{}, error) {
	args := make([]interface{}, len(keys))
	for i, v := range keys {
		args[i] = v
	}
	return c.Conn.Do("MGET", args...)
}

func (c *Conn) MGetString(keys ...string) ([]string, error) {
	args := make([]interface{}, len(keys))
	for i, v := range keys {
		args[i] = v
	}
	return redis.Strings(c.Conn.Do("MGET", args...))
}

func (c *Conn) MGetInt(keys ...string) ([]int, error) {
	args := make([]interface{}, len(keys))
	for i, v := range keys {
		args[i] = v
	}
	return redis.Ints(c.Conn.Do("MGET", args...))
}

func (c *Conn) MGetInt64(keys ...string) ([]int64, error) {
	args := make([]interface{}, len(keys))
	for i, v := range keys {
		args[i] = v
	}
	return redis.Int64s(c.Conn.Do("MGET", args...))
}

func (c *Conn) Set(key string, val interface{}, setOption ...interface{}) (interface{}, error) {
	args := make([]interface{}, len(setOption)+2)
	args[0] = key
	switch v := val.(type) {
	case string, int, uint, int8, int16, int32, int64, uint8, uint16, uint32, uint64, float32, float64, bool, []byte:
		args[1] = v
	default:
		s, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		args[1] = s
	}
	copy(args[2:], setOption)
	return c.Conn.Do("SET", args...)
}

func (c *Conn) MSet(kvs map[string]interface{}) error {
	args := make([]interface{}, len(kvs)*2)
	i := 0
	for k, v := range kvs {
		args[i] = k
		args[i+1] = v
		i++
	}
	_, err := c.Conn.Do("MSET", args...)
	return err
}

func (c *Conn) Del(keys ...string) (int64, error) {
	d := make([]interface{}, len(keys))
	for i, v := range keys {
		d[i] = v
	}
	r, err := c.Conn.Do("DEL", d...)
	if err != nil {
		return 0, err
	}
	return r.(int64), nil
}

// If using Redis >= 4.0.0 you can call unlink
func (c *Conn) Unlink(keys ...string) (int64, error) {
	d := make([]interface{}, len(keys))
	for i, v := range keys {
		d[i] = v
	}
	r, err := c.Conn.Do("UNLINK", d...)
	if err != nil {
		return 0, err
	}
	return r.(int64), nil
}

func (c *Conn) SetEx(key string, val interface{}, expire int) error {
	var err error
	if expire > 0 {
		_, err = c.Set(key, val, "EX", expire)
	} else {
		_, err = c.Set(key, val)
	}
	if err != nil {
		return err
	}
	return nil
}

func (c *Conn) SetNx(key string, val interface{}, expire int) (bool, error) {
	var (
		r   interface{}
		err error
	)
	if expire > 0 {
		r, err = c.Set(key, val, "EX", expire, "NX")
	} else {
		r, err = c.Set(key, val, "NX")
	}
	if err != nil {
		return false, err
	}
	res, ok := r.(string)
	if ok && res == OK {
		return true, nil
	}
	return false, nil
}

func (c *Conn) SetXx(key string, val interface{}, expire int) (bool, error) {
	var (
		r   interface{}
		err error
	)
	if expire > 0 {
		r, err = c.Set(key, val, "EX", expire, "XX")
	} else {
		r, err = c.Set(key, val, "XX")
	}
	if err != nil {
		return false, err
	}
	res, ok := r.(string)
	if ok && res == OK {
		return true, nil
	}
	return false, nil
}

func (c *Conn) GetSet(key string, val interface{}) (interface{}, error) {
	return c.Conn.Do("GETSET", key, val)
}

func (c *Conn) Expire(key string, expire int) (bool, error) {
	r, err := c.Conn.Do("EXPIRE", key, expire)
	if err != nil {
		return false, err
	}
	return r.(int64) != 0, nil
}

func (c *Conn) Exists(key string) (bool, error) {
	r, err := c.Conn.Do("EXISTS", key)
	if err != nil {
		return false, err
	}
	return r.(int64) != 0, nil
}

func (c *Conn) Ttl(key string) (int64, error) {
	return redis.Int64(c.Conn.Do("TTL", key))
}

func (c *Conn) Incr(key string) (int64, error) {
	return redis.Int64(c.Conn.Do("INCR", key))
}

func (c *Conn) IncrBy(key string, increment int) (int64, error) {
	return redis.Int64(c.Conn.Do("INCRBY", key, increment))
}

func (c *Conn) IncrByFloat(key string, increment float64) (float64, error) {
	return redis.Float64(c.Conn.Do("INCRBYFLOAT", key, increment))
}
