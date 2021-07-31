package predis

import "github.com/gomodule/redigo/redis"

func (c *Conn) SAdd(key string, members ...interface{}) (int64, error) {
	args := make([]interface{}, len(members)+1)
	args[0] = key
	copy(args[1:], members)
	r, err := c.Conn.Do("SADD", args...)
	if err != nil {
		return 0, err
	}
	return r.(int64), nil
}

func (c *Conn) SCard(key string) (int64, error) {
	r, err := c.Conn.Do("SCARD", key)
	if err != nil {
		return 0, err
	}
	return r.(int64), nil
}

func (c *Conn) SIsMember(key string, member interface{}) (bool, error) {
	r, err := c.Conn.Do("SISMEMBER", key, member)
	if err != nil {
		return false, err
	}
	return r.(int64) == 0, nil
}

func (c *Conn) SMembersStrings(key string) ([]string, error) {
	return redis.Strings(c.Conn.Do("SMEMBERS", key))
}

func (c *Conn) SMembersInt64s(key string) ([]int64, error) {
	return redis.Int64s(c.Conn.Do("SMEMBERS", key))
}

func (c *Conn) SMembersInts(key string) ([]int, error) {
	return redis.Ints(c.Conn.Do("SMEMBERS", key))
}

func (c *Conn) SPop(key string, count ...int) (interface{}, error) {
	if len(count) > 0 {
		return c.Conn.Do("SPOP", key, count[0])
	}
	return c.Conn.Do("SPOP", key)
}

func (c *Conn) SPopString(key string) (string, error) {
	return redis.String(c.Conn.Do("SPOP", key))
}

func (c *Conn) SPopInt64(key string) (int64, error) {
	return redis.Int64(c.Conn.Do("SPOP", key))
}

func (c *Conn) SPopInt(key string) (int, error) {
	return redis.Int(c.Conn.Do("SPOP", key))
}

func (c *Conn) SRandMember(key string, num ...int) (interface{}, error) {
	n := 1
	if len(num) > 0 {
		n = num[0]
	}
	return c.Conn.Do("SRANDMEMBER", key, n)
}

func (c *Conn) SRem(key string, members ...string) (int64, error) {
	args := make([]interface{}, 0, len(members)+1)
	args = append(args, key)
	for i := range members {
		args = append(args, members[i])
	}
	r, err := c.Conn.Do("SREM", args...)
	if err != nil {
		return 0, err
	}
	return r.(int64), nil
}
