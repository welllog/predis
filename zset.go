package predis

import "github.com/gomodule/redigo/redis"

// ZAdd("key", 3, "test", 4, "phone", 4, "computer")
func (c *Conn) ZAdd(key string, scoreMembers ...interface{}) (int64, error) {
	args := make([]interface{}, len(scoreMembers)+1)
	args[0] = key
	copy(args[1:], scoreMembers)
	r, err := c.Conn.Do("ZADD", args...)
	if err != nil {
		return 0, err
	}
	return r.(int64), nil
}

func (c *Conn) ZAddNX(key string, scoreMembers ...interface{}) (int64, error) {
	args := make([]interface{}, len(scoreMembers)+2)
	args[0] = key
	args[1] = "NX"
	copy(args[2:], scoreMembers)
	r, err := c.Conn.Do("ZADD", args...)
	if err != nil {
		return 0, err
	}
	return r.(int64), nil
}

func (c *Conn) ZCard(key string) (int64, error) {
	r, err := c.Conn.Do("ZCARD", key)
	if err != nil {
		return 0, err
	}
	return r.(int64), nil
}

func (c *Conn) ZCount(key string, min, max int) (int64, error) {
	r, err := c.Conn.Do("ZCOUNT", key, min, max)
	if err != nil {
		return 0, err
	}
	return r.(int64), nil
}

func (c *Conn) ZIncrBy(key string, increment int64, member string) (int64, error) {
	r, err := c.Conn.Do("ZINCRBY", key, increment, member)
	if err != nil {
		return 0, err
	}
	return r.(int64), nil
}

func (c *Conn) ZRange(key string, start, stop int) ([]string, error) {
	return redis.Strings(c.Conn.Do("ZRANGE", key, start, stop))
}

func (c *Conn) ZRevRange(key string, start, stop int) ([]string, error) {
	return redis.Strings(c.Conn.Do("ZREVRANGE", key, start, stop))
}

func (c *Conn) ZRangeWithScores(key string, start, stop int) (map[string]int64, error) {
	return redis.Int64Map(c.Conn.Do("ZRANGE", key, start, stop, "WITHSCORES"))
}

func (c *Conn) ZRevRangeWithScores(key string, start, stop int) (map[string]int64, error) {
	return redis.Int64Map(c.Conn.Do("ZREVRANGE", key, start, stop, "WITHSCORES"))
}

// ZRangeByScore("test1", "-inf", "+inf")
func (c *Conn) ZRangeByScore(key string, min, max interface{}) ([]string, error) {
	return redis.Strings(c.Conn.Do("ZRANGEBYSCORE", key, min, max))
}

func (c *Conn) ZRevRangeByScore(key string, min, max interface{}) ([]string, error) {
	return redis.Strings(c.Conn.Do("ZREVRANGEBYSCORE", key, min, max))
}

func (c *Conn) ZRangeByScoreWithScore(key string, min, max interface{}) (map[string]int64, error) {
	return redis.Int64Map(c.Conn.Do("ZRANGEBYSCORE", key, min, max, "WITHSCORES"))
}

func (c *Conn) ZRevRangeByScoreWithScore(key string, min, max interface{}) (map[string]int64, error) {
	return redis.Int64Map(c.Conn.Do("ZREVRANGEBYSCORE", key, min, max, "WITHSCORES"))
}

func (c *Conn) ZRank(key, member string) (int64, error) {
	return redis.Int64(c.Conn.Do("ZRANK", key, member))
}

func (c *Conn) ZRevRank(key, member string) (int64, error) {
	return redis.Int64(c.Conn.Do("ZREVRANK", key, member))
}

func (c *Conn) ZScore(key, member string) (int64, error) {
	return redis.Int64(c.Conn.Do("ZSCORE", key, member))
}

func (c *Conn) ZRem(key string, members ...string) (int64, error) {
	args := make([]interface{}, 0, len(members)+1)
	args = append(args, key)
	for i := range members {
		args = append(args, members[i])
	}
	r, err := c.Conn.Do("ZREM", args...)
	if err != nil {
		return 0, err
	}
	return r.(int64), nil
}

func (c *Conn) ZRemRangeByRank(key string, start, stop int) (int64, error) {
	return redis.Int64(c.Conn.Do("ZREMRANGEBYRANK", key, start, stop))
}

func (c *Conn) ZRemRangeByScore(key string, min, max int64) (int64, error) {
	return redis.Int64(c.Conn.Do("ZREMRANGEBYSCORE", key, min, max))
}
