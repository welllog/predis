package predis

import (
	"encoding/json"
	"fmt"

	"github.com/gomodule/redigo/redis"
)

type PipelineReply struct {
	conn         redis.Conn
	pipelineIncr int
	reply        []interface{}
	err          error
}

func (p *PipelineReply) Value(index int) (interface{}, error) {
	if index >= p.pipelineIncr {
		return nil, redis.ErrNil
	}

	if len(p.reply) == 0 && p.err == nil {
		p.reply = make([]interface{}, 0, p.pipelineIncr)
		for i := 0; i < p.pipelineIncr; i++ {
			val, err := p.conn.Receive()
			if err != nil {
				p.err = err
				break
			}
			p.reply = append(p.reply, val)
		}
	}

	if len(p.reply) > index {
		return p.reply[index], nil
	}

	return nil, p.err
}

type Pipeline struct {
	conn         redis.Conn
	pipelineIncr int
	err          error
}

func (p *Pipeline) Send(commandName string, args ...interface{}) {
	p.pipelineIncr++
	if p.err != nil {
		return
	}
	p.err = p.conn.Send(commandName, args...)
}

func (p *Pipeline) Get(key string) {
	p.Send("GET", key)
}

func (p *Pipeline) MGet(keys ...string) {
	args := make([]interface{}, len(keys))
	for i, v := range keys {
		args[i] = v
	}
	p.Send("MGET", args...)
}

func (p *Pipeline) Set(key string, val interface{}, setOption ...interface{}) {
	args := make([]interface{}, len(setOption)+2)
	args[0] = key
	switch v := val.(type) {
	case string, int, uint, int8, int16, int32, int64, uint8, uint16, uint32, uint64, float32, float64, bool, []byte:
		args[1] = v
	default:
		args[1], _ = json.Marshal(v)
	}
	copy(args[2:], setOption)
	fmt.Println("haha")
	p.Send("SET", args...)
}

func (p *Pipeline) MSet(kvs map[string]interface{}) {
	args := make([]interface{}, len(kvs)*2)
	i := 0
	for k, v := range kvs {
		args[i] = k
		args[i+1] = v
		i++
	}
	p.Send("MSET", args...)
}

func (p *Pipeline) Del(keys ...string) {
	d := make([]interface{}, len(keys))
	for i, v := range keys {
		d[i] = v
	}
	p.Send("DEL", d...)
}

// If using Redis >= 4.0.0 you can call unlink
func (p *Pipeline) Unlink(keys ...string) {
	d := make([]interface{}, len(keys))
	for i, v := range keys {
		d[i] = v
	}
	p.Send("UNLINK", d...)
}

func (p *Pipeline) SetEx(key string, val interface{}, expire int) {
	if expire > 0 {
		p.Set(key, val, "EX", expire)
		return
	}
	p.Set(key, val)
}

func (p *Pipeline) SetNx(key string, val interface{}, expire int) {
	if expire > 0 {
		p.Set(key, val, "EX", expire, "NX")
		return
	}
	p.Set(key, val, "NX")
}

func (p *Pipeline) SetXx(key string, val interface{}, expire int) {
	if expire > 0 {
		p.Set(key, val, "EX", expire, "XX")
		return
	}
	p.Set(key, val, "XX")
}

func (p *Pipeline) GetSet(key string, val interface{}) {
	p.Send("GETSET", key, val)
}

func (p *Pipeline) Expire(key string, expire int) {
	p.Send("EXPIRE", key, expire)
}

func (p *Pipeline) Exists(key string) {
	p.Send("EXISTS", key)
}

func (p *Pipeline) Ttl(key string) {
	p.Send("TTL", key)
}

func (p *Pipeline) Incr(key string) {
	p.Send("INCR", key)
}

func (p *Pipeline) IncrBy(key string, increment int) {
	p.Send("INCRBY", key, increment)
}

func (p *Pipeline) IncrByFloat(key string, increment float64) {
	p.Send("INCRBYFLOAT", key, increment)
}

func (p *Pipeline) SAdd(key string, members ...interface{}) {
	args := make([]interface{}, len(members)+1)
	args[0] = key
	copy(args[1:], members)
	p.Send("SADD", args...)
}

func (p *Pipeline) SCard(key string) {
	p.Send("SCARD", key)
}

func (p *Pipeline) SIsMember(key string, member interface{}) {
	p.Send("SISMEMBER", key, member)
}

func (p *Pipeline) SPop(key string, count ...int) {
	if len(count) > 0 {
		p.Send("SPOP", key, count[0])
		return
	}
	p.Send("SPOP", key)
}

func (p *Pipeline) SRandMember(key string, num ...int) {
	n := 1
	if len(num) > 0 {
		n = num[0]
	}
	p.Send("SRANDMEMBER", key, n)
}

func (p *Pipeline) SRem(key string, members ...string) {
	args := make([]interface{}, 0, len(members)+1)
	args = append(args, key)
	for i := range members {
		args = append(args, members[i])
	}
	p.Send("SREM", args...)
}

func (p *Pipeline) HGetAll(key string) {
	p.Send("HGETALL", key)
}

func (p *Pipeline) HSet(key, field string, val interface{}) {
	var value interface{}
	switch v := val.(type) {
	case string, int, uint, int8, int16, int32, int64, uint8, uint16, uint32, uint64, float32, float64, bool, []byte:
		value = v
	default:
		value, _ = json.Marshal(v)
	}
	p.Send("HSET", key, field, value)
}

func (p *Pipeline) HMset(key string, val interface{}) {
	p.Send("HMSET", redis.Args{}.Add(key).AddFlat(val)...)
}

func (p *Pipeline) HGet(key, field string) {
	p.Send("HGET", key, field)
}

func (p *Pipeline) HMGet(key string, fields []string) {
	args := make([]interface{}, len(fields)+1)
	args = append(args, key)
	for i := range fields {
		args = append(args, fields[i])
	}
	p.Send("HMGET", args...)
}

func (p *Pipeline) HIncrBy(key, field string, increment int64) {
	p.Send("HINCRBY", key, field, increment)
}

func (p *Pipeline) HSetNx(key, field string, val interface{}) {
	p.Send("HSETNX", key, field, val)
}

func (p *Pipeline) HLen(key string) {
	p.Send("HLEN", key)
}

func (p *Pipeline) HDel(key string, fields ...string) {
	args := make([]interface{}, 0, len(fields)+1)
	args = append(args, key)
	for _, v := range fields {
		args = append(args, v)
	}
	p.Send("HDEL", args...)
}

func (p *Pipeline) HExists(key string, field string) {
	p.Send("HEXISTS", key, field)
}

// ZAdd("key", 3, "test", 4, "phone", 4, "computer")
func (p *Pipeline) ZAdd(key string, scoreMembers ...interface{}) {
	args := make([]interface{}, len(scoreMembers)+1)
	args[0] = key
	copy(args[1:], scoreMembers)
	p.Send("ZADD", args...)
}

func (p *Pipeline) ZAddNX(key string, scoreMembers ...interface{}) {
	args := make([]interface{}, len(scoreMembers)+2)
	args[0] = key
	args[1] = "NX"
	copy(args[2:], scoreMembers)
	p.Send("ZADD", args...)
}

func (p *Pipeline) ZCard(key string) {
	p.Send("ZCARD", key)
}

func (p *Pipeline) ZCount(key string, min, max int) {
	p.Send("ZCOUNT", key, min, max)
}

func (p *Pipeline) ZIncrBy(key string, increment int64, member string) {
	p.Send("ZINCRBY", key, increment, member)
}

func (p *Pipeline) ZRange(key string, start, stop int) {
	p.Send("ZRANGE", key, start, stop)
}

func (p *Pipeline) ZRevRange(key string, start, stop int) {
	p.Send("ZREVRANGE", key, start, stop)
}

func (p *Pipeline) ZRangeWithScores(key string, start, stop int) {
	p.Send("ZRANGE", key, start, stop, "WITHSCORES")
}

func (p *Pipeline) ZRevRangeWithScores(key string, start, stop int) {
	p.Send("ZREVRANGE", key, start, stop, "WITHSCORES")
}

// ZRangeByScore("test1", "-inf", "+inf")
func (p *Pipeline) ZRangeByScore(key string, min, max interface{}) {
	p.Send("ZRANGEBYSCORE", key, min, max)
}

func (p *Pipeline) ZRevRangeByScore(key string, min, max interface{}) {
	p.Send("ZREVRANGEBYSCORE", key, min, max)
}

func (p *Pipeline) ZRangeByScoreWithScore(key string, min, max interface{}) {
	p.Send("ZRANGEBYSCORE", key, min, max, "WITHSCORES")
}

func (p *Pipeline) ZRevRangeByScoreWithScore(key string, min, max interface{}) {
	p.Send("ZREVRANGEBYSCORE", key, min, max, "WITHSCORES")
}

func (p *Pipeline) ZRank(key, member string) {
	p.Send("ZRANK", key, member)
}

func (p *Pipeline) ZRevRank(key, member string) {
	p.Send("ZREVRANK", key, member)
}

func (p *Pipeline) ZScore(key, member string) {
	p.Send("ZSCORE", key, member)
}

func (p *Pipeline) ZRem(key string, members ...string) {
	args := make([]interface{}, 0, len(members)+1)
	args = append(args, key)
	for i := range members {
		args = append(args, members[i])
	}
	p.Send("ZREM", args...)
}

func (p *Pipeline) ZRemRangeByRank(key string, start, stop int) {
	p.Send("ZREMRANGEBYRANK", key, start, stop)
}

func (p *Pipeline) ZRemRangeByScore(key string, min, max int64) {
	p.Send("ZREMRANGEBYSCORE", key, min, max)
}

func (p *Pipeline) LPush(key string, vals ...interface{}) {
	args := make([]interface{}, len(vals)+1)
	args[0] = key
	copy(args[1:], vals)
	p.Send("LPUSH", args...)
}

func (p *Pipeline) RPush(key string, vals ...interface{}) {
	args := make([]interface{}, len(vals)+1)
	args[0] = key
	copy(args[1:], vals)
	p.Send("RPUSH", args...)
}

func (p *Pipeline) LPop(key string) {
	p.Send("LPOP", key)
}

func (p *Pipeline) RPop(key string) {
	p.Send("RPOP", key)
}

func (p *Pipeline) LLen(key string) {
	p.Send("LLEN", key)
}

func (p *Pipeline) LRem(key string, count int, val string) {
	p.Send("LREM", key, count, val)
}

func (p *Pipeline) LTrim(key string, start, stop int) {
	p.Send("LTRIM", key, start, stop)
}

func (p *Pipeline) LRange(key string, start, stop int) {
	p.Send("LRANGE", key, start, stop)
}
