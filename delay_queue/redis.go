package delay_queue

import (
    "fmt"
    "github.com/go-redis/redis"
    "time"
)

// 获取延时消息数 -- 即进入sorted set中的数量
func zcard(rdb *redis.Client, key string) *redis.IntCmd {
    return rdb.ZCard(key)
}

// 获取等待执行的消息数
func llen(rdb *redis.Client, key string) *redis.IntCmd {
    return rdb.LLen(key)
}

// 推送新消息到队列<list>
func lpush(rdb *redis.Client, key string, value interface{}) error {
    return rdb.LPush(key, value).Err()
}

// 发送延时消息
func zadd(rdb *redis.Client, key string, value interface{}, delay int64) error {
    return rdb.ZAdd(key, redis.Z{Score: float64(delay), Member: value}).Err()
}

// 获取可执行消息
func rpop(rdb *redis.Client, key string) *redis.StringCmd {
    return rdb.RPop(key)
}

// 将过期消息迁移到待执行队列
func migrateExpiredMsg(rdb *redis.Client, delayKey, readKey string) error {
    // 这里其实要做三件事
    /*
        1. 查询要过期的消息
        2. 从sorted set中移除要过期的消息
        3. 将过期的消息投入到List中
    */
    // 为了保证原子性，可以使用lua脚本实现。但如果先进行查询，然后保证2，3的原子性，可以通过redis的事务来实现，试试这种写法
    // 到达过期时间的
    now := time.Now().Unix()
    values := rdb.ZRangeByScore(delayKey, redis.ZRangeBy{Min: "-inf", Max: fmt.Sprintf("%d", now)}).Val()
    pipe := rdb.TxPipeline()
    defer pipe.Close()
    for _, v := range values {
        pipe.ZRem(delayKey, v)
        pipe.LPush(readKey, v)
    }
    _, err := pipe.Exec()
    // 有错误就回滚
    if err != nil {
        pipe.Discard()
    }
    return err
}

// 使用lua脚本的也搞一下
func migrateExpiredJobsByScript(rdb *redis.Client, delayKey, readKey string) error {
    script := redis.NewScript(`
    local val = redis.call('zrangebyscore', KEYS[1], '-inf', ARGV[1], 'limit', 0, 20)
    if(next(val) ~= nil) then
      redis.call('zremrangebyrank', KEYS[1], 0, #val -1)
      redis.call('rpush', KEYS[2], unpack(val, 1, #val))
    end
    return #val
    `)
    return script.Run(rdb, []string{delayKey, readKey}, time.Now().Unix()).Err()
}