package delay_queue

import (
    "fmt"
    "github.com/go-redis/redis"
    "github.com/smartystreets/goconvey/convey"
    "testing"
    "time"
)

var rdb *redis.Client

func init() {
    rdb = redis.NewClient(&redis.Options{
        Addr: "localhost:6379",
        Password: "",
        DB: 0,
    })
}

func TestZadd(t *testing.T)  {
    zadd(rdb, "test_zset", "zset_100", 100)
    zadd(rdb, "test_zset", "zset_200", 200)
    zadd(rdb, "test_zset", "zset_50", 50)
}

func TestZcard(t *testing.T) {
    fmt.Println(zcard(rdb, "test_zset").Val())
}

func TestMigrateExpiredMsg(t *testing.T) {
    migrateExpiredMsg(rdb, "test_zset", "test_list")
}

func TestLogic(t *testing.T) {
    convey.Convey("正经の测试", t, func() {
        now := time.Now()
        fmt.Println("当前时间： ", now.Format("2006-01-02 15:04:05"))
        delay_key := "t_sort_set"
        read_key := "t_read_list"
        zadd(rdb, delay_key, "预计5s过期的数据", now.Add(time.Second * 5).Unix())
        zadd(rdb, delay_key, "预计10s过期的数据", now.Add(time.Second * 10).Unix())
        zadd(rdb, delay_key, "预计2s过期的数据", now.Add(time.Second * 2).Unix())
        zadd(rdb, delay_key, "预计20s过期的数据", now.Add(time.Second * 20).Unix())
        go calMigrate(delay_key, read_key)
        go consumer(read_key)
        select {}
    })
}

func calMigrate(delayKey,readKey string) {
    // 每秒钟处理一次
    for range time.Tick(time.Second * 1) {
        //fmt.Println("处理过期数据，移入到list中，当前时间: ", time.Now().Format("2006-01-02 15:04:05"))
        migrateExpiredMsg(rdb, delayKey, readKey)
    }
}

func consumer(readKey string) {
    // 每秒钟读一次，这里可以改成chan优化一下
    for range time.Tick(time.Millisecond * 500) {
        value := rpop(rdb, readKey).Val()
        if len(value) > 0 {
            fmt.Println("读取延迟队列中的数据，当前时间: ", time.Now().Format("2006-01-02 15:04:05"), "data: ", value)
        }
    }

}