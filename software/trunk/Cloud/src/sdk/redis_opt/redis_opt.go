package redis_opt

import (
	"github.com/garyburd/redigo/redis"
)

type DuidInfo struct {
	Ipc_server string
	Port       string
	//expire_time int64 // 有效时期
}

type MsgInfo struct {
	Msg_id      int64
	Duid        int64
	Msg_data    []byte
	Expire_time int32
}

type RedisOpt struct {
	redis_conn redis.Conn
}

func CreateRedisOpt(ip_address string) *RedisOpt {
	return nil
}

func (this *RedisOpt) GetDuidData(duid int64, info *DuidInfo) (int, error) {
	return 0, nil
}

func (this *RedisOpt) SetDuidData(duid int64, info *DuidInfo, expire_time int32) (int, error) {
	return 0, nil
}

func (this *RedisOpt) AddPushMsg(duid int64, info *MsgInfo) (int, error) {
	return 0, nil
}

func (this *RedisOpt) GetPushMsg(duid int64, array *[]MsgInfo) (int, error) {
	return 0, nil
}

func (this *RedisOpt) RmPushMsg(duid int64, msg_id int64) (int, error) {
	return 0, nil
}

func (this *RedisOpt) RmPushMsgs(duid int64, array []int64) (int, error) {
	return 0, nil
}
