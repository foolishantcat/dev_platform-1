package redis_opt

import (
	"sdk/logger"
	"sdk/serialize"
	"time"

	"github.com/garyburd/redigo/redis"
)

type DuidInfo struct {
	Ipc_server string
	Port       string
	//expire_time int64 // 有效时期
}

func (this *DuidInfo) Serialize(arc *serialize.Archive) {
	arc.PushString(this.Ipc_server)
	arc.PushString(this.Port)
}

func (this *DuidInfo) UnSerialize(arc *serialize.Archive) error {
	var err error

	this.Ipc_server, err = arc.PopString()
	this.Port, err = arc.PopString()

	return err
}

type MsgInfo struct {
	Msg_id          int64
	Msg_len         int32
	Msg_data        [256]byte
	Last_time_stamp int32
}

func (this *MsgInfo) Serialize(arc *serialize.Archive) {
	arc.PushInt64(this.Msg_id)
	arc.PushInt32(this.Msg_len)
	arc.PushArray(this.Msg_data[:])
	arc.PushInt32(this.Last_time_stamp)
}

func (this *MsgInfo) UnSerialize(arc *serialize.Archive) error {
	var err error
	var data []byte
	this.Msg_id, err = arc.PopInt64()
	if err != nil {
		return err
	}

	this.Msg_len, err = arc.PopInt32()
	if err != nil {
		return err
	}

	data, err = arc.PopArray(256)
	if err != nil {
		return err
	}
	copy(this.Msg_data[:], data)

	this.Last_time_stamp, err = arc.PopInt32()
	return err
}

func isDev() bool {
	return true
}
func getPrexRedisKey() string {
	if isDev() {
		return "debug_"
	}

	return "release_"
}

type RedisOpt struct {
	redis_conn     redis.Conn
	prex_redis_key string
}

func CreateRedisOpt(ip_address string, redis_opt_time int) *RedisOpt {
	log_obj := logger.Instance()
	duration_time := time.Duration(redis_opt_time) * time.Second
	redis_conn, err := redis.DialTimeout("tcp", ip_address, duration_time, duration_time, duration_time)
	if err != nil {
		log_obj.LogAppError("Connect Redis Failed!Errstring=%s", err.Error())

		return nil
	}

	var redis_opt RedisOpt

	redis_opt.redis_conn = redis_conn
	redis_opt.prex_redis_key = getPrexRedisKey()
	return &redis_opt
}

func (this *RedisOpt) GetDuidData(duid string, info *DuidInfo) (bool, error) {
	redis_key := this.prex_redis_key
	redis_key += duid
	redis_key += "_dev"
	reply, err := this.redis_conn.Do("GET", redis_key)
	if err != nil {
		return false, err
	}

	// Not Find
	if reply == nil {
		return false, nil
	}

	var buf []byte
	buf, err = redis.Bytes(reply, err)
	if err != nil {
		return false, err
	}

	err = serialize.UnSerializeData(buf, info)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (this *RedisOpt) SetDuidData(duid string, info *DuidInfo, expire_time int) error {
	redis_key := this.prex_redis_key
	redis_key += duid
	redis_key += "_dev"
	buf := serialize.SerializeData(info)
	_, err := this.redis_conn.Do("SET", redis_key, buf)
	return err
}

func (this *RedisOpt) RmDuidData(duid string) error {
	redis_key := this.prex_redis_key
	redis_key += duid
	redis_key += "_dev"
	_, err := this.redis_conn.Do("DEL", redis_key)
	return err
}

func (this *RedisOpt) AddPushMsg(duid string, info *MsgInfo, expire_time int) error {
	redis_key := this.prex_redis_key
	redis_key += duid
	redis_key += "_msg"
	buf := serialize.SerializeData(info)

	_, err := this.redis_conn.Do("ZADD", redis_key, info.Msg_id, buf)
	if err != nil {
		return err
	}

	_, err = this.redis_conn.Do("Expire", redis_key, expire_time)
	return err
}

func (this *RedisOpt) GetPushMsg(duid string, last_msg_id int64) ([]MsgInfo, error) {
	redis_key := this.prex_redis_key
	redis_key += duid
	redis_key += "_msg"

	// 先删除小于msg_id的消息
	reply, err := this.redis_conn.Do("Zremrangebyscore", redis_key, 0, last_msg_id)
	if err != nil {
		return nil, err
	}

	// 获取大于msg_id的
	reply, err = this.redis_conn.Do("Zrangebyscore", redis_key, last_msg_id, -1)
	if err != nil {
		return nil, err
	}

	// 转化为数组
	var buf_array [][]byte
	buf_array, err = redis.ByteSlices(reply, err)
	if err != nil {
		return nil, err
	}

	// 开始进行反序列化处理
	var msg_array []MsgInfo = make([]MsgInfo, 0)
	for _, value := range buf_array {
		var msg_inf MsgInfo

		err := serialize.UnSerializeData(value, &msg_inf)
		if err != nil {
			log_obj := logger.Instance()
			log_obj.LogAppError("UnSerializeData Failed!ErrString=%s", err.Error())
			return nil, err
		}

		// 判断消息是否已经过期
		if msg_inf.Last_time_stamp < int32(time.Now().Unix()) {
			log_obj := logger.Instance()
			log_obj.LogAppInfo("Msg TimeOut!Duid=%s,MsgId=%d", duid, msg_inf.Msg_id)
			continue
		}
		msg_array = append(msg_array, msg_inf)
	}

	return msg_array, nil
}

func (this *RedisOpt) RmPushMsg(duid string, msg_id int64) (int, error) {
	return 0, nil
}

func (this *RedisOpt) RmPushMsgs(duid string, array []int64) (int, error) {
	return 0, nil
}
