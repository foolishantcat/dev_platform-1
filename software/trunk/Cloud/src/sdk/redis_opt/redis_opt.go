package redis_opt

type DuidInfo struct {
	ipc_server  string
	port        string
	expire_time int64 // 有效时期
}

type MsgInfo struct {
}

type RedisOpt struct {
}

func CreateRedisOpt(ip_address string) *RedisOpt {

}

func GetDuidData(duid int64, info *DuidInfo) (int, error) {

}

func SetDuidData(duid int64, info *DuidInfo) (int, error) {

}

func AddPushMsg(duid int64, info *MsgInfo) (int, error) {

}

func GetPushMsg(duid int64, array *[]MsgInfo) (int, error) {

}

func RmPushMsg(duid int64, msg_id int64) (int, error) {

}

func RmPushMsgs(duid int64, array []int64) (int, error) {

}
