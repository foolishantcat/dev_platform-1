package main

import (
	"protol"
	"sdk/logger"
	"sdk/net_server"
	"sdk/redis_opt"
	"sync"
	"time"
	"unsafe"

	//"github.com/golang/protobuf/proto"
)

var conf net_server.Config

const (
	RESULT_OK         = 0
	OPT_DB_ERR        = -1
	OPT_REDIS_ERR     = -2
	DEVICE_NOT_IN_SVR = -3
)

type DataOpt struct {
	conns_map map[string]net_server.INetClient
	redis     *redis_opt.RedisOpt
	opt_lock  *sync.Mutex
}

type ServerLogic struct {
	data_opt []DataOpt
}

type event_func func(packet_buf []byte, ser_obj *ServerLogic, client net_server.INetClient, contxt interface{})

var event_map map[string]event_func = map[string]event_func{
	"ipc.dev_offline": EventDevOffline,
	"ipc.devs_online": EventDevOnline,
	"ipc.devs_alive":  EventDevAlive,
	"ipc.pull_data":   EventPullData,
	"ipc.push_data":   EventPushData,
}

func ParseData(buf []byte) ([]byte, int) {


	log_obj:= logger.Instance()
	log_obj.LogAppDebug("Come in")

	// 先收头部
	if len(buf) >= int(unsafe.Sizeof(int32(0))) {
		var packet_len int

		packet_len = (int)(*(*int32)(unsafe.Pointer(&buf[0])))
		packet_len += int(unsafe.Sizeof(int32(0)))
		if len(buf) >= packet_len { // 加上packet_len
			packet_buf := make([]byte, 0, packet_len)
			copy(packet_buf, buf)

			return packet_buf, packet_len
		}
	}

	return nil, 0
}

func (this *ServerLogic) OnNetAccpet(peer_ip string) (interface{}, bool, func(buf []byte) ([]byte, int)) {
	return nil, true, ParseData
}

func (this *ServerLogic) OnNetRecv(client net_server.INetClient, packet_buf []byte, contxt interface{}) {

	log_obj := logger.Instance()

	// 开始进行解包
	data, msg_name, ok := protol.Unpack(packet_buf)
	if !ok {
		log_obj.LogAppError("UnPacket Failed!PeerAddr=%s", client.GetPeerAddr())
		// 断开连接
		client.Close()
	}

	event_func, ok := event_map[msg_name]
	if !ok {
		log_obj.LogAppWarn("Not Find Method!MsgName=%s", msg_name)
		return
	}
	event_func(data, this, client, contxt)
}

func (this *ServerLogic) OnNetErr(client net_server.INetClient, contxt interface{}) {

}

func (this *ServerLogic) Initialize() {

	log_obj := logger.Instance()

	// 初始化相关的环境操作
	size := 8
	this.data_opt = make([]DataOpt, size)
	redis_address := "127.0.0.1:12306"

	for i := 0; i < len(this.data_opt); i++ {
		redis := redis_opt.CreateRedisOpt(redis_address, 10)
		if redis == nil {
			log_obj.LogAppError("Redis Init Failed!")
			return
		}

		this.data_opt[i].conns_map = make(map[string]net_server.INetClient)
		this.data_opt[i].opt_lock = &sync.Mutex{}
		this.data_opt[i].redis = redis
	}

	if !conf.LoadFile("./conf/server.ini"){
	log_obj.LogAppError("Load File Failed!")
	return		
	}
	ipc_server_obj := net_server.CreateNetServer(this)
	if !ipc_server_obj.Start(&conf) {
		log_obj.LogAppError("ipc_server Start Failed!")
		return
	}

	for {
		time.Sleep(10 * time.Second)
		// 不做任何处理
	}
}

func (this ServerLogic) GetValidOpt(duid string) *DataOpt {
	data_buf := []byte(duid)
	hash_key := 0
	for _, value := range data_buf {
		hash_key += int(value)
	}
	index := hash_key % len(this.data_opt)
	return &this.data_opt[index]
}
