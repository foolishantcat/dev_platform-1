package main

import (
	"protol"
	"sdk/logger"
	"sdk/net_server"
	"sdk/redis_opt"
	"sync"
	"unsafe"

	"github.com/golang/protobuf/proto"
)

type DataOpt struct {
	conns_map map[int64]net_server.INetClient
	redis     *redis_opt.RedisOpt
	opt_lock  *sync.Mutex
}

type ServerLogic struct {
	data_opt []DataOpt
}

func EventDevOnline(packet_buf []byte,
	ser_obj *ServerLogic,
	client net_server.INetClient,
	contxt interface{}) {

	var req protol.DevOnline

	log_obj := logger.Instance()
	err := proto.Unmarshal(packet_buf, &req)
	if err != nil {
		log_obj.LogAppErr("Pb Unmarshaled Failed!MessageName=DevOnline, ErrString=%s", err.Error())
		client.Close()
		return
	}

	duid := req.GetDuid()
	index := duid % len(ser_obj.data_opt)
	defer ser_obj.data_opt[index].opt_lock.Unlock()

	ser_obj.data_opt[index].opt_lock.Unlock()
	var msg_inf redis_opt.MsgInfo
	var nums int

	// 获取推送的信息
	redis_opt := ser_obj.data_opt[index].redis
	nums, err = redis_opt.GetDuidData(duid, &duid_inf)
	if err != nil {
		log_obj.LogAppErr("redis_opt GetDuidData Failed! ErrString=%s", err.Error())
		return
	}

}

func EventDevOffline(packet_buf []byte,
	ser_obj *ServerLogic,
	client net_server.INetClient,
	contxt interface{}) {

}

func EventDevAlive(packet_buf []byte,
	ser_obj *ServerLogic,
	client net_server.INetClient,
	contxt interface{}) {

}

func EvenPullData(packet_buf []byte,
	ser_obj *ServerLogic,
	client net_server.INetClient,
	contxt interface{}) {

}

func EventPushData(packet_buf []byte,
	ser_obj *ServerLogic,
	client net_server.INetClient,
	contxt interface{}) {

}

type event_func func(packet_buf []byte, ser_obj *ServerLogic, client net_server.INetClient, contxt interface{})
var event_map map[string]event_func = {
	"ipc.dev_offline":EventDevOffline,
	"ipc.devs_online":EventDevOnline,
	"ipc_pull_data":EventPushData,
}

func ParseData(buf []byte) ([]byte, int) {

	// 先收头部
	if len(buf) >= unsafe.Sizeof(int32(0)) {
		var packet_len int

		packet_len = (int)(*(*int32)(unsafe.Pointer(&buf[0])))
		packet_len += unsafe.Sizeof(int32(0))
		if len(buf) >= packet_len { // 加上packet_len
			packet_buf := make([]byte, 0, packet_len)
			copy(pack_buf, buf, packet_len)

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
		log_obj.LogAppErr("UnPacket Failed!PeerAddr=%s", client.GetPeerAddr())
		// 断开连接
		client.Close()
	}
}

func (this *ServerLogic) OnNetErr(client net_server.INetClient, contxt interface{}) {

}

func (this *ServerLogic) Initialize() {
	var conf net_server.Config
	log_obj := logger.Instance()

	if !conf.LoadFile("../conf/server.json") {
		log_obj.Printf("Load Config File Failed!")
		return
	}

	// 初始化相关的环境操作
	size := 8
	this.data_opt = make([]data_opt, size)
	redis_address := "127.0.0.1"

	for i := 0; i < len(this.data_opt); i++ {
		redis = redis_opt.CreateRedisOpt(redis_address)
		if redis != nil {
			logger.LogAppErr("Redis Init Failed!")
			return
		}

		this.data_opt[i].conns_map = make(map[uint64]net_server.INetClient)
		this.data_opt[i].opt_lock = &sync.Mutex{}
		this.data_opt[i].redis = redis
	}

	ipc_server_obj := net_server.CreateNetServer(&conf)
	if !ipc_server_obj.Start(&conf) {
		logger.LogAppErr("ipc_server Start Failed!")
		return
	}

	for {
		time.Sleep(time.Now().Second() + 10*time.Second)
		// 不做任何处理
	}

}
