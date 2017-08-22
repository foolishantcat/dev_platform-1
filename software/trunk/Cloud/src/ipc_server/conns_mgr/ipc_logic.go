package main

import (
	"ipc_server/ipc_comm"
	"protol"
	"runtime"
	"sdk/logger"
	"sdk/net_server"
	"sync"
	"time"

	"github.com/larspensjo/config"
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
	conns_map map[int64]net_server.INetClient
	redis     *ipc_comm.RedisOpt
	opt_lock  *sync.Mutex
}

type ServerLogic struct {
	data_opt   []DataOpt
	db_opt     *ipc_comm.DbOpt // 连接池，所以定义全局
	redis_pool *ipc_comm.RedOptPool
}

type event_func func(packet_buf []byte, ser_obj *ServerLogic, client net_server.INetClient, contxt interface{})

var event_map map[string]event_func = map[string]event_func{
	"ipc.dev_offline":  EventDevOffline,
	"ipc.devs_online":  EventDevOnline,
	"ipc.devs_alive":   EventDevAlive,
	"ipc.pull_data":    EventPullData,
	"ipc.push_data":    EventPushData,
	"ipc.server_occur": EventServerOccur,
}

func (this *ServerLogic) OnNetAccpet(peer_ip string) (interface{}, bool, func(buf []byte) ([]byte, int)) {
	log_obj := logger.Instance()
	log_obj.LogAppDebug("New Connect Accpet!PeerIP=%s", peer_ip)
	return nil, true, ipc_comm.ParseData
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
	size := runtime.NumCPU()
	this.data_opt = make([]DataOpt, size)

	ini_conf, err := config.ReadDefault("./conf/server.ini")
	if err != nil {
		log_obj.LogAppError("Load Config Failed!ErrString=%s", err.Error())
		return
	}

	// 获取db配置信息
	// db_address string, login_name string, password string, db_name string
	if !ini_conf.HasOption("db", "dns") {
		log_obj.LogAppError("Has No [db.dns] Section!")
		return
	}
	db_address, _ := ini_conf.String("db", "dns")

	if !ini_conf.HasOption("db", "port") {
		log_obj.LogAppError("Has No [db.port] Section!")
		return
	}
	db_port, _ := ini_conf.String("db", "port")

	db_address += ":"
	db_address += db_port

	if !ini_conf.HasOption("db", "username") {
		log_obj.LogAppError("Has No [db.username] Section!")
		return
	}
	user_name, _ := ini_conf.String("db", "username")

	if !ini_conf.HasOption("db", "password") {
		log_obj.LogAppError("Has No [db.password] Section!")
		return
	}
	password, _ := ini_conf.String("db", "password")

	if !ini_conf.HasOption("db", "dbname") {
		log_obj.LogAppError("Has No [db.dbname] Section!")
		return
	}
	db_name, _ := ini_conf.String("db", "dbname")

	log_obj.LogAppInfo("DataBase Conf!DbAddress=%s,UserName=%s,password=%s,DbName=%s",
		db_address,
		user_name,
		password,
		db_name)

	this.db_opt, err = ipc_comm.CreateDbObj(db_address, user_name, password, db_name)
	if err != nil {
		log_obj.LogAppError("Create DbObj Failed!ErrString=%s", err.Error())
		return
	}

	log_obj.LogAppInfo("Create Data Base Ok!")

	// 获取redis配置信息
	if !ini_conf.HasOption("redis", "host") {
		log_obj.LogAppError("Has No [redis.host] Section!")
		return
	}
	redis_address, _ := ini_conf.String("redis", "host")

	if !ini_conf.HasOption("redis", "port") {
		log_obj.LogAppError("Has No [redis.port] Section!")
		return
	}
	port, _ := ini_conf.String("redis", "port")

	if !ini_conf.HasOption("redis", "opt_time") {
		log_obj.LogAppError("Has No [redis.opt_time] Section!")
		return
	}
	opt_time, _ := ini_conf.Int("redis", "opt_time")

	redis_address += ":"
	redis_address += port

	// 创建redis池
	redis_pool := ipc_comm.CreateRedsOptPool(redis_address, size, size, opt_time)
	if redis_pool == nil {
		log_obj.LogAppError("Create Redis Pool Faild!RedisAdress=%s", redis_address)
		return
	}

	this.redis_pool = redis_pool
	log_obj.LogAppInfo("Redis Conf!RedisAddress=%s,OptTime=%d", redis_address, opt_time)
	for i := 0; i < len(this.data_opt); i++ {
		redis := redis_pool.Get()
		if redis == nil {
			log_obj.LogAppError("Get Redis Init Failed!")
			return
		}

		this.data_opt[i].conns_map = make(map[int64]net_server.INetClient)
		this.data_opt[i].opt_lock = &sync.Mutex{}
		this.data_opt[i].redis = redis
	}

	log_obj.LogAppInfo("Create Redis Ok!")

	if !conf.LoadFile("./conf/server.ini") {
		log_obj.LogAppError("Load File Failed!")
		return
	}
	ipc_server_obj := net_server.CreateNetServer(this)
	if !ipc_server_obj.Start(&conf) {
		log_obj.LogAppError("ipc_server Start Failed!")
		return
	}

	log_obj.LogAppInfo("Server Start!")
	for {
		time.Sleep(10 * time.Second)
		// 不做任何处理
	}
}

func (this ServerLogic) GetValidOpt(duid int64) *DataOpt {
	index := int(duid) % len(this.data_opt)
	return &this.data_opt[index]
}
