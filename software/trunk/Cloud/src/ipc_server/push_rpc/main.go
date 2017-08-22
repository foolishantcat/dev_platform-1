package main

import (
	"fmt"
	"ipc_server/ipc_comm"
	"protol"
	"sdk/comm_func"
	"sdk/logger"
	"sdk/net_server"
	"sdk/stub"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/larspensjo/config"
)

type RpcHander struct {
	redis_pool *ipc_comm.RedOptPool
	db         *ipc_comm.DbOpt
	//stub_pool  *StubPool
}

var rpc_handle RpcHander

func (this *RpcHander) OnNetAccpet(peer_ip string) (interface{}, bool, func(buf []byte) ([]byte, int)) {
	return nil, true, ipc_comm.ParseData
}

func (this *RpcHander) OnNetRecv(client net_server.INetClient, packet_buf []byte, contxt interface{}) {
	data, msg_name, ok := protol.Unpack(packet_buf)
	if !ok {
		logger.Instance().LogAppError("Unpack Failed!")
		return
	}

	// 暂时只处理推送消息
	if msg_name == "ipc.push_data" {
		var req_data protol.PushDevBatchsReq
		var res_data protol.PushDevBatchsRes
		if err := proto.Unmarshal(data, &req_data); err != nil {
			logger.Instance().LogAppError("Protobuf Unmarshal Failed!Errstring=%s", err.Error())
			client.Close()
			return
		}

		redis_opt := this.redis_pool.Get()
		res_data.BatchsResult = make([]*protol.PushDeviceRes, len(req_data.Batchs))
		var push_msg_map = make(map[int64]bool)
		for index, value := range req_data.Batchs {
			logger.Instance().LogAccInfo("ReqData!FromDuid=%lu,ToDuid=%lu,ExpireTime=%d,MsgData=%s",
				value.GetFromDuid(),
				value.GetToDuid(),
				value.GetExpireTime(),
				string(value.GetMsgData()))

			// 记录到redis中
			var push_res protol.PushDeviceRes

			defer func() {
				res_data.BatchsResult[index] = &push_res
			}()

			push_res.ReqId = proto.Int32(value.GetReqId())
			push_res.Result = proto.Int32(-1) // 失败
			if redis_opt == nil {             // 获取redis池失败
				logger.Instance().LogAppError("Get Redis Object Failed!")
				continue
			}

			var msg_inf ipc_comm.MsgInfo

			duid := value.GetToDuid()
			msg_inf.From_duid = value.GetFromDuid()
			msg_inf.Last_time_stamp = int32(time.Now().Unix())
			msg_inf.Last_time_stamp += value.GetExpireTime()
			msg_inf.Msg_data = value.GetMsgData()
			msg_inf.Msg_id = comm_func.CreateMsgId()

			logger.Instance().LogAppDebug("Push Data!")

			// 记录数据到db
			if _, err := this.db.RecordMsg(duid, &msg_inf); err != nil {
				logger.Instance().LogAppError("RecordMsg Failed!ErrString=%s", err.Error())
				continue
			}

			// 记录消息记录redis
			if err := redis_opt.AddPushMsg(duid, &msg_inf); err != nil {
				logger.Instance().LogAppError("Add Message Failed!ErrString=%s", err.Error())
				continue
			}

			push_res.Result = proto.Int32(0) // 成功
			_, ok := push_msg_map[duid]
			if !ok {
				push_msg_map[duid] = true
			}
		}

		// 应答给客户端
		data, _ = proto.Marshal(&res_data)
		res_buf := protol.Pack(data, "ipc.push_data")
		client.Send(res_buf)

		// 推送给指定的conn_mgr，发送"信号"
		for key, _ := range push_msg_map {
			var svr_occur_req protol.SvrOccurReq

			// 获取对应duid连接conn_mgr
			var duid_inf ipc_comm.DuidInfo
			ok, err := redis_opt.GetDuidData(key, &duid_inf)
			if err != nil {
				logger.Instance().LogAppError("GetDuidData Failed!Duid=%s,ErrString=%s", key, err.Error())
				continue
			}

			// 获取不到DUID
			if !ok {
				logger.Instance().LogAppError("Not Find!Duid=%v", key)
				continue
			}

			// 开始投递
			svr_occur_req.Duid = proto.Int64(key)
			req, _ := proto.Marshal(&svr_occur_req)

			var ip_address string

			ip_address = fmt.Sprintf("%s:%s", duid_inf.Ipc_server, duid_inf.Port)
			stub_pool := stub.CreateStubPool(ip_address, 1, 1, 500)
			stub := stub_pool.Get()

			defer stub_pool.Close()
			var res []byte
			res, _, err = stub.Invoke(req, "ipc.server_occur")
			if err != nil {
				logger.Instance().LogAppError("Invoke Failed!Host=%s,ErrStrting=%s", ip_address, err.Error())
				continue
			}

			// 暂时不判断返回值，只是打印出来
			var svr_occur_res protol.SvrOccurRes
			err = proto.Unmarshal(res, &svr_occur_res)
			if err != nil {
				logger.Instance().LogAppError("Proto Unmarshal Failed!ErrString=%s", err.Error())
				continue
			}

			logger.Instance().LogAppDebug("Invoke ipc.server_occur Ok!Host=%s,Result=%d", ip_address, svr_occur_res.GetResult())

		}
	}
}

func (this *RpcHander) OnNetErr(client net_server.INetClient, contxt interface{}) {

}

func main() {

	// 读取配置文件
	log_obj := logger.Instance()
	var conf net_server.Config
	if err := log_obj.Load("./conf/conf.xml"); err != nil {
		return
	}
	ini_conf, err := config.ReadDefault("./conf/server.ini")
	if err != nil {
		log_obj.LogAppError("Load Config Failed!ErrString=%s", err.Error())
		return
	}

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

	rpc_handle.redis_pool = ipc_comm.CreateRedsOptPool(redis_address, 1, 1, opt_time)
	if rpc_handle.redis_pool == nil {
		logger.Instance().LogAppError("Create Redis Failed!")
		return
	}
	logger.Instance().LogAppInfo("Create Redis Ok!")

	// db_host
	if !ini_conf.HasOption("db", "dns") {
		log_obj.LogAppError("Has No[db.dns] Section!")
		return
	}
	db_host, _ := ini_conf.String("db", "dns")

	// db_port
	if !ini_conf.HasOption("db", "port") {
		log_obj.LogAppError("Has No[db.port] Section!")
		return
	}
	db_port, _ := ini_conf.String("db", "port")
	db_host += ":"
	db_host += db_port

	// user_name
	if !ini_conf.HasOption("db", "username") {
		log_obj.LogAppError("Has No[db.username] Section!")
		return
	}
	user_name, _ := ini_conf.String("db", "username")

	// password
	if !ini_conf.HasOption("db", "password") {
		log_obj.LogAppError("Has No[db.password] Section!")
		return
	}
	db_passwd, _ := ini_conf.String("db", "password")

	if !ini_conf.HasOption("db", "dbname") {
		log_obj.LogAppError("Has No[db.dbname] Section!")
		return
	}
	db_name, _ := ini_conf.String("db", "dbname")

	log_obj.LogAppInfo("Init Db!DbHost=%s,UserName=%s,Passwd=%s,DbName=%s",
		db_host,
		user_name,
		db_passwd,
		db_name)

	rpc_handle.db, err = ipc_comm.CreateDbObj(db_host, user_name, db_passwd, db_name)
	if err != nil {
		log_obj.LogAppError("CreateDbObj Failed!String=%s", err.Error())
		return
	}

	if !conf.LoadFile("./conf/server.ini") {
		log_obj.LogAppError("Load File Failed!")
		return
	}
	ipc_server_obj := net_server.CreateNetServer(&rpc_handle)
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
