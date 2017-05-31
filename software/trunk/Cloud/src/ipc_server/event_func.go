package main

import (
	"protol"
	"sdk/logger"
	"sdk/net_server"
	"sdk/redis_opt"
	"time"

	"github.com/golang/protobuf/proto"
)

const (
	key_expire_time = 24 * 60 * 60
)

func EventDevOnline(packet_buf []byte,
	ser_obj *ServerLogic,
	client net_server.INetClient,
	contxt interface{}) {

	var req protol.DevOnline

	log_obj := logger.Instance()
	err := proto.Unmarshal(packet_buf, &req)
	if err != nil {
		log_obj.LogAppError("Pb Unmarshaled Failed!MessageName=DevOnline, ErrString=%s", err.Error())
		client.Close()
		return
	}

	// 更新状态
	duid := req.GetDuid()
	last_msg_id := req.GetLastMsgId()
	data_opt := ser_obj.GetValidOpt(duid)

	var duid_inf redis_opt.DuidInfo
	var msg_datas []redis_opt.MsgInfo

	duid_inf.Ipc_server = conf.GetBinderAddr()
	duid_inf.Port = conf.GetBinderPort()
	{
		data_opt.opt_lock.Lock()
		defer data_opt.opt_lock.Unlock()

		redis_opt := data_opt.redis
		err = redis_opt.SetDuidData(duid, &duid_inf, 10) // 激活duid信息
		if err != nil {
			log_obj.LogAppError("redis_opt SetDuidData Failed! ErrString=%s", err.Error())
			return
		}

		// 记录连接信息
		data_opt.conns_map[duid] = client

		// 获取当前需要推送消息
		msg_datas, err = redis_opt.GetPushMsg(duid, last_msg_id)
		if err != nil {
			log_obj.LogAppError("redis_opt GetPushMsg Failed! ErrString=%s", err.Error())
			return
		}
	}

	if len(msg_datas) > 0 { // 开始进行推送
		var res protol.DataResBatchs
		res.DataBatchs = make([]*protol.DataRes, len(msg_datas))

		for _, value := range msg_datas {
			var data protol.DataRes

			data.Duid = proto.String(duid)
			data.MsgData = value.Msg_data[:]
			data.MsgId = proto.Int64(value.Msg_id)

			res.DataBatchs = append(res.DataBatchs, &data)
		}

		res_data, err := proto.Marshal(&res)
		if err != nil {
			log_obj.LogAppError("Protol Marshal Failed!ErrString=%s", err.Error())
			return
		}
		buff := protol.Pack(res_data, "ipc.pull_data")
		client.Send(buff)
	}
}

func EventDevOffline(packet_buf []byte,
	ser_obj *ServerLogic,
	client net_server.INetClient,
	contxt interface{}) {

	// 离线消息
	var req protol.DevOffline
	log_obj := logger.Instance()

	err := proto.Unmarshal(packet_buf, &req)
	if err != nil {
		log_obj.LogAppError("Proto UnmarShal Failed!ErrString=%s", err.Error())
		return
	}

	log_obj.LogAppInfo("Device Offline!Duid=%s", req.GetDuid())

	// 删除连接信息
	duid := req.GetDuid()
	data_opt := ser_obj.GetValidOpt(duid)

	defer data_opt.opt_lock.Unlock()
	data_opt.opt_lock.Lock()

	redis_opt := data_opt.redis
	if err := redis_opt.RmDuidData(duid); err != nil {
		log_obj.LogAppError("Rm Duid Failed!Duid=%s,ErrString=%s", duid, err.Error())
	}
	delete(data_opt.conns_map, duid)

}

func EventDevAlive(packet_buf []byte,
	ser_obj *ServerLogic,
	client net_server.INetClient,
	contxt interface{}) {

	var req protol.DevsAlive
	var res_batchs protol.DataResBatchs // 应答推送的消息
	var msg_array []redis_opt.MsgInfo

	res_batchs.DataBatchs = make([]*protol.DataRes, 0)
	log_obj := logger.Instance()
	err := proto.Unmarshal(packet_buf, &req)
	if err != nil {
		log_obj.LogAppError("Proto UnmarShal Failed!ErrString=%s", err.Error())
		return
	}

	//next_time := req.GetNetTimeSec()
	devs_stat := req.GetDevOnline()
	expire_time := 60 // 默认有效连接为60s

	for _, value := range devs_stat {

		// 更新当前连接信息
		duid := value.GetDuid()
		last_msg_id := value.GetLastMsgId()
		data_opt := ser_obj.GetValidOpt(duid)
		redis := data_opt.redis

		var duid_inf redis_opt.DuidInfo
		duid_inf.Ipc_server = conf.GetBinderAddr()
		duid_inf.Port = conf.GetBinderPort()

		{
			data_opt.opt_lock.Lock()
			defer data_opt.opt_lock.Unlock()

			// 更新连接信息
			data_opt.conns_map[duid] = client

			if err := redis.SetDuidData(duid, &duid_inf, expire_time); err != nil {
				log_obj.LogAppError("SetDuidData() Failed!ErrString=%s", err.Error())
			}

			// 获取duid推送的消息
			if msg_array, err = redis.GetPushMsg(duid, last_msg_id); err != nil {
				log_obj.LogAppError("GetPushMsg() Falied, ErrString=s", err.Error())
				return
			}
		}

		log_obj.LogAppDebug("*********Duid=%s,Message Size=%d**********",
			duid,
			len(msg_array))
		for _, value := range msg_array {
			log_obj.LogAppDebug("Message Info!msg_id=%d,Data=%s",
				value.Msg_id,
				string(value.Msg_data[:]))

			// 构造消息进行推送
			var data_res protol.DataRes

			data_res.Duid = proto.String(duid)
			data_res.MsgId = proto.Int64(value.Msg_id)
			data_res.MsgData = value.Msg_data[:]

			res_batchs.DataBatchs = append(res_batchs.DataBatchs, &data_res)
		}
	}

	if len(res_batchs.DataBatchs) > 0 {
		data, err := proto.Marshal(&res_batchs)
		if err != nil {
			log_obj.LogAppDebug("proto MarShal Failed!ErrString=%s", err.Error())
			return
		}

		send_buf := protol.Pack(data, "ipc.pull_data")
		client.Send(send_buf)
	}

}

func EventPullData(packet_buf []byte,
	ser_obj *ServerLogic,
	client net_server.INetClient,
	contxt interface{}) {

	var req protol.DataReq
	var msg_array []redis_opt.MsgInfo
	log_obj := logger.Instance()

	err := proto.Unmarshal(packet_buf, &req)
	if err != nil {
		log_obj.LogAppError("Unmarshal() Failed!ErrString=%s", err.Error())
		return
	}

	duid := req.GetDuid()
	last_msg_id := req.GetLatMsgId()
	data_opt := ser_obj.GetValidOpt(duid)
	{
		data_opt.opt_lock.Lock()
		defer data_opt.opt_lock.Unlock()

		// 获取消息
		if msg_array, err = data_opt.redis.GetPushMsg(duid, last_msg_id); err != nil {
			log_obj.LogAppError("Get Push-Msg Failed!Duid=%s,ErrString=%s", duid, err.Error())
			return
		}
	}

	if len(msg_array) > 0 {
		var res_batchs protol.DataResBatchs

		res_batchs.DataBatchs = make([]*protol.DataRes, len(msg_array))
		for index, value := range msg_array {
			var data_res protol.DataRes

			data_res.Duid = proto.String(duid)
			data_res.MsgId = proto.Int64(value.Msg_id)
			data_res.MsgData = value.Msg_data[:]

			res_batchs.DataBatchs[index] = &data_res
		}

		proto_buf, _ := proto.Marshal(&res_batchs)
		send_buf := protol.Pack(proto_buf, "ipc.pull_data")
		client.Send(send_buf)
	}

}

func EventPushData(packet_buf []byte,
	ser_obj *ServerLogic,
	client net_server.INetClient,
	contxt interface{}) {

	// 设备推送数据
	var req protol.PushDeviceReq
	var res protol.PushDeviceRes
	var push_client net_server.INetClient
	var ok bool

	log_obj := logger.Instance()
	if err := proto.Unmarshal(packet_buf, &req); err != nil { // 程序开发者的问题，所以没有返回
		log_obj.LogAppError("Unmarshar Failed!ErrString=%s", err.Error())
		return
	}

	// 存入到redis中
	duid := req.GetDuid()
	data_opt := ser_obj.GetValidOpt(duid)
	redis := data_opt.redis
	res.Duid = proto.String(duid)
	res.Result = proto.Int32(RESULT_OK)
	{
		data_opt.opt_lock.Lock()
		defer data_opt.opt_lock.Unlock()

		// 存入到db

		//  存入到redis
		var msg_inf redis_opt.MsgInfo

		msg_inf.Last_time_stamp = req.GetExpireTime() + int32(time.Now().Unix())
		copy(msg_inf.Msg_data[:], req.GetMsgData())
		msg_inf.Msg_id = 1 // 获取消息MsdId

		if err := redis.AddPushMsg(duid, &msg_inf, key_expire_time); err != nil {
			log_obj.LogAppError("Add Push-Msg Failed!Duid=%s,ErrString=%s", duid, err.Error())
			res.Result = proto.Int32(OPT_REDIS_ERR)
		}

		// 能否找到对应推送客户端
		push_client, ok = data_opt.conns_map[duid]
	}

	if !ok {
		// 没有找到记录下来，但是不在继续推送了，后续需要优化
		log_obj.LogAppWarn("Cann't Find PushClient In Server!Duid=%s", duid)
		res.Result = proto.Int32(DEVICE_NOT_IN_SVR)
	} else {

		// 推送设备
		var data_occur protol.DataOccur

		data_occur.Duid = proto.String(duid)
		proto_buf, _ := proto.Marshal(&data_occur)
		data_buf := protol.Pack(proto_buf, "ipc.data_occur")
		push_client.Send(data_buf)
	}

	proto_buf, _ := proto.Marshal(&res)
	data_buf := protol.Pack(proto_buf, "ipc.push_data")
	client.Send(data_buf)
}
