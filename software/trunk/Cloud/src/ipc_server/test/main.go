package main

import (
	"fmt"
	//"ipc_server/ipc_comm"
	"protol"
	//"sdk/comm_func"
	"sdk/logger"
	//"sdk/net_server"
	"sdk/stub"
	"time"

	"github.com/golang/protobuf/proto"
)

func main() {
	if err := logger.Instance().Load("./conf/conf.xml"); err != nil {
		fmt.Printf("Load Config File Failed!ErrString=%s", err.Error())
		return
	}

	ip_address := "127.0.0.1"
	ip_address += ":10001"

	// 创建stub
	stub_pool := stub.CreateStubPool(ip_address, 1, 1, 3)
	stub := stub_pool.Get()

	var req protol.PushDevBatchsReq
	var value protol.PushDeviceReq
	var res protol.PushDevBatchsRes

	req.Batchs = make([]*protol.PushDeviceReq, 0)
	value.FromDuid = proto.Int64(1221)
	value.ToDuid = proto.Int64(232123)
	value.MsgData = []byte("hello world")
	value.ExpireTime = proto.Int32(3)
	value.ReqId = proto.Int32(int32(time.Now().Unix()))

	req.Batchs = append(req.Batchs, &value)

	_, err := stub.InvokePb(&req, "ipc.push_data", &res)
	if err != nil {
		logger.Instance().LogAppError("Invoke Failed!")
		return
	}

	for _, value := range res.BatchsResult {
		logger.Instance().LogAppInfo("Result=%d,ReqId=%d",
			value.GetResult(),
			value.GetReqId())
	}

	logger.Instance().Close()

}
