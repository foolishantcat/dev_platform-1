package conn_pool

import (
	"container/list"
	"sdk/logger"
	"sync/atomic"
	"time"
)

const (
	BIG_LONG_TIME = 0x0fffffff
)

type ConnsCfg struct {
	Max_packet_size int
	Heart_bit       *[]byte
	Alram_time      int
	Conns_nums      int
	Set_func        ConnFunc
}

type IConnPool interface {
	AddConns(net_addr string)
	RmConns(net_addr string)
	Send(net_addr string, buf []byte)
	Send2(connect_id int64, buf []byte)
	Stop()
}

/*
type connsValue struct {
	//set_func ConnFunc
	conns *list.List
}
*/

type connsValue struct {
	conn       *cConn
	connect_id int64
}

type cConnPool struct {
	protol_name  string
	conns_map    map[string]*list.List
	conns_id_map map[int64]*cConn
	hub_chann    chan hubEvent
	close_chann  chan int
	conns_conf   ConnsCfg
	auto_incs    int64
}

func (this *cConnPool) AddConns(net_addr string) {
	hub_event := hubEvent{
		event_type: ADD_CONNS,
		params:     &net_addr,
	}

	this.sendEvent(&hub_event, 10)
}

func (this *cConnPool) RmConns(net_addr string) {
	hub_event := hubEvent{
		event_type: RM_CONNS,
		params:     net_addr,
	}

	this.sendEvent(&hub_event, 10)
}
func (this *cConnPool) Send(net_addr string, buf []byte) {

	send_conns := sendConns{
		net_addr: net_addr,
		send_buf: buf,
	}

	hub_event := hubEvent{
		event_type: SEND_CONNS,
		params:     &send_conns,
	}

	this.sendEvent(&hub_event, 10)
}

func (this *cConnPool) Send2(connect_id int64, buf []byte) {
	send_conns := sendConns2{
		connect_id: connect_id,
		send_buf:   buf,
	}

	hub_event := hubEvent{
		event_type: SEND_CONNS2,
		params:     &send_conns,
	}

	this.sendEvent(&hub_event, 10)
}

func (this *cConnPool) sendEvent(event *hubEvent, wait_time int) {

	time_chann := make(chan int, 1)
	log_obj := logger.Instance()

	//uint32 uTimeSec = uint32(wait_time)

	go func() {
		time.Sleep(time.Duration(wait_time) * time.Second) // Sleep参数如果小于0，则立即返回
		time_chann <- 1
	}()

	select {
	case this.hub_chann <- *event:
		break
	case <-time_chann:
		// 插入失败,超时
		log_obj.LogSysWarn("Add event failed!protol_name=%s,event_type=%d, hubevent size=%d", this.protol_name, event.event_type, len(this.hub_chann))
		break
	}
}

const (
	ADD_CONNS   = 1
	RM_CONNS    = 2
	SEND_CONNS  = 3
	SEND_CONNS2 = 4
	AF_CONNS    = 5
	CLOSE_CONNS = 6
	ALRAM_CONNS = 7
)

type hubEvent struct {
	event_type int
	params     interface{}
}

type addConns struct {
	net_addr string
	set_func ConnFunc
}

type afterConns struct {
	net_addr    string
	conn_client *cConn
	connect_id  int64
}

type sendConns struct {
	net_addr string
	send_buf []byte
}

type sendConns2 struct {
	connect_id int64
	send_buf   []byte
}

func doAddConns(pool *cConnPool, params interface{}) {
	log_obj := logger.Instance()

	net_addr := params.(*string)
	_, ok := pool.conns_map[*net_addr]
	if !ok {
		pool.conns_map[*net_addr] = list.New()

		log_obj.LogSysDebug("Add New Connecion Pool!RemoteIp=%s", *net_addr)

		go func() {
			connect_id := atomic.AddInt64(&pool.auto_incs, 1)
			for i := 0; i < pool.conns_conf.Conns_nums; i++ {
				conn := createConn(*net_addr,
					pool.conns_conf.Max_packet_size,
					pool.conns_conf.Set_func,
					pool,
					connect_id)
				if conn != nil {

					// 投递消息，告知连接创建成功
					hub_event := hubEvent{
						event_type: AF_CONNS,
						params: &afterConns{
							net_addr:    *net_addr,
							conn_client: conn,
							connect_id:  connect_id,
						},
					}
					pool.hub_chann <- hub_event
				}
			}
		}()
	}
}

func doSendData(pool *cConnPool, params interface{}) {
	send_conns := params.(*sendConns)
	conns_lst, ok := pool.conns_map[send_conns.net_addr]
	if ok {
		// 没有可用的连接
		if conns_lst.Len() <= 0 {
			// 查询余下可用的连接,不同连接服务
			for k, v := range pool.conns_map {
				if k == send_conns.net_addr {
					continue
				}

				if v.Len() > 0 {
					conns_lst = v
					break
				}
			}
		}

		// 发送失败
		if conns_lst.Len() <= 0 {
			//回调发送失败函数
			//go pool.conns_conf.Set_func.Af_send_err(send_conns.net_addr, send_conns.send_buf)

		} else { // 发送数据
			conn_v := conns_lst.Remove(conns_lst.Front()).(*connsValue)
			conn_v.conn.Send(send_conns.send_buf)
			conns_lst.PushBack(conn_v)

		}
	}

}

func doSendData2(pool *cConnPool, params interface{}) {
	send_conns2 := params.(*sendConns2)
	conn, ok := pool.conns_id_map[send_conns2.connect_id]
	if ok {
		// 有可用的连接,发送
		conn.Send(send_conns2.send_buf)
	}

}

func doAfterConns(pool *cConnPool, params interface{}) {
	afer_conns_data := params.(*afterConns)
	net_addr := afer_conns_data.net_addr
	conn := afer_conns_data.conn_client
	connect_id := afer_conns_data.connect_id
	log_obj := logger.Instance()
	var conn_value connsValue

	log_obj.LogSysInfo("New Connection!RemoteServer=%s", net_addr)
	conns_lst, ok := pool.conns_map[net_addr]
	if !ok { // 连接已经删除不存在了
		conn.Close()
		return
	}
	conn_value.conn = conn
	conn_value.connect_id = connect_id
	conns_lst.PushBack(&conn_value)

	pool.conns_id_map[connect_id] = conn
}

func doRmConns(pool *cConnPool, params interface{}) {
	rm_addr := params.(string)
	conns_lst, ok := pool.conns_map[rm_addr]
	log_obj := logger.Instance()

	log_obj.LogSysInfo("Remove Connection!RemoteIp=%s", rm_addr)
	if ok {
		for v := conns_lst.Front(); v != nil; v = v.Next() {
			conn_v := v.Value.(*connsValue)
			conn_v.conn.Close() // 断开连接
		}
	}
	delete(pool.conns_map, rm_addr)
}

func doCloseConns(pool *cConnPool, params interface{}) {
	conn := params.(*cConn)
	net_addr := conn.RemoteAddr()
	conns_lst, ok := pool.conns_map[net_addr]
	log_obj := logger.Instance()

	log_obj.LogSysInfo("Connection Close!RemoteIp=%s", net_addr)
	if ok {
		for v := conns_lst.Front(); v != nil; v = v.Next() {
			rm_conn := v.Value.(*connsValue).conn
			if rm_conn == conn {

				//	conn.clearSendChann()
				conns_lst.Remove(v)

				// 剔除connect_id索引
				delete(pool.conns_id_map, v.Value.(*connsValue).connect_id)
				break

			}
		}
	}
	conn.clearSendChann()
}

func doAlramConns(pool *cConnPool, params interface{}) {
	log_obj := logger.Instance()

	for k, v := range pool.conns_map {

		if pool.conns_conf.Heart_bit != nil {
			log_obj.LogSysDebug("New Heartbit Send!RemoteServer=%s", k)

			// 投递心跳包
			for element := v.Front(); element != nil; element = element.Next() {
				conn_v := element.Value.(*connsValue)
				conn_v.conn.Send(*pool.conns_conf.Heart_bit)

			}
		}
		if v.Len() < pool.conns_conf.Conns_nums {

			// 重新连接
			go func() {
				for i := v.Len(); i < pool.conns_conf.Conns_nums; i++ {
					connect_id := atomic.AddInt64(&pool.auto_incs, 1)
					conn := createConn(k,
						pool.conns_conf.Max_packet_size,
						pool.conns_conf.Set_func,
						pool,
						connect_id)
					if conn == nil {
						log_obj.LogSysInfo("Reconnect Failed!RemoteServer=%s", k)
						break
					}

					log_obj.LogSysInfo("Reconnect Ok!RemoteServer=%s", k)

					// 投递消息，告知连接创建成功
					hub_event := hubEvent{
						event_type: AF_CONNS,
						params: &afterConns{net_addr: k,
							connect_id:  connect_id,
							conn_client: conn},
					}
					pool.hub_chann <- hub_event
				}
			}()
		}
	}

	pool.alram()

	//pool.alram()
}

var func_map = map[int]func(pool *cConnPool, params interface{}){
	ADD_CONNS:   doAddConns,
	RM_CONNS:    doRmConns,
	SEND_CONNS:  doSendData,
	SEND_CONNS2: doSendData2,
	AF_CONNS:    doAfterConns,
	CLOSE_CONNS: doCloseConns,
	ALRAM_CONNS: doAlramConns,
}

func (this *cConnPool) connHub() {
	log_obj := logger.Instance()

	this.alram()
	for {
		select {
		case hub_event := <-this.hub_chann:
			event_func, ok := func_map[hub_event.event_type]
			if !ok {
				log_obj.LogSysWarn("HubEvent Not Find!")
				break
			}
			event_func(this, hub_event.params)
			break

		case <-this.close_chann: // 退出
			return

		}
	}
}

func (this *cConnPool) Stop() {

}

func (this *cConnPool) alram() {
	if this.conns_conf.Alram_time > 0 {
		log_obj := logger.Instance()
		log_obj.LogSysDebug("New Alram Occur!")

		go func() {
			time.Sleep(time.Duration(this.conns_conf.Alram_time) * time.Second)
			event := &hubEvent{
				event_type: ALRAM_CONNS,
				params:     nil,
			}
			this.sendEvent(event, BIG_LONG_TIME)
		}()
	}
}

func NewConnPool(protol_name string, conf *ConnsCfg) IConnPool {
	conns_pool := &cConnPool{
		protol_name:  protol_name,
		conns_map:    make(map[string]*list.List),
		conns_id_map: make(map[int64]*cConn),
		hub_chann:    make(chan hubEvent, 1024),
		//close_chann: make(chan int, 1),
		close_chann: make(chan int),
		conns_conf:  *conf,
		auto_incs:   0,
	}

	go conns_pool.connHub()
	return conns_pool
}
