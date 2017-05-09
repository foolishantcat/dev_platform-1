package main

import (
	"database/sql"
	"fmt"
	"game_core"
	"logger"
	"net_server"
	"pb_go"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	_ "github.com/go-sql-driver/mysql"
	"github.com/golang/protobuf/proto"
)

type eventFunc func(client net_server.INetClient, buf []byte, contxt interface{})

type dbConfig struct {
	Dns_name  string
	Port      string
	Db_name   string
	User_name string
	Pass_word string
}

func checkSum(buf []byte) int32 {
	var check_sum int32

	check_sum = 0
	for _, v := range buf {
		check_sum += int32(v)
	}

	return check_sum
}

type Account struct {
	user_name string
	pass_word string
}

type UserData struct {
	token_nums      string
	token_keep_time int64
	create_time     int64
	access_right    int
}

type ResContxt struct {
	mux      sync.Mutex
	user_map map[Account]UserData
}

type Login struct {
	db              *sql.DB
	turn_id         int32
	res_contxt_pool []ResContxt
	event_handle    map[string]eventFunc
}

func (this *Login) OnNetAccpet(peer_ip string) (interface{}, bool, func(buf []byte) ([]byte, int)) {
	return nil, true, game_core.GameParse
}

func (this *Login) OnNetRecv(client net_server.INetClient, rev_buf []byte, contxt interface{}) {
	log_obj := logger.Instance()

	// 解析头部的包
	packet_header := (*PacketHeader)(unsafe.Pointer(&rev_buf[0]))
	size := int(packet_header.size) + int(unsafe.Sizeof(PacketHeader{}))

	check_sum := checkSum(rev_buf[4:size])
	if check_sum != packet_header.check_sum {
		log_obj.LogAppWarn("CheckSum Failed!calc_checksum=%d,checksum=%d,PeerIP=%s",
			check_sum,
			packet_header.check_sum,
			client.GetPeerAddr())
		client.Close()
		return
	}

	_func, ok := this.event_handle[string(packet_header.protol_name[:])]
	if !ok {
		log_obj.LogAccWarn("Packet Can't be Parsed")
		client.Close()
		return
	}

	_func(client, rev_buf, this)
}

func (this *Login) OnNetErr(client net_server.INetClient, contxt interface{}) {

}

func (this *Login) InitParms(resoure_nums int, db_conf dbConfig) bool {

	this.res_contxt_pool = make([]ResContxt, resoure_nums)
	log_obj := logger.Instance()

	db_connect := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8",
		db_conf.User_name,
		db_conf.Pass_word,
		db_conf.Dns_name,
		db_conf.Port,
		db_conf.Db_name)

	// 创建连接
	var err error
	this.db, err = sql.Open("mysql", db_connect)
	if err != nil {
		log_obj.LogAppError("Init Db Failed!ErrString=%s", err.Error())
		return false
	}

	for i, _ := range this.res_contxt_pool {
		this.res_contxt_pool[i].user_map = make(map[Account]UserData)

	}

	this.event_handle["Login"] = login_Event

	return true
}

func login_Event(client net_server.INetClient, buf []byte, contxt interface{}) {
	var request game.LoginRequest
	var response game.LoginResponse
	var account Account

	logic := contxt.(*Login)
	proto.Unmarshal(buf[unsafe.Sizeof(PacketHeader{}):], &request)

	func() {
		// 选取合适资源
		index := atomic.AddInt32(&logic.turn_id, 1)
		index %= int32(len(logic.res_contxt_pool))
		user_map := logic.res_contxt_pool[index].user_map

		defer logic.res_contxt_pool[index].mux.Unlock()

		logic.res_contxt_pool[index].mux.Lock()

		account.pass_word = request.GetPassWord()
		account.user_name = request.GetUserName()
		user_data, ok := user_map[account]
		if !ok { // 找不到

			// 查找db
		}

		if user_data.access_right != 0 {
			response.ErrCode = game.ErrCode_USER_LOCKED.Enum()
			response.ErrString = proto.String("User Has be Locked!")
			return
		}

		if time.Now().Unix()-user_data.create_time >= user_data.token_keep_time { // token已经无效
			delete(user_map, account)
		}
		new_user_data := UserData{
			create_time:     time.Now().Unix(),
			access_right:    user_data.access_right,
			token_keep_time: 10,
			token_nums:      game_core.CreateTokenNums(),
		}
		user_map[account] = new_user_data

		response.ErrCode = game.ErrCode_AUTHEN_OK.Enum()
		response.ErrString = proto.String("Login Ok!")
		response.PlayerId = proto.Int64(123456)
		response.GameServer = proto.String("192.168.1.123")
		response.Port = proto.String("123456")
	}()

	buf, _ := proto.Marshal(&response)

}
