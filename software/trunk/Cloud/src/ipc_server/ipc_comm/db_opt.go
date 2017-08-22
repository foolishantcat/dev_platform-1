package ipc_comm

import (
	"database/sql"
	"fmt"
	"sdk/logger"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type DbOpt struct {
	db_obj *sql.DB
}

func CreateDbObj(db_address string, login_name string, password string, db_name string) (*DbOpt, error) {
	//sql.Open("mysql", "user:password@tcp(localhost:5555)/dbname?charset=utf8")
	// 拼装信息
	data_source_name := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8",
		login_name,
		password,
		db_address,
		db_name)

	db_opt, err := sql.Open("mysql", data_source_name)
	if err != nil {
		return nil, err
	}

	return &DbOpt{db_opt}, nil
}

func (this DbOpt) RecordMsg(to_duid int64, msg_inf *MsgInfo) (bool, error) {

	// 按照过期时间进行分表,先求出时间
	date := time.Unix(int64(msg_inf.Last_time_stamp), 0)
	table_name := fmt.Sprintf("duid_msg%d%02d", date.Year(), date.Month())

	sql_commit := fmt.Sprintf("insert into %s(msg_id,from_duid,to_duid,data,start_time,end_time,status)value(%v,%v,%v,'%s',%v,%v,%v)",
		table_name,
		msg_inf.Msg_id,
		msg_inf.From_duid,
		to_duid,
		msg_inf.Msg_data,
		time.Now().Unix(),
		msg_inf.Last_time_stamp,
		msg_inf.Status)

	logger.Instance().LogAppDebug("Insert Sql-Commit=%s", sql_commit)

	_, err := this.db_obj.Exec(sql_commit)

	if err != nil {
		return false, err
	}

	return true, nil
}
