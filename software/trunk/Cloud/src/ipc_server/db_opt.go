package main

import (
	"database/sql"
	"fmt"
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

func (this DbOpt) RecordMsg(msg_id int64, to_duid string, msg_inf *MsgInfo) (bool, error) {

	// 按照过期时间进行分表,先求出时间
	date := time.Unix(int64(msg_inf.Last_time_stamp), 0)
	table_name := fmt.Sprintf("duid_msg%d%2d", date.Year(), date.Month())

	sql_commit := fmt.Sprintf("insert into ?(msg_id,from_duid,to_duid,data,start_time,end_time,status)value(%?,%?,%?,%?,from_unixtime(%?),from_unixtime(%?),0)",
		table_name,
		msg_id,
		msg_inf.From_duid,
		to_duid,
		msg_inf.Msg_data,
		time.Now().Unix(),
		msg_inf.Last_time_stamp)

	_, err := this.db_obj.Exec(sql_commit,
		table_name,
		msg_id,
		msg_inf.From_duid,
		to_duid,
		msg_inf.Msg_data,
		time.Now().Unix(),
		msg_inf.Last_time_stamp)

	if err != nil {
		return false, err
	}

	return true, nil
}
