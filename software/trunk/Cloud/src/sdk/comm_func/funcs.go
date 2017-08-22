package comm_func

import (
	"sync/atomic"
	"time"
)

var msg_id int64 = time.Now().Unix()

func CreateMsgId() int64 {
	return atomic.AddInt64(&msg_id, 1)
}
