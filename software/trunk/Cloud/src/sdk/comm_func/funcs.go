package comm_func

import (
	"sync/atomic"
)

var msg_id int64 = 0

func CreateMsgId() int64 {
	return atomic.AddInt64(&msg_id, 1)
}
