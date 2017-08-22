package net_server

import (
	"time"
)

type TimeHandler interface {
	OnTimer(param interface{})
}

/*
type INetClient interface {
	Send(send_buf []byte) bool
	Close()
	GetPeerAddr() (ip_address string)
}
*/

type Timer struct {
	time_sec  int
	time_msec int
	time_nums int
	handle    TimeHandler
}

func SetTimer(time_sec int, time_msec int, timer_nums int, handle TimeHandler, contxt interface{}) *Timer {

	timer := &Timer{
		time_sec,
		time_msec,
		timer_nums,
		handle,
	}

	for i := 0; i < timer.time_nums; i++ {
		go timerFunc(timer, contxt)
	}
	return timer
}

func timerFunc(timer *Timer, param interface{}) {
	for {
		chan_a := make(chan int, 0)
		var time_msec int64

		time_msec = int64(timer.time_sec) * 1000
		time_msec += int64(timer.time_msec)

		go func() {
			//after_time := time.Now().Add(time.Duration(time_msec) * time.Millisecond)
			time.Sleep(time.Duration(time_msec) * time.Millisecond)
			chan_a <- 1
		}()

		select {
		case <-chan_a:
			timer.handle.OnTimer(param)
		}
	}
}

func KillTimer(time *Timer) {

}
