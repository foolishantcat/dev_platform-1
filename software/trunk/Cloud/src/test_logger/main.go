package main

import (
	"fmt"
	"sdk/logger"
	"time"
)

var wait_chann chan int

func Run(stop_chann chan int) {
	for {
		select {
		case <-stop_chann:
			wait_chann <- 1
			return
		default:
			logger.Instance().LogAppDebug("Hello Worldefefeafeaefef!")
			break
		}

	}
}

func main() {
	wait_chann = make(chan int, 1024)

	err := logger.Instance().Load("./conf.xml")
	if err != nil {
		fmt.Printf("ErrString=%s\n", err.Error())
		return
	}

	var stop_array []chan int = make([]chan int, 0, 1024)
	for i := 0; i < 100; i++ {
		stop_chann := make(chan int)
		stop_array = append(stop_array, stop_chann)
		go Run(stop_chann)
	}
	time.Sleep(100 * time.Second)

	for _, stop_chann := range stop_array {
		stop_chann <- 1
	}

	fmt.Printf("Finished!\n")

	nums := 0
	for {
		select {
		case <-wait_chann:
			nums++
			break
		}

		if nums == 100 {
			break
		}
	}

	logger.Instance().Close()
}
