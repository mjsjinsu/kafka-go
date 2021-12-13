package main

import (
	"runtime"
	"time"

	"github.com/mjsjinsu/kafka-go-sample/producer/utils"
)

var (
	brokers = "172.31.152.16:9092,172.31.152.16:9093,172.31.152.16:9094"
	topics  = "test-p2"
)

func main() {
	runtime.GOMAXPROCS(4)
	for i := 0; i < 10; i++ {
		go utils.Publish(string("abc"))
		go utils.Publish(string("111111111111111111111111111111"))
	}

	time.Sleep(time.Second * 10)

}
