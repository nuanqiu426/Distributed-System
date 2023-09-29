package main

import (
	"fmt"
	"strconv"
	"time"
)

func main() {
	fmt.Println("hello" + strconv.Itoa(1)) // int转string
	// 心跳检查的原理：计时是从创建时钟channel开始的
	timecal := time.Now().Add(time.Second * 5)
	time.Sleep(time.Second * 5)
	// After函数接受一个时长，等待时间到后，将等待完成时所处时间点写入到 channel 中并返回这个只读 channel。
	if time.Now().After(timecal) {
		fmt.Println("Yes")
	}

}
