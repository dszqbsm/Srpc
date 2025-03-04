package main

import (
	"log"
	"net"
	"sync"
	"time"

	srpc "github.com/dszqbsm/Srpc"
)

type Foo int

type Args struct {
	Num1, Num2 int
}

func (f Foo) Sum(args Args, reply *int) error {
	*reply = args.Num1 + args.Num2
	return nil
}

func startServer(addr chan string) {
	// 注册Foo到Server中，并启动rpc服务
	var foo Foo
	if err := srpc.Register(&foo); err != nil {
		log.Fatal("register error:", err)
	}
	// 监听端口
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal("network error:", err)
	}
	log.Println("start rpc server on", l.Addr())
	addr <- l.Addr().String()
	srpc.Accept(l)
}

func main() {
	// 设置日志输出格式，清楚所有格式标记，仅输出纯消息内容
	log.SetFlags(0)
	addr := make(chan string)
	go startServer(addr)
	// 使用了通道addr，能够确保服务端端口监听成功，客户端再发起请求
	// 即服务端端口监听成功后，会将监听的地址发送到通道中，addr是无缓冲通道，客户端会阻塞直到服务端端口监听成功
	// 建立连接，与服务端进行协议协商，创建客户端实例
	client, _ := srpc.Dial("tcp", <-addr)
	defer func() { _ = client.Close() }()

	time.Sleep(time.Second)

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// args := fmt.Sprintf("srpc req %d", i)
			args := &Args{Num1: i, Num2: i * i}
			var reply int
			// 客户端向连接中注入数据
			// Call方法会阻塞等待服务端响应才返回
			if err := client.Call("Foo.Sum", args, &reply); err != nil {
				log.Fatal("call Foo.Sum error:", err)
			}
			log.Println("%d + %d = %d", args.Num1, args.Num2, reply)
		}(i)
	}
	wg.Wait()
}
