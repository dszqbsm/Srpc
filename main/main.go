package main

import (
	"context"
	"log"
	"net"
	"sync"
	"time"

	srpc "github.com/dszqbsm/Srpc"
	"github.com/dszqbsm/Srpc/xclient"
)

type Foo int

type Args struct {
	Num1, Num2 int
}

func (f Foo) Sum(args Args, reply *int) error {
	*reply = args.Num1 + args.Num2
	return nil
}

func (f Foo) Sleep(args Args, reply *int) error {
	time.Sleep(time.Second * time.Duration(args.Num1))
	*reply = args.Num1 + args.Num2
	return nil
}

// 注册Foo到Server中，并启动rpc服务
/* func startServer(addr chan string) {

	var foo Foo
	// 启动端口监听
	l, _ := net.Listen("tcp", ":9999")
	// 注册服务
	_ = srpc.Register(&foo)
	// 绑定路由与对应的处理函数
	srpc.HandleHTTP()
	addr <- l.Addr().String()
	// 启动rpc服务，等待客户端连接
	// srpc.Accept(l)
	// 启动http服务器，监听指定端口并按路由调用对应的处理函数进行处理
	_ = http.Serve(l, nil)
}*/

func startServer(addrCh chan string) {
	var foo Foo
	l, _ := net.Listen("tcp", ":0")
	server := srpc.NewServer()
	_ = server.Register(&foo)
	addrCh <- l.Addr().String()
	server.Accept(l)
}

func foo(xc *xclient.XClient, ctx context.Context, typ, serviceMethod string, args *Args) {
	var reply int
	var err error
	switch typ {
	case "call":
		// 同步调用
		err = xc.Call(ctx, serviceMethod, args, &reply)
	case "broadcast":
		// 广播调用
		err = xc.Broadcast(ctx, serviceMethod, args, &reply)
	}
	if err != nil {
		log.Printf("%s %s error: %v", typ, serviceMethod, err)
	} else {
		log.Printf("%s %s success: %d + %d = %d", typ, serviceMethod, args.Num1, args.Num2, reply)
	}
}

/*
func call(addr chan string) {
	// 使用了通道addr，能够确保服务端端口监听成功，客户端再发起请求
	// 即服务端端口监听成功后，会将监听的地址发送到通道中，addr是无缓冲通道，客户端会阻塞直到服务端端口监听成功
	// 建立连接，与服务端进行协议协商，创建客户端实例
	client, _ := srpc.DialHTTP("tcp", <-addr)
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
			if err := client.Call(context.Background(), "Foo.Sum", args, &reply); err != nil {
				log.Fatal("call Foo.Sum error:", err)
			}
			log.Printf("%d + %d = %d", args.Num1, args.Num2, reply)
		}(i)
	}
	wg.Wait()
}
*/

func call(addr1, addr2 string) {
	d := xclient.NewMultiServerDiscovery([]string{"tcp@" + addr1, "tcp@" + addr2})
	xc := xclient.NewXClient(d, xclient.RandomSelect, nil)
	defer func() { _ = xc.Close() }()
	// send request & receive response
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			foo(xc, context.Background(), "call", "Foo.Sum", &Args{Num1: i, Num2: i * i})
		}(i)
	}
	wg.Wait()
}

func broadcast(addr1, addr2 string) {
	d := xclient.NewMultiServerDiscovery([]string{"tcp@" + addr1, "tcp@" + addr2})
	xc := xclient.NewXClient(d, xclient.RandomSelect, nil)
	defer func() { _ = xc.Close() }()
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			foo(xc, context.Background(), "broadcast", "Foo.Sum", &Args{Num1: i, Num2: i * i})
			// expect 2 - 5 timeout
			ctx, _ := context.WithTimeout(context.Background(), time.Second*2)
			foo(xc, ctx, "broadcast", "Foo.Sleep", &Args{Num1: i, Num2: i * i})
		}(i)
	}
	wg.Wait()
}

func main() {
	// 设置日志输出格式，清楚所有格式标记，仅输出纯消息内容
	log.SetFlags(0)
	ch1 := make(chan string)
	ch2 := make(chan string)
	// 启动两个服务
	go startServer(ch1)
	go startServer(ch2)

	addr1 := <-ch1
	addr2 := <-ch2

	time.Sleep(time.Second)
	call(addr1, addr2)
	broadcast(addr1, addr2)
}
