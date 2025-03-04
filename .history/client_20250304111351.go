package srpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/dszqbsm/Srpc/codec"
)

// 承载满足rpc调用条件所需要的信息，rpc调用上下文
// 通过Done通道实现异步回调，避免阻塞调用方
// 存储请求调用信息
type Call struct {
	Seq           uint64
	ServiceMethod string
	Args          interface{}
	Reply         interface{}
	Error         error
	Done          chan *Call // 用于支持异步调用
}

// 用于封装创建客户端的结果，使得通道一次性可以传递多个值
type clientResult struct {
	client *Client
	err    error
}

// 通过抽象函数类型，使得dialTimeout函数能够接收不同的客户端创建函数作为参数
type newClientFunc func(conn net.Conn, opt *Option) (*Client, error)

// 建立rpc客户端连接，并处理超时逻辑，客户端向服务端打电话建立连接
// 客户端创建连接时导致的超时：使用默认超时时间建立带有超时的连接，通过通道等待创建连接函数的返回结果，超时则失败，也支持用户自行定义Option配置超时时间，否则使用默认配置
func dialTimeout(f newClientFunc, network, address string, opts ...*Option) (client *Client, err error) {
	// 解析协商内容
	opt, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}
	// 建立带超时的连接
	conn, err := net.DialTimeout(network, address, opt.ConnectTimeout)
	if err != nil {
		return nil, err
	}
	defer func() {
		if client == nil {
			// 只有NewClient失败时才需要关闭连接，错误处理
			_ = conn.Close()
		}
	}()
	// 结果通道
	ch := make(chan clientResult)
	// 启动协程执行客户端创建函数，并将结果发送到通道ch
	go func() {
		client, err := f(conn, opt)
		ch <- clientResult{client: client, err: err}
	}()
	// 超时处理逻辑
	if opt.ConnectTimeout == 0 { // 不启用超时控制，直接等待通道结果
		result := <-ch
		return result.client, result.err
	}
	// 使用select同时等待两个事件：超时或接收到结果
	select {
	case <-time.After(opt.ConnectTimeout):
		return nil, fmt.Errorf("rpc client: connect timeout: expect within %s", opt.ConnectTimeout)
	case result := <-ch:
		return result.client, result.err
	}
}

// 通过通道异步通知调用方结果就绪
func (call *Call) done() {
	call.Done <- call
}

// 客户端，多一些维护管理字段
// 双锁机制减少竞争，map实现O(1)复杂度请求管理
type Client struct {
	cc  codec.Codec // 消息的编解码器
	opt *Option
	// 双锁分离机制，减少锁竞争
	sending sync.Mutex   // 互斥锁确保请求发送的原子性
	header  codec.Header // 每个请求的消息头
	mu      sync.Mutex   // 互斥锁保护seq和pending的并发访问
	seq     uint64
	pending map[uint64]*Call // 存储未处理完的请求，映射管理请求生命周期
	// closing和shutdown任意一个值置为true，则表示client处于不可用的状态
	closing  bool // 用户主动关闭
	shutdown bool // 有错误发生
}

// 实现编译期断言，Client实现了io.Closer接口才能通过编译
var _ io.Closer = (*Client)(nil)

var ErrShutdown = errors.New("connection is shut down")

// 安全关闭连接
func (client *Client) Close() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing {
		return ErrShutdown
	}
	client.closing = true
	return client.cc.Close()
}

func (client *Client) IsAvailable() bool {
	client.mu.Lock()
	defer client.mu.Unlock()
	return !client.shutdown && !client.closing
}

// 注册新的rpc调用
func (client *Client) registerCall(call *Call) (uint64, error) {
	// 协程注册rpc调用信息前必须先获得锁，保证并发安全
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing || client.shutdown {
		return 0, ErrShutdown
	}
	call.Seq = client.seq
	client.pending[call.Seq] = call
	client.seq++
	return call.Seq, nil
}

// 请求写入后清理资源，从pending映射中移除指定seq的Call对象，并返回该对象
// 确保在并发环境下，不会操作已被其他协程处理的Call对象
func (client *Client) removeCall(seq uint64) *Call {
	client.mu.Lock()
	defer client.mu.Unlock()
	call := client.pending[seq]
	delete(client.pending, seq)
	return call
}

// 异常时终止所有进行中的调用，清空pending映射
func (client *Client) terminateCalls(err error) {
	client.sending.Lock()
	defer client.sending.Unlock()
	client.mu.Lock()
	defer client.mu.Unlock()
	client.shutdown = true
	for _, call := range client.pending {
		call.Error = err
		call.done()
	}
}

// 响应的接收是从客户端与服务端协商好之后开始的，并且对当前连接进行持续的响应接收，根据响应中的请求seq区分不同请求的响应
func (client *Client) receive() {
	var err error
	for err == nil {
		var h codec.Header
		if err = client.cc.ReadHeader(&h); err != nil {
			break
		}
		// 服务端返回响应时，在removeCall中，会根据seq找到对应的Call，并将其移除pending，避免重复处理
		call := client.removeCall(h.Seq)
		switch {
		case call == nil:
			// 调用不存在，可能是请求没有发送完整，或者因为其他原因被取消
			// 但是服务端仍旧处理了，因此我们直接忽略
			err = client.cc.ReadBody(nil)
		case h.Error != "":
			// 调用发生错误，即服务端处理出错
			call.Error = fmt.Errorf(h.Error)
			err = client.cc.ReadBody(nil)
		default:
			err = client.cc.ReadBody(call.Reply)
			if err != nil {
				call.Error = errors.New("reading body" + err.Error())
			}
			call.done()
		}
	}
	// 错误处理
	client.terminateCalls(err)
}

// 发送协商内容，并创建客户端
func NewClient(conn net.Conn, opt *Option) (*Client, error) {
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		err := fmt.Errorf("invalid codec type %s", opt.CodecType)
		log.Println("rpc client: codec error:", err)
		return nil, err
	}
	// 发送协商内容
	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		log.Println("rpc client: options error: ", err)
		_ = conn.Close()
		return nil, err
	}
	// 创建客户端实例
	return newClientCodec(f(conn), opt), nil
}

// 创建客户端实例
func newClientCodec(cc codec.Codec, opt *Option) *Client {
	client := &Client{
		seq:     1, // seq从1开始，是对于每一个客户端连接来说
		cc:      cc,
		opt:     opt,
		pending: make(map[uint64]*Call),
	}
	// 接收响应
	go client.receive()
	return client
}

// 用于解析Option协商方式，每个连接的协商字段长度都是固定的
func parseOptions(opts ...*Option) (*Option, error) {
	// 如果没有传入Option，则使用默认的协商机制
	if len(opts) == 0 || opts[0] == nil {
		return DefaultOption, nil
	}
	// 只允许传入一个Option
	if len(opts) != 1 {
		return nil, errors.New("number of options is more than 1")
	}
	opt := opts[0]
	// srpc框架只接收预定义的协商方式，协商字段长度固定
	opt.MagicNumber = DefaultOption.MagicNumber
	if opt.CodecType == "" {
		opt.CodecType = DefaultOption.CodecType
	}
	return opt, nil
}

// 建立连接
/* func Dial(network, address string, opts ...*Option) (client *Client, err error) {
	// 解析协商内容
	opt, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}
	// 解析目标地址、创建tcp套接字、与服务端完成握手、返回可用于数据传输的连接对象
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	// 关闭连接
	defer func() {
		// 只有NewClient失败时才需要关闭连接，错误处理
		if client == nil {
			_ = conn.Close()
		}
	}()
	// 向服务端发送协商内容并创建客户端实例
	return NewClient(conn, opt)
} */

// 建立连接
func Dial(network, address string, opts ...*Option) (*Client, error) {
	return dialTimeout(NewClient, network, address, opts...)
}

// 发送请求
func (client *Client) send(call *Call) {
	// 确保同一时间只有一个协程能执行请求发送操作
	// 即协程要执行发送操作，必须先获得锁，只是通过这一点来保证同一时间只有一个协程能执行写操作，而不是对字段什么的加锁不能访问之类的，也只是逻辑上的加锁
	client.sending.Lock()
	defer client.sending.Unlock()

	// 注册rpc调用信息
	seq, err := client.registerCall(call)
	if err != nil {
		call.Error = err
		call.done()
		return
	}

	// 准备请求头
	client.header.ServiceMethod = call.ServiceMethod
	client.header.Seq = seq
	client.header.Error = ""

	// 写入请求
	// 虽然编码器不是线程安全的，并发调用会导致数据竞争，通过sending锁解决这个问题
	if err = client.cc.Write(&client.header, call.Args); err != nil {
		call := client.removeCall(seq)
		// 当请求发送失败时，清理已注册的Call，并及时通知调用方，避免调用方永久阻塞
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

// GO和Call方法是客户端暴露给用户的两个rpc服务调用接口，GO是一个异步接口，返回Call实例；Call是对GO的封装，阻塞等待call.Done，等待响应返回，是一个同步接口

// 异步调用，没有阻塞，直接返回call
func (client *Client) GO(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	// GO方法根据调用方需求，支持指定容量缓存通道，默认容量为10
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 { // 传入无缓冲通道会panic
		log.Panic("rpc client: done channel is unbuffered")
	}
	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}
	client.send(call)
	return call
}

// serviceMethod参数表示要调用的远程服务方法
// 同步调用，阻塞等待从Done通道中获取数据
/* func (client *Client) Call(serviceMethod string, args, reply interface{}) error {
	// 创建带缓冲的完成通道，容量为1避免阻塞异步回调
	// 阻塞等待服务端响应，保证完成一次请求，是同步rpc调用的语义，即让调用方可以用同步的方式编写代码，而时机底层是异步网络通信
	// 通过Done通道，只有服务端响应后，才能从done通道中读取数据，这里体现的是客户端调用方对每一次发送请求的同步调用
	call := <-client.GO(serviceMethod, args, reply, make(chan *Call, 1)).Done
	return call.Error
} */

// rpc方法调用，允许调用方传入带有超时或取消信号的上下文
// 客户端调用rpc方法整个过程导致的超时：通过context支持调用方设定整个过程超时时间
func (client *Client) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	call := client.GO(serviceMethod, args, reply, make(chan *Call, 1))
	select {
	// 超时触发
	case <-ctx.Done(): // 返回一个通道，当Context被取消或超时时，通道会被关闭
		client.removeCall(call.Seq)
		return errors.New("rpc client: call failed: " + ctx.Err().Error())
		// 调用完成
	case call := <-call.Done:
		return call.Error
	}
}
