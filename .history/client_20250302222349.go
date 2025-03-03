package srpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"github.com/dszqbsm/Srpc/codec"
)

// 承载满足rpc调用条件所需要的信息，rpc调用上下文
// 通过Done通道实现异步回调，避免阻塞调用方
type Call struct {
	Seq           uint64
	ServiceMethod string
	Args          interface{}
	Reply         interface{}
	Error         error
	Done          chan *Call // 用于支持异步调用
}

// 通过通道异步通知调用完成
func (call *Call) done() {
	call.Done <- call
}

// 客户端
// 双锁机制减少竞争，map实现O(1)复杂度请求管理
type Client struct {
	cc  codec.Codec // 消息的编解码器
	opt *Option
	// 双锁分离机制，减少锁竞争
	sending sync.Mutex   // 发送锁
	header  codec.Header // 每个请求的消息头
	mu      sync.Mutex   // 状态锁
	seq     uint64
	pending map[uint64]*Call // 存储未处理完的请求
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

// 完成调用后清理资源
func (client *Client) removeCall(seq uint64) *Call {
	client.mu.Lock()
	defer client.mu.Unlock()
	call := client.pending[seq]
	delete(client.pending, seq)
	return call
}

// 异常时终止所有进行中的调用
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

func (client *Client) receive() {
	var err error
	for err == nil {
		var h codec.Header
		if err = client.cc.ReadHeader(&h); err != nil {
			break
		}
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

func NewClient(conn io.ReadWriteCloser, opt *Option) (*Client, error) {
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
	return newClientCodec(f(conn), opt), nil
}

func newClientCodec(cc codec.Codec, opt *Option) *Client {
	client := &Client{
		seq:     1, // seq从1开始
		cc:      cc,
		opt:     opt,
		pending: make(map[uint64]*Call),
	}
	// 接收响应
	go client.receive()
	return client
}

// 用于解析Option协商方式，每个连接只能有唯一的协商机制
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
func Dial(network, address string, opts ...*Option) (client *Client, err error) {
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
		if client == nil {
			_ = conn.Close()
		}
	}()
	return NewClient(conn, opt)
}

func (client *Client) send(call *Call) {
	// 确保客户端能发送一个完整的请求
	client.sending.Lock()
	defer client.sending.Unlock()

	// 注册rpc调用
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

	// 发送请求
	if err = client.cc.Write(&client.header, call.Args); err != nil {
		call := client.removeCall(seq)
		// 调用可能被取消，因此需要使用call != nil进行判断
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

func (client *Client) GO(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
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

func (client *Client) Call(serviceMethod string, args, reply interface{}) error {
	call := <-client.GO(serviceMethod, args, reply, make(chan *Call, 1)).Done
	return call.Error
}
