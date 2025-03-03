package srpc

import (
	"errors"
	"io"
	"sync"

	"github.com/dszqbsm/Srpc/codec"
)

// 承载满足rpc调用条件所需要的信息
type Call struct {
	Seq           uint64
	ServiceMethod string
	Args          interface{}
	Reply         interface{}
	Error         error
	Done          chan *Call // 用于支持异步调用
}

func (call *Call) done() {
	call.Done <- call
}

// 客户端
type Client struct {
	cc      codec.Codec // 消息的编解码器
	opt     *Option
	sending sync.Mutex   // 互斥锁，保证请求有序发送
	header  codec.Header // 每个请求的消息头
	mu      sync.Mutex
	seq     uint64
	pending map[uint64]*Call // 存储未处理完的请求
	// closing和shutdown任意一个值置为true，则表示client处于不可用的状态
	closing  bool // 用户主动关闭
	shutdown bool // 有错误发生
}

var _ io.Closer = (*Client)(nil)

var ErrShutdown = errors.New("connection is shut down")

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

func (client *Client) removeCall(seq uint64) *Call {
	client.mu.Lock()
	defer client.mu.Unlock()
	call := client.pending[seq]
	delete(client.pending, seq)
	return call
}

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
