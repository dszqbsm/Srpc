package srpc

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"reflect"
	"sync"

	"github.com/dszqbsm/Srpc/codec"
)

const MagicNumber = 0x3bef5c // 固定编码用于传输协商内容

// 协商的编码方式通过Option中的CodeType承载
// 为了实现简单，固定采用json编码Option，而后续的header和body则用CodecType指定的编码方式进行编码
// 在一次连接中，Option固定在报文的最开始，header和body可以有多个
type Option struct {
	MagicNumber int
	CodecType   codec.Type
}

var DefaultOption = &Option{
	MagicNumber: MagicNumber,
	CodecType:   codec.GobType,
}

type Server struct{}

func NewServer() *Server {
	return &Server{}
}

var DefaulServer = NewServer()

func (server *Server) Accept(lis net.Listener) {
	// for循环等待socket连接建立
	for {
		// 阻塞等待连接到达并完成TCP三次握手，返回连接对象
		conn, err := lis.Accept()
		if err != nil {
			log.Println("rpc server: accept error:", err)
			return
		}
		// 连接建立后，开启一个goroutine处理连接
		go server.ServeConn(conn)
	}
}

// 作为rpc框架，对外暴露Accept方法，用来接收一个tcp协议或unix协议的连接，启动服务，等待连接建立并处理连接
// 即若想启动服务，只需要传入listener即可
func Accept(lis net.Listener) {
	DefaulServer.Accept(lis)
}

// 连接处理
func (server *Server) ServeConn(conn io.ReadWriteCloser) {
	// 处理完后关闭连接
	defer func() { _ = conn.Close() }()
	// 协议握手验证，即验证协商内容是否符合预期
	// 从连接中解码Option结构体并赋值给opt变量
	// 底层会通过net.Conn的Read()方法从socket缓冲区阻塞读取字节，直到收到客户端发送的完整json数据/连接被关闭/超时
	var opt Option
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println("rpc server: options error: ", err)
		return
	}
	if opt.MagicNumber != MagicNumber {
		log.Printf("rpc server: invalid magic number %x", opt.MagicNumber)
		return
	}
	// 根据协商内容生成对于header和body内容的编解码器的构造函数
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		log.Printf("rpc server: invalid codec type %s", opt.CodecType)
		return
	}
	// 处理完协商内容后，处理header和body内容，传入对应的编解码器
	server.serveCodec(f(conn))
}

var invalidRequest = struct{}{}

// 处理header和body内容，分为三步：读取请求、处理请求、回复请求
func (server *Server) serveCodec(cc codec.Codec) {
	// 保证响应顺序
	sending := new(sync.Mutex)
	// 等待所有请求处理完毕
	wg := new(sync.WaitGroup)
	// 因为一次连接中允许接收多个请求，即长连接，因此用for无限等待请求的到来
	// handleRequest使用了协程并发处理请求
	// 回复请求的报文必须是逐个发送的，而handleRequest是并发处理请求，并发容易导致回复报文交织在一起，这里使用锁保证逐个回复请求报文
	// 尽力而为，只有在header解析失败时，才终止循环
	// 在长连接中是如何区分不同请求的？————通过请求序号区分不同请求
	// 循环阻塞等待读取请求，直到发生连接级错误才终止
	for {
		req, err := server.readRequest(cc)
		if err != nil {
			// 读取失败分为可恢复错误和不可恢复错误
			if req == nil {
				break // 连接级错误，必须终止
			}
			// 请求级错误，发送错误响应
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending)
			continue
		}
		wg.Add(1)
		// 每个请求独立协程处理
		go server.handleRequest(cc, req, sending, wg)
	}
	// 阻塞等待所有handler协程完成
	wg.Wait()
	_ = cc.Close()
}

type request struct {
	h            *codec.Header
	argv, replyv reflect.Value
}

// 读取请求头
func (server *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var h codec.Header
	// 从连接中按Header格式读取请求头，并按协议编解码得到填充后的Header结构体
	if err := cc.ReadHeader(&h); err != nil {
		// io.EOF表示正常连接关闭，io.ErrUnexpectedEOF表示数据未读完时连接断开
		// 这两种属于常见网络行为，无序记录为错误
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Println("rpc server: read header error:", err)
		}
		return nil, err
	}
	return &h, nil
}

// 读取请求头和请求体
func (server *Server) readRequest(cc codec.Codec) (*request, error) {
	h, err := server.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}
	req := &request{h: h}

	// TODO: 现在我们不知道请求参数类型，只支持string类型
	// 使用反射机制获取字符串类型的反射类型对象，通过Interface()获取真实的指针并按string类型解码到该指针的内容中
	req.argv = reflect.New(reflect.TypeOf(""))
	if err = cc.ReadBody(req.argv.Interface()); err != nil {
		log.Println("rpc server: read argv err:", err)
	}
	return req, nil
}

// 发送响应
func (server *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	// 互斥锁保证响应发送顺序：确保同一时刻只有一个goroutine能执行写操作
	// 即使多个响应已经处理完成，也会按照获取锁的顺序执行写入
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(h, body); err != nil {
		log.Println("rpc server: write response error:", err)
	}
}

// 处理请求
func (server *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup) {
	// 应该调用注册rpc方法来获得正确的回复
	// 暂时只打印参数并回复一个hello消息
	defer wg.Done()
	log.Println(req.h, req.argv.Elem())
	req.replyv = reflect.ValueOf(fmt.Sprintf("srpc resp %d", req.h.Seq))
	server.sendResponse(cc, req.h, req.replyv.Interface(), sending)
}
