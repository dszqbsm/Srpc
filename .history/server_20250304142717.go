package srpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/dszqbsm/Srpc/codec"
)

const MagicNumber = 0x3bef5c // 固定编码用于传输协商内容

// 协商的编码方式通过Option中的CodeType承载
// 为了实现简单，固定采用json编码Option，而后续的header和body则用CodecType指定的编码方式进行编码
// 在一次连接中，Option固定在报文的最开始，header和body可以有多个
type Option struct {
	MagicNumber    int
	CodecType      codec.Type
	ConnectTimeout time.Duration // 默认10s
	HandleTimeout  time.Duration // 默认0s，即不设限
}

var DefaultOption = &Option{
	MagicNumber:    MagicNumber,
	CodecType:      codec.GobType,
	ConnectTimeout: time.Second * 10,
}

type Server struct {
	// sync.Map替代普通map+互斥锁，能够提升并发读写性能，适合读多写少的场景
	// 无需显式加锁，通过sync.Map内部原子操作和分片设计，实现读操作的并发安全
	// 写操作优化，LoadOrStore方法通过原子操作实现存在即返回，不存在即写入
	// 自动管理内存分配，避免频繁扩容带来的性能抖动
	serviceMap sync.Map // 并发安全服务注册表
}

func NewServer() *Server {
	return &Server{}
}

var DefaulServer = NewServer()

// 注册服务实例
func (server *Server) Register(rcvr interface{}) error {
	s := newService(rcvr)
	// 原子操作检查服务名是否已存在，存在即返回，不存在即写入
	if _, dup := server.serviceMap.LoadOrStore(s.name, s); dup {
		return fmt.Errorf("rpc: service already defined: %s", s.name)
	}
	return nil
}

func Register(rcvr interface{}) error {
	return DefaulServer.Register(rcvr)
}

// 解析Service.Method格式的方法路径
func (server *Server) findService(serviceMethod string) (svc *service, mtype *methodType, err error) {
	// 解析Service.Method格式的方法路径，获取服务名和方法名
	dot := strings.LastIndex(serviceMethod, ".")
	if dot < 0 {
		err = errors.New("rpc server: service/method request ill-formed: " + serviceMethod)
		return
	}
	serviceName, methodName := serviceMethod[:dot], serviceMethod[dot+1:]
	// 一级查找服务
	svci, ok := server.serviceMap.Load(serviceName)
	if !ok {
		err = errors.New("rpc server: can't find service " + serviceName)
		return
	}
	svc = svci.(*service)
	// 二级查找方法
	mtype = svc.method[methodName]
	if mtype == nil {
		err = errors.New("rpc server: can't find method " + methodName)
	}
	return
}
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
	server.serveCodec(f(conn), &opt)
}

var invalidRequest = struct{}{}

// 处理header和body内容，分为三步：读取请求、处理请求、回复请求
func (server *Server) serveCodec(cc codec.Codec, opt *Option) {
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
		go server.handleRequest(cc, req, sending, wg, opt.HandleTimeout)
	}
	// 阻塞等待所有handler协程完成
	wg.Wait()
	_ = cc.Close()
}

type request struct {
	h            *codec.Header
	argv, replyv reflect.Value // argv是方法输入参数；replyv是方法输出参数
	mtype        *methodType
	svc          *service
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

	/* 	// TODO: 现在我们不知道请求参数类型，只支持string类型
	   	// 使用反射机制获取字符串类型的反射类型对象，通过Interface()获取真实的指针并按string类型解码到该指针的内容中
	   	req.argv = reflect.New(reflect.TypeOf(""))
	   	if err = cc.ReadBody(req.argv.Interface()); err != nil {
	   		log.Println("rpc server: read argv err:", err)
	   	}
	   	return req, nil */
	req.svc, req.mtype, err = server.findService(req.h.ServiceMethod)
	if err != nil {
		return req, err
	}
	// 根据请求的参数类型，创建对应的入参实例
	req.argv = req.mtype.newArgv()
	req.replyv = req.mtype.newReplyv()

	// 将入参类型统一转化为指针类型
	argvi := req.argv.Interface()
	if req.argv.Type().Kind() != reflect.Ptr {
		// 若入参类型不为指针，则取入参地址转化为指针类型
		argvi = req.argv.Addr().Interface()
	}
	if err = cc.ReadBody(argvi); err != nil {
		log.Println("rpc server: read argv err:", err)
		return req, err
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
/* func (server *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup) {
	// 应该调用注册rpc方法来获得正确的回复
	// 暂时只打印参数并回复一个hello消息
	defer wg.Done()
	/* 	log.Println(req.h, req.argv.Elem())
	   	req.replyv = reflect.ValueOf(fmt.Sprintf("srpc resp %d", req.h.Seq))
	   	server.sendResponse(cc, req.h, req.replyv.Interface(), sending)
	err := req.svc.call(req.mtype, req.argv, req.replyv)
	if err != nil {
		req.h.Error = err.Error()
		server.sendResponse(cc, req.h, invalidRequest, sending)
		return
	}
	server.sendResponse(cc, req.h, req.replyv.Interface(), sending)
}
*/

// 处理请求
// 服务端处理报文，即Server.handleRequest超时：
func (server *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	defer wg.Done()
	called := make(chan struct{}) // 用于通知服务方法调用已完成，不论成功失败
	sent := make(chan struct{})   // 用于通知响应已通过编解码器发送完成，即响应发送完成，并释放了互斥锁
	go func() {
		// 执行实际的服务方法
		err := req.svc.call(req.mtype, req.argv, req.replyv)
		called <- struct{}{}
		if err != nil {
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending)
			sent <- struct{}{}
			return
		}
		server.sendResponse(cc, req.h, req.replyv.Interface(), sending)
		sent <- struct{}{} // 但是这里可能已经超时了，而原goroutine还阻塞在这里，因为没有接收者
	}()

	// 超时处理逻辑
	if timeout == 0 {
		<-called
		<-sent
		return
	}
	select {
	case <-time.After(timeout):
		req.h.Error = fmt.Sprintf("rpc server: request handle timeout: expect within %s", timeout)
		server.sendResponse(cc, req.h, invalidRequest, sending)
	case <-called:
		<-sent
	}
}

const (
	connected        = "200 Connected to srpc"
	defaultRPCPath   = "/_srpc_"
	defaultDebugPath = "/debug/srpc"
)

func (server *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "CONNECT" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		io.WriteString(w, "405 must CONNECT\n")
		return
	}
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Print("rpc hijacking ", req.RemoteAddr, ": ", err.Error())
		return
	}
	_, _ = io.WriteString(conn, "HTTP/1.0 "+connected+"\n\n")
	server.ServeConn(conn)
}

func (server *Server) HandleHTTP() {
	http.Handle(defaultRPCPath, server)
}

func HandleHTTP() {
	DefaulServer.HandleHTTP()
}
