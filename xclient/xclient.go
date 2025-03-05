package xclient

import (
	"context"
	"io"
	"reflect"
	"sync"

	. "github.com/dszqbsm/Srpc" // 点导入，进而在调用Srpc中的方法时，不需要Srpc.前缀
)

// 继承服务发现与负载均衡，维护连接池复用tcp连接减少资源消耗，读写锁保证并发安全
type XClient struct {
	d       Discovery          // 服务发现器
	mode    SelectMode         // 负载均衡模式
	opt     *Option            // rpc配置参数
	mu      sync.Mutex         // 保护clients的读写锁
	clients map[string]*Client // 地址到client的映射（连接池）
}

// 编译期断言，保证XClient实现了io.Closer接口
var _ io.Closer = (*XClient)(nil)

// XClient构造函数
func NewXClient(d Discovery, mode SelectMode, opt *Option) *XClient {
	return &XClient{d: d, mode: mode, opt: opt, clients: make(map[string]*Client)}
}

// 关闭连接池中的所有连接
func (xc *XClient) Close() error {
	xc.mu.Lock()
	defer xc.mu.Unlock()
	for key, client := range xc.clients {
		_ = client.Close()
		delete(xc.clients, key)
	}
	return nil
}

// 创建或获取连接池中的连接
func (xc *XClient) dial(rpcAddr string) (*Client, error) {
	xc.mu.Lock()
	defer xc.mu.Unlock()
	// 根据rpc服务端地址获取对应的客户端连接
	client, ok := xc.clients[rpcAddr]
	// 连接已缓存，但已失效
	if ok && !client.IsAvailable() {
		_ = client.Close()
		delete(xc.clients, rpcAddr)
		client = nil
	}
	// 懒加载：按需创建新连接并缓存
	// 连接不存在或已失效，创建新连接并缓存
	if client == nil {
		var err error
		client, err = XDial(rpcAddr, xc.opt)
		if err != nil {
			return nil, err
		}
		xc.clients[rpcAddr] = client
	}
	return client, nil
}

// 与目标服务端建立连接
func (xc *XClient) call(rpcAddr string, ctx context.Context, serviceMethod string, args, reply interface{}) error {
	client, err := xc.dial(rpcAddr)
	if err != nil {
		return err
	}
	return client.Call(ctx, serviceMethod, args, reply)
}

// 根据负载均衡策略，选择一个服务端，与其建立连接
func (xc *XClient) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	rpcAddr, err := xc.d.Get(xc.mode)
	if err != nil {
		return err
	}
	return xc.call(rpcAddr, ctx, serviceMethod, args, reply)
}

// 向注册在服务发现模块中的所有节点广播请求
// 快速失败机制：返回首个错误便于快速定位问题源头，避免错误风暴，但可能丢失其他节点错误详情
// 仅首个成功的协程会将响应写入reply对象
func (xc *XClient) Broadcast(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	// 获取所有服务地址
	servers, err := xc.d.GetAll()
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	var mu sync.Mutex // 保护共享变量e和replyDone
	var e error
	// 表示调用方是否关心返回值，即若调用方没有传入承载响应的指针，就不需要写入响应
	replyDone := reply == nil
	ctx, cancel := context.WithCancel(ctx)
	// 遍历所有服务地址
	for _, rpcAddr := range servers {
		wg.Add(1)
		go func(rpcAddr string) {
			defer wg.Done()
			var clonedReply interface{}
			// 调用方关心返回值，传入了承载响应的指针，此时克隆类型，由clonedReply承载响应，避免并发写入冲突
			if reply != nil {
				// 通过reflect包动态创建与reply同类型的新对象，避免并发写入冲突
				clonedReply = reflect.New(reflect.ValueOf(reply).Elem().Type()).Interface()
			}
			err := xc.call(rpcAddr, ctx, serviceMethod, args, clonedReply)
			mu.Lock()
			if err != nil && e == nil {
				// 记录首个错误，这里可以改成一个错误集合，记录所有错误
				e = err
				// 通知其他协程提前退出
				cancel()
			}
			// 调用方关注响应，且调用服务成功，将响应写入reply
			// 确保只有第一个成功协程将响应写入reply
			if err == nil && !replyDone {
				reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(clonedReply).Elem())
				replyDone = true
			}
			mu.Unlock()
		}(rpcAddr)
	}
	wg.Wait()
	return e
}
