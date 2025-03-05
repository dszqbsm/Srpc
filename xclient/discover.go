package xclient

import (
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"
)

type SelectMode int

const (
	RandomSelect     SelectMode = iota // 0：随机选择
	RoundRobinSelect                   // 1：轮询选择
)

// 服务发现接口，包含服务发现所需的接口方法
type Discovery interface {
	Refresh() error                      // 从注册中心更新服务列表
	Update(servers []string) error       // 手动更新服务列表
	Get(mode SelectMode) (string, error) // 根据负载均衡策略，选择一个服务实例
	GetAll() ([]string, error)           // 返回所有的服务实例
}

// 没有注册中心的服务发现器
// 用户显式提供服务器地址
type MultiServersDiscovery struct {
	r *rand.Rand // 随机数生成器
	// 读写锁专门针对读多写少场景优化的并发控制机制，即通过读写分离策略提升性能
	// 互斥锁是完全互斥，读写锁读操作可并发，写操作互斥
	mu      sync.RWMutex // 读写锁
	servers []string     // 服务地址列表
	index   int          // 轮询索引
}

// 服务发现器构造函数
func NewMultiServerDiscovery(servers []string) *MultiServersDiscovery {
	d := &MultiServersDiscovery{
		servers: servers,
		r:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	// 随机初始化index，防止所有客户端同时从0开始轮询
	d.index = d.r.Intn(math.MaxInt32 - 1) // Intn方法返回一个伪随机整数，范围从0到MaxInt32-1
	return d
}

// 编译期断言，确保MultiServersDiscovery实现了Discovery接口
var _ Discovery = (*MultiServersDiscovery)(nil)

// 空接口实现，实际用于需要动态发现的实现
func (d *MultiServersDiscovery) Refresh() error {
	return nil
}

// 动态更新可用服务列表
func (d *MultiServersDiscovery) Update(servers []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.servers = servers
	return nil
}

// 根据负载均衡策略，选择一个服务实例
func (d *MultiServersDiscovery) Get(mode SelectMode) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := len(d.servers)
	if n == 0 {
		return "", errors.New("rpc discovery: no available servers")
	}
	switch mode {
	// 随机选择
	case RandomSelect:
		return d.servers[d.r.Intn(n)], nil
		// 轮询选择
	case RoundRobinSelect:
		// 模n确保索引不越界，实现环形遍历
		s := d.servers[d.index%n]
		d.index = (d.index + 1) % n
		return s, nil
	default:
		return "", errors.New("rpc discovery: not supported select mode")
	}
}

// 返回所有服务器实例
func (d *MultiServersDiscovery) GetAll() ([]string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	// 返回副本，避免外部修改内部状态
	servers := make([]string, len(d.servers), len(d.servers))
	copy(servers, d.servers)
	return servers, nil
}
