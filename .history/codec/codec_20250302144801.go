package codec

import "io"

// 与消息编解码相关的代码

//
type Header struct {
	ServiceMethod string // 服务名和方法名
	Seq           uint64 // 请求序号，用于区分不同请求
	Error         string // 错误信息
}

// 用于对消息体进行编解码的接口Codec，用于实现不同的Codec实例
type Codec interface {
	io.Closer
	ReadHeader(*Header) error
	ReadBody(interface{}) error
	Write(*Header, interface{}) error
}

// 抽象出Codec的构造函数，类似工厂模式代码，输入一个Codec类型，工厂对应类型的Codec构造函数
type NewCodecFunc func(io.ReadWriteCloser) Codec

type Type string

const (
	GobType  Type = "application/gob"
	JsonType Type = "application/json"
)

var NewCodecFuncMap map[Type]NewCodecFunc

func init() {
	NewCodecFuncMap = make(map[Type]NewCodecFunc)
	NewCodecFuncMap[GobType] = NewGobCodec
}
