package codec

import "io"

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
