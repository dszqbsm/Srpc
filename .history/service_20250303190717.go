package srpc

import (
	"go/ast"
	"log"
	"reflect"
	"sync/atomic"
)

// 包含一个方法的完整信息
type methodType struct {
	method    reflect.Method // 方法本身
	ArgType   reflect.Type   // 第一个参数的类型
	ReplyType reflect.Type   // 第二个参数的类型
	numCalls  uint64         // 方法被调用次数
}

func (m *methodType) NumCalls() uint64 {
	return atomic.LoadUint64(&m.numCalls)
}

// 根据参数1类型创建实例，动态创建参数以支持序列化，最后返回的argv都是可修改的reflect.Value，即指针的解引用或值的直接持有
func (m *methodType) newArgv() reflect.Value {
	// 用于存储参数实例的反射值
	var argv reflect.Value
	// 参数可能是指针类型或值类型
	if m.ArgType.Kind() == reflect.Ptr {
		argv = reflect.New(m.ArgType.Elem()) // 根据指针指向的类型，创建新的指针实例
	} else {
		argv = reflect.New(m.ArgType).Elem() // 创建指向值类型的指针，再通过Elem()解引用得到值实例
	}
	return argv
}

// 根据参数2类型创建实例，因为参数2必须是指针类型，确保响应指针指向已初始化的容器，避免反序列化时因未初始化导致panic
func (m *methodType) newReplyv() reflect.Value {
	// rpc要求响应必须是指针类型
	replyv := reflect.New(m.ReplyType.Elem()) // 先获取指针指向的类型，再创建该类型的指针实例
	// 根据指针指向的类型i进行分支处理，仅对Map和Slice进行特殊处理
	switch m.ReplyType.Elem().Kind() {
	// reflect.MakeMap和reflect.MakeSlice分别创建空容器实例，并通过Set方法赋值给指针指向的值
	case reflect.Map:
		replyv.Elem().Set(reflect.MakeMap(m.ReplyType.Elem()))
	case reflect.Slice:
		replyv.Elem().Set(reflect.MakeSlice(m.ReplyType.Elem(), 0, 0))
	}
	return replyv
}

type service struct {
	name   string                 // 映射的结构体名称
	typ    reflect.Type           // 结构体类型
	rcvr   reflect.Value          // 结构体实例本身，保留rcvr是因为在调用方法时需要rcvr作为第0个参数
	method map[string]*methodType // 存储映射的结构体的所有符合条件的方法
}

// 用于创建service实例，入参是任意需要映射为服务的结构体实例
func newService(rcvr interface{}) *service {
	s := new(service)
	s.rcvr = reflect.ValueOf(rcvr)
	s.name = reflect.Indirect(s.rcvr).Type().Name() // 取指针指向的类型，再获取类型名称
	s.typ = reflect.TypeOf(rcvr)
	if !ast.IsExported(s.name) {
		log.Fatalf("rpc server: %s is not a valid service name", s.name)
	}
	s.registerMethods()
	return s
}

// 过滤出了符合条件的方法
func (s *service) registerMethods() {
	s.method = make(map[string]*methodType)
	for i := 0; i < s.typ.NumMethod(); i++ {
		method := s.typ.Method(i)
		mType := method.Type
		if mType.NumIn() != 3 || mType.NumOut() != 1 {
			continue
		}
		if mType.Out(0) != reflect.TypeOf((*error)(nil)).Elem() {
			continue
		}
		argType, replyType := mType.In(1), mType.In(2)
		if !isExportedOrBuiltinType(argType) || !isExportedOrBuiltinType(replyType) {
			continue
		}
		s.method[method.Name] = &methodType{
			method:    method,
			ArgType:   argType,
			ReplyType: replyType,
		}
		log.Printf("rpc server: register %s.%s\n", s.name, method.Name)
	}
}

func isExportedOrBuiltinType(t reflect.Type) bool {
	return ast.IsExported(t.Name()) || t.PkgPath() == ""
}

func (s *service) call(m *methodType, argv, replyv reflect.Value) error {
	atomic.AddUint64(&m.numCalls, 1)
	f := m.method.Func
	returnValues := f.Call([]reflect.Value{s.rcvr, argv, replyv})
	if errInter := returnValues[0].Interface(); errInter != nil {
		return errInter.(error)
	}
	return nil
}
