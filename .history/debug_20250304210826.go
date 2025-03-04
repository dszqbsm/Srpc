package srpc

import (
	"fmt"
	"html/template"
	"net/http"
)

// 定义了展示服务的页面结构，使用GO模板语法动态渲染数据
const debugText = `<html>
	<body>
	<title>GeeRPC Services</title>
	{{range .}}
	<hr>
	Service {{.Name}}
	<hr>
		<table>
		<th align=center>Method</th><th align=center>Calls</th>
		{{range $name, $mtype := .Method}}
			<tr>
			<td align=left font=fixed>{{$name}}({{$mtype.ArgType}}, {{$mtype.ReplyType}}) error</td>
			<td align=center>{{$mtype.NumCalls}}</td>
			</tr>
		{{end}}
		</table>
	{{end}}
	</body>
	</html>`

// 模板初始化，Must确保模板解析成功否则触发panic，创建名为"RPC debug"的模板对象，用于后续渲染
var debug = template.Must(template.New("RPC debug").Parse(debugText))

// 嵌入rpc服务器的核心功能，用于访问服务注册信息
type debugHTTP struct {
	*Server
}

// 服务数据容器，存储服务名称和方法信息
type debugService struct {
	Name   string
	Method map[string]*methodType
}

// Runs at /debug/geerpc
func (server debugHTTP) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// Build a sorted version of the data.
	var services []debugService
	// 遍历所有已注册的服务，并转换为debugService类型收集到services切片中
	server.serviceMap.Range(func(namei, svci interface{}) bool {
		svc := svci.(*service)
		services = append(services, debugService{
			Name:   namei.(string),
			Method: svc.method,
		})
		return true
	})
	// 渲染模板，将services数据注入模板，生成html响应
	err := debug.Execute(w, services)
	if err != nil {
		_, _ = fmt.Fprintln(w, "rpc: error executing template:", err.Error())
	}
}
