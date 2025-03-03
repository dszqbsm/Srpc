package codec

// 担任编解码器的作用，使用gob包实现二进制序列化协议
// gob（go binary）是go语言自带的序列化协议实现

import (
	"bufio"
	"encoding/gob"
	"io"
	"log"
)

type GobCodec struct {
	conn io.ReadWriteCloser // 链接实例，负责字节流传输
	buf  *bufio.Writer      // 为防止阻塞而创建的带缓冲的Writer
	dec  *gob.Decoder       // 解码器
	enc  *gob.Encoder       // 编码器
}

// 接口实现的编译期断言，不会产生任何运行时开销，确保GobCodec必须完整实现Codec接口才能通过编译
var _ Codec = (*GobCodec)(nil)

// 工厂方法创建GobCodec实例
func NewGobCodec(conn io.ReadWriteCloser) Codec {
	buf := bufio.NewWriter(conn)
	return &GobCodec{
		conn: conn,
		buf:  buf,
		dec:  gob.NewDecoder(conn),
		enc:  gob.NewEncoder(buf),
	}
}

func (c *GobCodec) ReadHeader(h *Header) error {
	return c.dec.Decode(h)
}

func (c *GobCodec) ReadBody(body interface{}) error {
	return c.dec.Decode(body)
}

// 写入操作，自动缓冲管理，defer保证Flush
func (c *GobCodec) Write(h *Header, body interface{}) (err error) {
	defer func() {
		_ = c.buf.Flush()
		if err != nil {
			_ = c.Close()
		}
	}()
	if err := c.enc.Encode(h); err != nil {
		log.Println("rpc codec: gob error encoding header:", err)
		return err
	}
	if err := c.enc.Encode(body); err != nil {
		log.Println("rpc codec: gob error encoding body:", err)
		return err
	}
	return nil
}

func (c *GobCodec) Close() error {
	return c.conn.Close()
}
