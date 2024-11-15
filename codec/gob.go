package codec

import (
	"bufio"
	"encoding/gob"
	"io"
	"log"
)

type GobCodec struct {
	conn io.ReadWriteCloser
	buf  *bufio.Writer
	dec  *gob.Decoder
	enc  *gob.Encoder
}

// 确保接口被实现的编译期间检查方式
var _ Codec = (*GobCodec)(nil)

func NewGobCodec(conn io.ReadWriteCloser) Codec {
	buf := bufio.NewWriter(conn)
	dec := gob.NewDecoder(conn)
	enc := gob.NewEncoder(buf)
	return &GobCodec{conn: conn, buf: buf, dec: dec, enc: enc}
}

// implements Codec interface
func (c *GobCodec) ReadHeader(header *Header) error {
	return c.dec.Decode(header)
}

func (c *GobCodec) ReadBody(body interface{}) error {
	return c.dec.Decode(body)
}

func (c *GobCodec) Close() error {
	return c.conn.Close()
}

func (c *GobCodec) Write(header *Header, body interface{}) error {
	var err error
	defer func() {
		_ = c.buf.Flush()
		if err != nil {
			_ = c.Close()
		}
	}()

	if err = c.enc.Encode(header); err != nil {
		log.Printf("micro-rpc error: error encoding gob header: %v\n", err)
		return err
	}

	if err = c.enc.Encode(body); err != nil {
		log.Printf("micro-rpc error: error encoding gob body: %v\n", err)
		return err
	}
	return nil
}
