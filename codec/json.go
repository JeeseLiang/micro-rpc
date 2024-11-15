package codec

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
)

type JsonCodec struct {
	conn io.ReadWriteCloser
	buf  *bufio.Writer
	dec  *json.Decoder
	enc  *json.Encoder
}

func NewJsonCodec(conn io.ReadWriteCloser) Codec {
	buf := bufio.NewWriter(conn)
	return &JsonCodec{
		conn: conn,
		buf:  buf,
		dec:  json.NewDecoder(conn),
		enc:  json.NewEncoder(buf),
	}
}

// implements Codec interface
func (c *JsonCodec) ReadHeader(header *Header) error {
	return c.dec.Decode(header)
}

func (c *JsonCodec) ReadBody(body interface{}) error {
	return c.dec.Decode(body)
}

func (c *JsonCodec) Close() error {
	return c.conn.Close()
}

func (c *JsonCodec) Write(header *Header, body interface{}) error {
	var err error
	defer func() {
		_ = c.buf.Flush()
		if err != nil {
			_ = c.Close()
		}
	}()

	if err = c.enc.Encode(header); err != nil {
		log.Printf("micro-rpc error: error encoding Json header: %v\n", err)
		return err
	}

	if err = c.enc.Encode(body); err != nil {
		log.Printf("micro-rpc error: error encoding Json body: %v\n", err)
		return err
	}
	return nil
}
