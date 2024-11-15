package codec

import "io"

type Header struct {
	ID            uint64 // 请求号
	ServiceMethod string // 服务方法名
	Error         string // 错误信息
}

type Codec interface {
	io.Closer
	Write(*Header, interface{}) error
	ReadHeader(*Header) error
	ReadBody(interface{}) error
}

type CodecFunc func(io.ReadWriteCloser) Codec

type Type string

const (
	JSON Type = "application/json"
	GOB  Type = "application/gob"
)

var CodecFuncMap map[Type]CodecFunc

func init() {
	CodecFuncMap = make(map[Type]CodecFunc)
	CodecFuncMap[JSON] = NewJsonCodec
	CodecFuncMap[GOB] = NewGobCodec
}
