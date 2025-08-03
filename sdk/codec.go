package sdk

import (
	"fmt"
	"github.com/vmihailenco/msgpack/v5"
	"google.golang.org/protobuf/proto"
)

type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

// MsgPack
type MsgpackCodec struct{}

func (MsgpackCodec) Marshal(v any) ([]byte, error)   { return msgpack.Marshal(v) }
func (MsgpackCodec) Unmarshal(b []byte, v any) error { return msgpack.Unmarshal(b, v) }

// Protobuf
type ProtoCodec struct{}

func (ProtoCodec) Marshal(v any) ([]byte, error) {
	m, ok := v.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("not a proto.Message")
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(m)
}
func (ProtoCodec) Unmarshal(b []byte, v any) error {
	m, ok := v.(proto.Message)
	if !ok {
		return fmt.Errorf("not a proto.Message")
	}
	return proto.Unmarshal(b, m)
}
