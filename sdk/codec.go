package sdk

import (
	"encoding/json"
	"fmt"
	"github.com/vmihailenco/msgpack/v5"
	"google.golang.org/protobuf/proto"
)

type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

type JSONCodec struct{}

func (JSONCodec) Marshal(v any) ([]byte, error) {
	jsonData, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("json.Marshal: %w", err)
	}
	return jsonData, nil
}

func (JSONCodec) Unmarshal(data []byte, v any) error {
	err := json.Unmarshal(data, v)
	if err != nil {
		return fmt.Errorf("json.Unmarshal: %w", err)
	}
	return nil
}

type MsgpackCodec struct{}

func (MsgpackCodec) Marshal(v any) ([]byte, error) {
	return msgpack.Marshal(v)
}

func (MsgpackCodec) Unmarshal(b []byte, v any) error {
	return msgpack.Unmarshal(b, v)
}

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
