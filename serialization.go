package relay

import (
	"encoding/gob"
	"encoding/json"
	"io"
)

// Serializer interface is used to encode and
// decode messages. If not provided, a default serializer
// using gob is provided.
type Serializer interface {
	ContentType() string
	RelayEncode(io.Writer, interface{}) error
	RelayDecode(io.Reader, interface{}) error
}

// GOBSerializer implements the Serializer interface and uses the GOB format
type GOBSerializer struct{}

func (*GOBSerializer) ContentType() string {
	return "binary/gob"
}
func (*GOBSerializer) RelayEncode(w io.Writer, e interface{}) error {
	enc := gob.NewEncoder(w)
	return enc.Encode(e)
}
func (*GOBSerializer) RelayDecode(r io.Reader, o interface{}) error {
	dec := gob.NewDecoder(r)
	return dec.Decode(o)
}

// JSONSerializer implements the Serializer interface and uses JSON
type JSONSerializer struct{}

func (*JSONSerializer) ContentType() string {
	return "text/json"
}

func (*JSONSerializer) RelayEncode(w io.Writer, e interface{}) error {
	enc := json.NewEncoder(w)
	return enc.Encode(e)
}

func (*JSONSerializer) RelayDecode(r io.Reader, o interface{}) error {
	dec := json.NewDecoder(r)
	return dec.Decode(o)
}
